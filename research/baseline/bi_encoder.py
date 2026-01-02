from typing import List, Dict, Any, Iterable, Optional, Tuple
import time
import tracemalloc
import functools
import logging
import psutil
import json
import os

import numpy as np
import torch
from torch import nn
from transformers import AutoTokenizer, AutoModel
from sklearn.metrics.pairwise import cosine_similarity

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def measure_perf(print_result: bool = True):
  """Декоратор для измерения времени выполнения и потребления памяти в инференсе.
  
  
  Измеряет:
  - wall time (seconds) через time.perf_counter
  - peak Python memory allocation (KiB) через tracemalloc
  - системную RSS (MB) через psutil (если доступно)
  - GPU memory (MB) через torch.cuda (если доступно)
  
  
  Возвращает исходное значение функции и, дополнительно, кладет в атрибут .perf_info словарь с метриками.
  """
  def decorator(fn):
      @functools.wraps(fn)
      def wrapper(*args, **kwargs):
          tracemalloc_started = False
          try:
              tracemalloc.start()
              tracemalloc_started = True
          except Exception:
              pass

          start = time.perf_counter()
          rss_before = None
          if psutil is not None:
              try:
                  rss_before = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
              except Exception:
                  rss_before = None
          gpu_before = None
          if torch.cuda.is_available():
              try:
                  gpu_before = torch.cuda.memory_allocated() / (1024 * 1024)
              except Exception:
                  gpu_before = None

          result = fn(*args, **kwargs)

          end = time.perf_counter()
          peak_kib = None
          if tracemalloc_started:
              current, peak = tracemalloc.get_traced_memory()
              peak_kib = peak / 1024.0
              tracemalloc.stop()

          rss_after = None
          if psutil is not None:
              try:
                  rss_after = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
              except Exception:
                  rss_after = None

          gpu_after = None
          if torch.cuda.is_available():
              try:
                  gpu_after = torch.cuda.memory_allocated() / (1024 * 1024)
              except Exception:
                  gpu_after = None

          perf_info = {
              "wall_time_s": end - start,
              "peak_python_alloc_kib": peak_kib,
              "rss_mb_before": rss_before,
              "rss_mb_after": rss_after,
              "gpu_mb_before": gpu_before,
              "gpu_mb_after": gpu_after,
          }

            if hasattr(result, "__dict__"):
                try:
                    setattr(result, "perf_info", perf_info)
                except Exception:
                    pass

            if print_result:
                logger.info(f"Perf info for {fn.__name__}: {perf_info}")

            return result, perf_info

        return wrapper

    return decorator


class bi_encoder:
    """Bi-encoder класс для кодирования вопросов и документов с использованием BERT-like моделей.

    Основные методы:
    - encode_texts(texts, batch_size=32): возвращает numpy массив эмбеддингов
    - index_documents(docs): индексирует документы (список dict с полем 'id' и 'text')
    - search(query, top_k=5): возвращает top_k документов по косинусной близости
    - validate(dataset, K={1,3,5,8}): оценка Recall@k и Precision@k

    Параметры:
    - model_name: имя модели HuggingFace (например, 'bert-base-uncased')
    - device: 'cpu' или 'cuda'
    - max_length: максимальная длина токенов
    - pooling: 'mean' или 'cls'
    """

  def __init__(
      self,
      model_name: str = "bert-base-uncased",
      device: Optional[str] = None,
      max_length: int = 256,
      pooling: str = "mean",
  ):
      self.model_name = model_name
      self.max_length = max_length
      self.pooling = pooling

      if device is None:
          self.device = "cuda" if torch.cuda.is_available() else "cpu"
      else:
          self.device = device

      logger.info(f"Loading tokenizer and model: {model_name} on {self.device}")
      self.tokenizer = AutoTokenizer.from_pretrained(model_name)
      self.model = AutoModel.from_pretrained(model_name)
      self.model.to(self.device)
      self.model.eval()

      self.doc_ids: List[Any] = []
      self.doc_texts: List[str] = []
      self.doc_embeddings: Optional[np.ndarray] = None

  def _pool_embeddings(self, last_hidden_state: torch.Tensor, attention_mask: torch.Tensor) -> torch.Tensor:
      """Пуллинг: mean pooling с учетом attention_mask, либо CLS token."""
      if self.pooling == "cls":
          return last_hidden_state[:, 0]
      input_mask_expanded = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
      sum_embeddings = torch.sum(last_hidden_state * input_mask_expanded, 1)
      sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)
      return sum_embeddings / sum_mask

  def encode_texts(self, texts: Iterable[str], batch_size: int = 32) -> np.ndarray:
      """Кодирует список текстов в эмбеддинги (numpy array).

      Возвращаем shape = (N, D)
      """
      all_embs = []
      iterator = iter(texts)

      batch = []
      for t in iterator:
          batch.append(t)
          if len(batch) >= batch_size:
              embs = self._encode_batch(batch)
              all_embs.append(embs)
              batch = []
      if batch:
          embs = self._encode_batch(batch)
          all_embs.append(embs)

      if len(all_embs) == 0:
          return np.zeros((0, self.model.config.hidden_size), dtype=np.float32)

      return np.vstack(all_embs)

  def _encode_batch(self, texts: List[str]) -> np.ndarray:
      """Внутренний метод кодирования пачки. Возвращает numpy array.
      Не декорирован — измерение perf производится над validate (или можно применить декоратор на этом методе при желании).
      """
      with torch.no_grad():
          encoded = self.tokenizer(
              texts,
              padding=True,
              truncation=True,
              max_length=self.max_length,
              return_tensors="pt",
          )
          input_ids = encoded["input_ids"].to(self.device)
          attention_mask = encoded["attention_mask"].to(self.device)

          outputs = self.model(input_ids=input_ids, attention_mask=attention_mask, return_dict=True)
          last_hidden = outputs.last_hidden_state  # (bs, seq_len, dim)
          pooled = self._pool_embeddings(last_hidden, attention_mask)
          pooled = pooled.cpu().numpy()
          norms = np.linalg.norm(pooled, axis=1, keepdims=True)
          norms[norms == 0] = 1e-9
          pooled = pooled / norms
          return pooled.astype(np.float32)

  def index_documents(self, docs: Iterable[Dict[str, Any]], batch_size: int = 32):
      """Индексирует документы из итерируемого JSON-like формата.

      Каждый элемент docs должен быть dict с как минимум полями:
          - 'id' : уникальный идентификатор
          - 'text' : текст документа

      Допускается передать список словарей, например, загруженных из JSON файла.
      После индексирования self.doc_embeddings содержит L2-нормированные эмбеддинги (numpy array).
      """
      docs = list(docs)
      if len(docs) == 0:
          return
      ids = [d["id"] for d in docs]
      texts = [d["text"] for d in docs]

      embs = self.encode_texts(texts, batch_size=batch_size)

      self.doc_ids = ids
      self.doc_texts = texts
      self.doc_embeddings = embs
      logger.info(f"Indexed {len(ids)} documents. Embedding shape: {self.doc_embeddings.shape}")

  def search(self, query: str, top_k: int = 5, batch_size: int = 32) -> List[Tuple[Any, float]]:
      """Поиск top_k документов по косинусному сходству.

      Возвращает список кортежей (doc_id, score) отсортированных по убыванию сходства.
      """
      if self.doc_embeddings is None or len(self.doc_embeddings) == 0:
          raise ValueError("No documents indexed. Call index_documents first.")

      q_emb = self.encode_texts([query], batch_size=batch_size)
      sims = cosine_similarity(q_emb, self.doc_embeddings)[0]
      top_idx = np.argsort(sims)[::-1][:top_k]
      results = [(self.doc_ids[i], float(sims[i])) for i in top_idx]
      return results

  @measure_perf(print_result=True)
  def validate(
      self,
      dataset: Iterable[Dict[str, Any]],
      K: Optional[Iterable[int]] = None,
      batch_size: int = 32,
  ) -> Dict[str, Any]:
      """Валидирует bi-encoder на наборе данных.

      dataset: итерируемый объектов, где каждый объект содержит:
          - 'query' : текст запроса
          - 'positive_doc_ids' : список id правильных документов

      K: iterable k значений для Recall@k / Precision@k, по умолчанию {1,3,5,8}

      Возвращает dict с метриками: mean Recall@k и mean Precision@k для каждого k.
      Также возвращает per-query результаты внутри 'per_query' ключа.
      """
      if K is None:
          K = [1, 3, 5, 8]
      else:
          K = list(K)

      data = list(dataset)
      if len(data) == 0:
          raise ValueError("Empty dataset passed to validate")

      if self.doc_embeddings is None:
          raise ValueError("No documents indexed. Call index_documents before validate.")

      per_query = []
      queries = [item["query"] for item in data]
      true_positives = [item.get("positive_doc_ids", []) for item in data]
      q_embs = self.encode_texts(queries, batch_size=batch_size)

      sims = cosine_similarity(q_embs, self.doc_embeddings)

      metrics = {f"R@{k}": [] for k in K}
      metrics.update({f"P@{k}": [] for k in K})

      for qi in range(len(queries)):
          sim_row = sims[qi]
          sorted_idx = np.argsort(sim_row)[::-1]
          sorted_doc_ids = [self.doc_ids[i] for i in sorted_idx]
          positives = set(true_positives[qi])
          if len(positives) == 0:
              continue

          for k in K:
              topk = sorted_doc_ids[:k]
              found = len([d for d in topk if d in positives])
              precision_k = found / float(k)
              recall_k = found / float(len(positives))
              metrics[f"P@{k}"].append(precision_k)
              metrics[f"R@{k}"].append(recall_k)

          per_query.append({
              "query": queries[qi],
              "positives": list(positives),
              "ranked": sorted_doc_ids[: max(K) ],
          })

      aggregated = {}
      for k in K:
          p_list = metrics[f"P@{k}"]
          r_list = metrics[f"R@{k}"]
          aggregated[f"mean_P@{k}"] = float(np.mean(p_list)) if len(p_list) > 0 else None
          aggregated[f"mean_R@{k}"] = float(np.mean(r_list)) if len(r_list) > 0 else None

      result = {
          "K": K,
          "aggregated": aggregated,
          "per_query": per_query,
      }
      return result


if __name__ == "__main__":
  docs = [
      {"id": "d1", "text": "The capital of France is Paris."},
      {"id": "d2", "text": "Berlin is the capital of Germany."},
      {"id": "d3", "text": "Madrid is the capital of Spain."},
      {"id": "d4", "text": "Rome is the capital of Italy."},
      {"id": "d5", "text": "Paris is known for the Eiffel Tower."},
  ]

  dataset = [
      {"query": "What is the capital of France?", "positive_doc_ids": ["d1", "d5"]},
      {"query": "Capital of Germany", "positive_doc_ids": ["d2"]},
      {"query": "Where is the Eiffel Tower located?", "positive_doc_ids": ["d5"]},
  ]

  encoder = bi_encoder(model_name="sentence-transformers/all-MiniLM-L6-v2")
  encoder.index_documents(docs)

  res = encoder.search("Which city is the capital of France?", top_k=3)
  print("Search results:")
  for doc_id, score in res:
      print(doc_id, score)

  (val_res, perf) = encoder.validate(dataset, K=[1, 3, 5])
  print("Validation aggregated:")
  print(json.dumps(val_res["aggregated"], indent=2, ensure_ascii=False))
  print("Perf:")
  print(perf)
