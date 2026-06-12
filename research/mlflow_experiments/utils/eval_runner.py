import json
import os
import tempfile
import time
from typing import Any, Callable, Dict, List, Optional

import mlflow
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from utils.metrics import compute_retrieval_metrics


def build_ground_truth(test_queries: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    gt = {}
    for tq in test_queries:
        gt[tq["query_id"]] = tq["relevant_doc_ids"]
    return gt


def dense_search(query_emb: np.ndarray, doc_embeddings: np.ndarray, top_k: int) -> List[int]:
    sims = cosine_similarity(query_emb.reshape(1, -1), doc_embeddings)[0]
    return np.argsort(sims)[::-1][:top_k].tolist()


def run_dense_retrieval(
    model: SentenceTransformer,
    doc_texts: List[str],
    doc_ids: List[str],
    query_texts: List[str],
    query_ids: List[str],
    ground_truth: Dict[str, List[str]],
    k_values: List[int],
    batch_size: int = 16,
) -> Dict[str, Any]:
    doc_embeddings = model.encode(
        doc_texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )
    query_embeddings = model.encode(
        query_texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )

    predictions = {}
    search_times = []
    top_k = max(k_values)

    for qi, qid in enumerate(query_ids):
        start = time.perf_counter()
        top_indices = dense_search(query_embeddings[qi], doc_embeddings, top_k)
        search_times.append(time.perf_counter() - start)
        predictions[qid] = [doc_ids[idx] for idx in top_indices]

    metrics = compute_retrieval_metrics(ground_truth, predictions, k_values)
    metrics["avg_search_latency_s"] = float(np.mean(search_times))
    metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))

    return {
        "metrics": metrics,
        "predictions": predictions,
        "doc_embeddings": doc_embeddings,
    }


def log_retrieval_run(
    params: Dict[str, Any],
    metrics: Dict[str, float],
    predictions: Optional[Dict[str, List[str]]] = None,
    extra: Optional[Dict[str, Any]] = None,
):
    mlflow.log_params(params)
    mlflow.log_metrics({k.replace("@", "_at_"): v for k, v in metrics.items()})
    payload = {"metrics": metrics, "predictions": predictions or {}}
    if extra:
        payload.update(extra)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        path = f.name
    mlflow.log_artifact(path, artifact_path="results")
    os.unlink(path)


def run_custom_retrieval(
    search_fn: Callable[[str], List[int]],
    doc_ids: List[str],
    query_texts: List[str],
    query_ids: List[str],
    ground_truth: Dict[str, List[str]],
    k_values: List[int],
) -> Dict[str, Any]:
    predictions = {}
    search_times = []
    top_k = max(k_values)

    for qi, qid in enumerate(query_ids):
        start = time.perf_counter()
        top_indices = search_fn(query_texts[qi])[:top_k]
        search_times.append(time.perf_counter() - start)
        predictions[qid] = [doc_ids[idx] for idx in top_indices]

    metrics = compute_retrieval_metrics(ground_truth, predictions, k_values)
    metrics["avg_search_latency_s"] = float(np.mean(search_times))
    metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
    return {"metrics": metrics, "predictions": predictions}
