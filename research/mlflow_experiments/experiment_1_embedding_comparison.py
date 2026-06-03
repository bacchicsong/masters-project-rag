"""
Experiment 1: Embedding Model Comparison
=========================================
Compare different embedding models for document retrieval quality.
Logs params, metrics, and artifacts to MLflow --> MinIO.

Models tested:
- sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 (384d)
- intfloat/multilingual-e5-small (384d)
- intfloat/multilingual-e5-base (768d)
- distiluse-base-multilingual-cased-v2 (512d)
"""
import json
import time
import tempfile
import os
from typing import List

import mlflow
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from mlflow_config import setup_experiment
from utils.metrics import compute_retrieval_metrics
from utils.data_loader import load_test_queries
from utils.document_processor import get_text_by_strategy


# === Configuration ===
EXPERIMENT_KEY = "embedding-comparison"
K_VALUES = [1, 3, 5, 8]

EMBEDDING_MODELS = [
    {
        "name": "paraphrase-multilingual-MiniLM-L12-v2",
        "model_id": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        "expected_dim": 384,
    },
    {
        "name": "multilingual-e5-small",
        "model_id": "intfloat/multilingual-e5-small",
        "expected_dim": 384,
    },
    {
        "name": "multilingual-e5-base",
        "model_id": "intfloat/multilingual-e5-base",
        "expected_dim": 768,
    },
    {
        "name": "distiluse-base-multilingual-cased-v2",
        "model_id": "sentence-transformers/distiluse-base-multilingual-cased-v2",
        "expected_dim": 512,
    },
]

DEFAULT_STRATEGY = "full"
NUM_QUERIES = None


def encode_corpus(model: SentenceTransformer, texts: List[str], batch_size: int = 16) -> np.ndarray:
    """Encode a list of texts into embeddings."""
    return model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=True,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )


def search_documents(
    query_emb: np.ndarray,
    doc_embeddings: np.ndarray,
    top_k: int = 10,
) -> List[int]:
    """Find top-k document indices for a query embedding."""
    sims = cosine_similarity(query_emb.reshape(1, -1), doc_embeddings)[0]
    top_indices = np.argsort(sims)[::-1][:top_k]
    return top_indices.tolist()


def run_experiment():
    """Run the embedding model comparison experiment."""
    experiment_name = setup_experiment(EXPERIMENT_KEY)

    docs, test_queries = load_test_queries(use_mock=True, num_queries=NUM_QUERIES)

    doc_ids = [doc.get("id", str(i)) for i, doc in enumerate(docs)]
    doc_texts = [get_text_by_strategy(doc, DEFAULT_STRATEGY) for doc in docs]

    ground_truth = {}
    for tq in test_queries:
        ground_truth[tq["query_id"]] = tq["relevant_doc_ids"]

    query_texts = [tq["query"] for tq in test_queries]
    query_ids = [tq["query_id"] for tq in test_queries]

    for model_cfg in EMBEDDING_MODELS:
        model_name = model_cfg["name"]
        model_id = model_cfg["model_id"]
        expected_dim = model_cfg["expected_dim"]

        print(f"\n{'='*60}")
        print(f"[EXP] Testing model: {model_name} ({model_id})")
        print(f"{'='*60}")

        with mlflow.start_run(run_name=model_name):
            mlflow.log_params({
                "model_name": model_name,
                "model_id": model_id,
                "expected_dim": expected_dim,
                "strategy": DEFAULT_STRATEGY,
                "num_documents": len(docs),
                "num_queries": len(test_queries),
                "k_values": json.dumps(K_VALUES),
            })

            start_time = time.perf_counter()
            model = SentenceTransformer(model_id)
            load_time = time.perf_counter() - start_time
            mlflow.log_metric("model_load_time_s", load_time)

            start_time = time.perf_counter()
            doc_embeddings = encode_corpus(model, doc_texts)
            index_time = time.perf_counter() - start_time

            actual_dim = doc_embeddings.shape[1]
            mlflow.log_metric("actual_embedding_dim", actual_dim)
            mlflow.log_metric("indexing_time_s", index_time)

            query_embeddings = encode_corpus(model, query_texts)

            predictions = {}
            search_times = []

            for qi, qid in enumerate(query_ids):
                start_time = time.perf_counter()
                top_indices = search_documents(
                    query_embeddings[qi], doc_embeddings, top_k=max(K_VALUES)
                )
                search_time = time.perf_counter() - start_time
                search_times.append(search_time)
                predictions[qid] = [doc_ids[idx] for idx in top_indices]

            retrieval_metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
            retrieval_metrics["avg_search_latency_s"] = float(np.mean(search_times))
            retrieval_metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))

            mlflow.log_metrics(retrieval_metrics)

            model_info = {
                "model_name": model_name,
                "model_id": model_id,
                "embedding_dim": actual_dim,
                "load_time_s": load_time,
                "indexing_time_s": index_time,
                "num_documents": len(docs),
                "num_queries": len(test_queries),
                "strategy": DEFAULT_STRATEGY,
            }

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False, encoding="utf-8"
            ) as f:
                json.dump({"model_info": model_info, "metrics": retrieval_metrics, "predictions": predictions}, f, ensure_ascii=False, indent=2)
                artifact_path = f.name

            mlflow.log_artifact(artifact_path, artifact_path="results")
            os.unlink(artifact_path)
            mlflow.log_text(json.dumps(model_info, ensure_ascii=False, indent=2), "model_info.json")

            print(f"\n[RESULTS] Metrics for {model_name}:")
            for k in K_VALUES:
                print(f"   P@{k}: {retrieval_metrics[f'mean_P@{k}']:.4f} | "
                      f"R@{k}: {retrieval_metrics[f'mean_R@{k}']:.4f}")
            print(f"   MRR: {retrieval_metrics['MRR']:.4f}")
            print(f"   Avg search latency: {retrieval_metrics['avg_search_latency_s']:.4f}s")

    print(f"\n[COMPLETE] Experiment '{experiment_name}' completed!")


if __name__ == "__main__":
    run_experiment()