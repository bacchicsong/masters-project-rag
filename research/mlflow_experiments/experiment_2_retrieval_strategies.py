"""
Experiment 2: Retrieval Strategies Comparison
==============================================
Compare pure bi-encoder (dense) vs BM25 (sparse) vs hybrid (BM25 + bi-encoder rerank).
Logs params, metrics, and artifacts to MLflow → MinIO.

Strategies:
1. Dense (bi-encoder only) - cosine similarity on embeddings
2. Sparse (BM25 only) - TF-IDF based keyword matching
3. Hybrid (BM25 → bi-encoder rerank) - two-stage retrieval
"""
import json
import time
import tempfile
import os
from typing import List, Dict, Any, Optional

import mlflow
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from rank_bm25 import BM25Okapi

from mlflow_config import setup_experiment
from utils.metrics import compute_retrieval_metrics
from utils.data_loader import load_test_queries
from utils.document_processor import get_text_by_strategy


# === Configuration ===
EXPERIMENT_KEY = "retrieval-strategies"
K_VALUES = [1, 3, 5, 8]

EMBEDDING_MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
STRATEGY = "full"
BM25_CANDIDATES = [20, 50, 100]  # Different BM25 candidate counts to test

NUM_QUERIES = None


def dense_search(
    query_text: str,
    model: SentenceTransformer,
    doc_embeddings: np.ndarray,
    top_k: int = 10,
) -> List[int]:
    """Pure dense retrieval using cosine similarity."""
    q_emb = model.encode([query_text], normalize_embeddings=True)
    sims = cosine_similarity(q_emb, doc_embeddings)[0]
    top_indices = np.argsort(sims)[::-1][:top_k]
    return top_indices.tolist()


def sparse_search(
    query_text: str,
    bm25: BM25Okapi,
    top_k: int = 10,
) -> List[int]:
    """Pure sparse retrieval using BM25."""
    tokenized_query = query_text.lower().split()
    scores = bm25.get_scores(tokenized_query)
    top_indices = np.argsort(scores)[::-1][:top_k]
    return top_indices.tolist()


def hybrid_search(
    query_text: str,
    model: SentenceTransformer,
    doc_embeddings: np.ndarray,
    bm25: BM25Okapi,
    bm25_k: int = 50,
    top_k: int = 10,
) -> List[int]:
    """Two-stage retrieval: BM25 candidates → bi-encoder reranking."""
    # Stage 1: BM25 candidate retrieval
    tokenized_query = query_text.lower().split()
    bm25_candidates_count = min(bm25_k, len(doc_embeddings))
    bm25_scores = bm25.get_scores(tokenized_query)
    candidate_indices = np.argsort(bm25_scores)[::-1][:bm25_candidates_count]

    # Stage 2: Bi-encoder reranking
    q_emb = model.encode([query_text], normalize_embeddings=True)
    candidate_embeddings = doc_embeddings[candidate_indices]
    sims = cosine_similarity(q_emb, candidate_embeddings)[0]
    reranked = candidate_indices[np.argsort(sims)[::-1][:top_k]]

    return reranked.tolist()


def run_experiment():
    """Run the retrieval strategies comparison experiment."""
    experiment_name = setup_experiment(EXPERIMENT_KEY)

    # Load test data
    docs, test_queries = load_test_queries(use_mock=True, num_queries=NUM_QUERIES)

    doc_ids = [doc.get("id", str(i)) for i, doc in enumerate(docs)]
    doc_texts = [get_text_by_strategy(doc, STRATEGY) for doc in docs]

    # Build ground truth dict
    ground_truth = {}
    for tq in test_queries:
        ground_truth[tq["query_id"]] = tq["relevant_doc_ids"]

    query_texts = [tq["query"] for tq in test_queries]
    query_ids = [tq["query_id"] for tq in test_queries]

    # Load embedding model once (shared across runs)
    print(f"[PACKAGE] Loading embedding model: {EMBEDDING_MODEL_ID}")
    model = SentenceTransformer(EMBEDDING_MODEL_ID)

    # Encode all documents once
    print("[PACKAGE] Encoding all documents...")
    doc_embeddings = model.encode(
        doc_texts, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True
    )

    # Build BM25 index once
    print("[PACKAGE] Building BM25 index...")
    tokenized_docs = [t.lower().split() for t in doc_texts]
    bm25 = BM25Okapi(tokenized_docs)

    # ============================
    # Strategy 1: Dense (pure bi-encoder)
    # ============================
    print(f"\n{'='*60}")
    print("[RESULTS] Strategy: Dense (bi-encoder only)")
    print(f"{'='*60}")

    with mlflow.start_run(run_name="dense-bi-encoder"):
        mlflow.log_params({
            "retrieval_type": "dense",
            "model_id": EMBEDDING_MODEL_ID,
            "strategy": STRATEGY,
            "num_documents": len(docs),
            "num_queries": len(test_queries),
            "embedding_dim": doc_embeddings.shape[1],
            "k_values": json.dumps(K_VALUES),
        })

        predictions = {}
        search_times = []

        for qi, qid in enumerate(query_ids):
            start_time = time.perf_counter()
            top_indices = dense_search(query_texts[qi], model, doc_embeddings, top_k=max(K_VALUES))
            search_time = time.perf_counter() - start_time
            search_times.append(search_time)
            predictions[qid] = [doc_ids[idx] for idx in top_indices]

        metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
        metrics["avg_search_latency_s"] = float(np.mean(search_times))
        metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
        mlflow.log_metrics(metrics)

    # ============================
    # Strategy 2: Sparse (BM25 only)
    # ============================
    print(f"\n{'='*60}")
    print("[RESULTS] Strategy: Sparse (BM25 only)")
    print(f"{'='*60}")

    with mlflow.start_run(run_name="sparse-bm25"):
        mlflow.log_params({
            "retrieval_type": "sparse",
            "bm25_algorithm": "Okapi",
            "strategy": STRATEGY,
            "num_documents": len(docs),
            "num_queries": len(test_queries),
            "k_values": json.dumps(K_VALUES),
        })

        predictions = {}
        search_times = []

        for qi, qid in enumerate(query_ids):
            start_time = time.perf_counter()
            top_indices = sparse_search(query_texts[qi], bm25, top_k=max(K_VALUES))
            search_time = time.perf_counter() - start_time
            search_times.append(search_time)
            predictions[qid] = [doc_ids[idx] for idx in top_indices]

        metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
        metrics["avg_search_latency_s"] = float(np.mean(search_times))
        metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
        mlflow.log_metrics(metrics)

    # ============================
    # Strategy 3: Hybrid (BM25 → bi-encoder) with different BM25_k
    # ============================
    for bm25_k in BM25_CANDIDATES:
        run_name = f"hybrid-bm25_k={bm25_k}"
        print(f"\n{'='*60}")
        print(f"[RESULTS] Strategy: Hybrid (BM25→dense, BM25_k={bm25_k})")
        print(f"{'='*60}")

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "retrieval_type": "hybrid",
                "model_id": EMBEDDING_MODEL_ID,
                "bm25_k": bm25_k,
                "strategy": STRATEGY,
                "num_documents": len(docs),
                "num_queries": len(test_queries),
                "embedding_dim": doc_embeddings.shape[1],
                "k_values": json.dumps(K_VALUES),
            })

            predictions = {}
            search_times = []

            for qi, qid in enumerate(query_ids):
                start_time = time.perf_counter()
                top_indices = hybrid_search(
                    query_texts[qi], model, doc_embeddings, bm25,
                    bm25_k=bm25_k, top_k=max(K_VALUES)
                )
                search_time = time.perf_counter() - start_time
                search_times.append(search_time)
                predictions[qid] = [doc_ids[idx] for idx in top_indices]

            metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
            metrics["avg_search_latency_s"] = float(np.mean(search_times))
            metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
            mlflow.log_metrics(metrics)

    # ============================
    # Summary comparison
    # ============================
    print(f"\n{'='*60}")
    print("[CHART] SUMMARY")
    print(f"{'='*60}")
    print(f"Experiment '{experiment_name}' completed!")
    print(f"Compare runs in MLflow UI: {mlflow.get_tracking_uri()}")


if __name__ == "__main__":
    run_experiment()