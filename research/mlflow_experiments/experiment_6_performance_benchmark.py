"""
Experiment 6: Performance Benchmark
====================================
Profile resource utilization and latency for different RAG components.
Measures wall time, memory (Python + RSS), GPU memory if available,
and throughput metrics.

Techniques tested:
1. Embedding encoding performance (batch size sweep)
2. Search latency (different vector index sizes)
3. Full pipeline profiling (retrieval + generation)
4. BM25 indexing vs dense encoding comparison
"""
import json
import time
import tracemalloc
import os
import sys
from typing import List, Dict, Any, Optional
import functools

import mlflow
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from rank_bm25 import BM25Okapi

from mlflow_config import setup_experiment
from utils.metrics import compute_performance_metrics
from utils.data_loader import load_test_queries
from utils.document_processor import get_text_by_strategy


# === Configuration ===
EXPERIMENT_KEY = "performance-benchmark"

MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
STRATEGY = "full"

# Batch sizes to test
BATCH_SIZES = [1, 8, 16, 32, 64]
NUM_DOCS_VALUES = [10, 50, 100, 200]

# Test queries
TEST_QUERIES_LIST = [
    "Что такое акция?",
    "Как работает ИИС?",
    "Чем отличаются обыкновенные акции от привилегированных?",
    "Какой налог на доход от инвестиций?",
    "Что такое дивиденды?",
]


def measure_performance(func):
    """
    Decorator that measures wall time, peak memory, RSS, and GPU memory.
    Adapted from the existing bi_encoder.py implementation.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Start tracing memory
        tracemalloc_started = False
        try:
            tracemalloc.start()
            tracemalloc_started = True
        except Exception:
            pass

        start = time.perf_counter()

        # RSS before
        rss_before = None
        try:
            import psutil
            rss_before = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        except Exception:
            pass

        # GPU before
        gpu_before = None
        try:
            import torch
            if torch.cuda.is_available():
                gpu_before = torch.cuda.memory_allocated() / (1024 * 1024)
        except Exception:
            pass

        # Execute function
        result = func(*args, **kwargs)

        end = time.perf_counter()

        # Peak memory
        peak_kib = None
        if tracemalloc_started:
            current, peak = tracemalloc.get_traced_memory()
            peak_kib = peak / 1024.0
            tracemalloc.stop()

        # RSS after
        rss_after = None
        try:
            import psutil
            rss_after = psutil.Process(os.getpid()).memory_info().rss / (1024 * 1024)
        except Exception:
            pass

        # GPU after
        gpu_after = None
        try:
            import torch
            if torch.cuda.is_available():
                gpu_after = torch.cuda.memory_allocated() / (1024 * 1024)
        except Exception:
            pass

        perf_info = {
            "wall_time_s": end - start,
            "peak_memory_kib": peak_kib,
            "rss_mb_before": rss_before,
            "rss_mb_after": rss_after,
            "gpu_mb_before": gpu_before,
            "gpu_mb_after": gpu_after,
        }

        return result, perf_info

    return wrapper


@measure_performance
def encode_documents(model: SentenceTransformer, texts: List[str], batch_size: int = 16):
    """Encode documents with performance measurement."""
    return model.encode(
        texts,
        batch_size=batch_size,
        show_progress_bar=False,
        convert_to_numpy=True,
        normalize_embeddings=True,
    )


@measure_performance
def search_query(model: SentenceTransformer, query: str, doc_embeddings: np.ndarray, top_k: int = 10):
    """Search with performance measurement."""
    q_emb = model.encode([query], normalize_embeddings=True)
    sims = cosine_similarity(q_emb, doc_embeddings)[0]
    top_indices = np.argsort(sims)[::-1][:top_k]
    return top_indices.tolist()


@measure_performance
def build_bm25_index(texts: List[str]):
    """Build BM25 index with performance measurement."""
    tokenized = [t.lower().split() for t in texts]
    return BM25Okapi(tokenized)


def run_experiment():
    """Run the performance benchmark experiment."""
    experiment_name = setup_experiment(EXPERIMENT_KEY)

    # Load documents
    docs, _ = load_test_queries(use_mock=True)
    all_doc_texts = [get_text_by_strategy(doc, STRATEGY) for doc in docs]

    # Load model once
    print(f"[PACKAGE] Loading model: {MODEL_ID}")
    model = SentenceTransformer(MODEL_ID)

    # ============================
    # Part 1: Batch Size Encoding Performance
    # ============================
    print(f"\n{'='*60}")
    print("[RESULTS] Part 1: Batch Size Encoding Performance")
    print(f"{'='*60}")

    for batch_size in BATCH_SIZES:
        run_name = f"encoding_batch={batch_size}"
        print(f"\n   Testing batch size: {batch_size}")

        # Use a subset of documents for quicker testing
        subset_texts = all_doc_texts[:50]

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "benchmark_type": "encoding_batch_size",
                "model_id": MODEL_ID,
                "batch_size": batch_size,
                "num_documents": len(subset_texts),
            })

            # Warm-up run
            _ = model.encode(subset_texts[:5], batch_size=batch_size,
                             show_progress_bar=False, convert_to_numpy=True,
                             normalize_embeddings=True)

            # Measured run
            embeddings, perf_info = encode_documents(model, subset_texts, batch_size)

            # Log performance metrics
            perf_metrics = compute_performance_metrics(
                wall_time=perf_info["wall_time_s"],
                peak_memory_kib=perf_info["peak_memory_kib"] or 0,
                rss_before=perf_info["rss_mb_before"],
                rss_after=perf_info["rss_mb_after"],
                gpu_before=perf_info["gpu_mb_before"],
                gpu_after=perf_info["gpu_mb_after"],
                num_docs=len(subset_texts),
            )
            mlflow.log_metrics(perf_metrics)

            print(f"   [OK] {run_name}: {perf_metrics['wall_time_s']:.4f}s, "
                  f"mem={perf_metrics.get('peak_memory_mb', 0):.2f}MB, "
                  f"throughput={perf_metrics.get('throughput_docs_per_sec', 0):.1f} docs/s")

    # ============================
    # Part 2: Document Count Scaling
    # ============================
    print(f"\n{'='*60}")
    print("[RESULTS] Part 2: Document Count Scaling")
    print(f"{'='*60}")

    for num_docs in NUM_DOCS_VALUES:
        run_name = f"scaling_docs={num_docs}"
        print(f"\n   Testing with {num_docs} documents")

        # Pad or truncate documents to reach desired count
        if num_docs <= len(all_doc_texts):
            subset_texts = all_doc_texts[:num_docs]
        else:
            # Repeat documents to reach desired count
            subset_texts = (all_doc_texts * (num_docs // len(all_doc_texts) + 1))[:num_docs]

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "benchmark_type": "document_count_scaling",
                "model_id": MODEL_ID,
                "num_documents": num_docs,
                "batch_size": 16,
            })

            # Encode
            embeddings, encode_perf = encode_documents(model, subset_texts, batch_size=16)

            # Search performance: average over test queries
            search_times = []
            for q in TEST_QUERIES_LIST:
                _, search_perf = search_query(model, q, embeddings, top_k=10)
                search_times.append(search_perf["wall_time_s"])

            # Log encoding metrics
            encode_metrics = compute_performance_metrics(
                wall_time=encode_perf["wall_time_s"],
                peak_memory_kib=encode_perf["peak_memory_kib"] or 0,
                rss_before=encode_perf["rss_mb_before"],
                rss_after=encode_perf["rss_mb_after"],
                num_docs=num_docs,
            )
            mlflow.log_metrics({
                f"encode_{k}": v for k, v in encode_metrics.items()
            })

            # Log search metrics
            mlflow.log_metrics({
                "avg_search_time_s": float(np.mean(search_times)),
                "min_search_time_s": float(np.min(search_times)),
                "max_search_time_s": float(np.max(search_times)),
                "p95_search_time_s": float(np.percentile(search_times, 95)),
            })

            print(f"   [OK] {run_name}: encode={encode_metrics['wall_time_s']:.4f}s, "
                  f"search={np.mean(search_times):.6f}s (avg), "
                  f"mem={encode_metrics.get('peak_memory_mb', 0):.2f}MB")

    # ============================
    # Part 3: BM25 vs Dense Encoding Performance
    # ============================
    print(f"\n{'='*60}")
    print("[RESULTS] Part 3: BM25 vs Dense Encoding Performance")
    print(f"{'='*60}")

    subset_texts = all_doc_texts[:len(docs)]

    with mlflow.start_run(run_name="bm25-indexing"):
        mlflow.log_params({
            "benchmark_type": "indexing_comparison",
            "index_type": "bm25",
            "num_documents": len(subset_texts),
        })

        _, bm25_perf = build_bm25_index(subset_texts)

        bm25_metrics = compute_performance_metrics(
            wall_time=bm25_perf["wall_time_s"],
            peak_memory_kib=bm25_perf["peak_memory_kib"] or 0,
            rss_before=bm25_perf["rss_mb_before"],
            rss_after=bm25_perf["rss_mb_after"],
            num_docs=len(subset_texts),
        )
        mlflow.log_metrics({f"bm25_{k}": v for k, v in bm25_metrics.items()})
        print(f"   [OK] BM25 indexing: {bm25_metrics['wall_time_s']:.4f}s, "
              f"mem={bm25_metrics.get('peak_memory_mb', 0):.2f}MB")

    with mlflow.start_run(run_name="dense-encoding"):
        mlflow.log_params({
            "benchmark_type": "indexing_comparison",
            "index_type": "dense_embedding",
            "model_id": MODEL_ID,
            "num_documents": len(subset_texts),
            "batch_size": 16,
        })

        embeddings, dense_perf = encode_documents(model, subset_texts, batch_size=16)

        dense_metrics = compute_performance_metrics(
            wall_time=dense_perf["wall_time_s"],
            peak_memory_kib=dense_perf["peak_memory_kib"] or 0,
            rss_before=dense_perf["rss_mb_before"],
            rss_after=dense_perf["rss_mb_after"],
            gpu_before=dense_perf["gpu_mb_before"],
            gpu_after=dense_perf["gpu_mb_after"],
            num_docs=len(subset_texts),
        )
        mlflow.log_metrics({f"dense_{k}": v for k, v in dense_metrics.items()})
        print(f"   [OK] Dense encoding: {dense_metrics['wall_time_s']:.4f}s, "
              f"mem={dense_metrics.get('peak_memory_mb', 0):.2f}MB, "
              f"embedding_dim={embeddings.shape[1]}")

        # Log embedding info artifact
        mlflow.log_text(
            json.dumps({
                "embedding_shape": list(embeddings.shape),
                "dtype": str(embeddings.dtype),
                "memory_mb": embeddings.nbytes / (1024 * 1024),
            }, ensure_ascii=False, indent=2),
            "embedding_info.json"
        )

    # ============================
    # Summary
    # ============================
    print(f"\n{'='*60}")
    print(f"[OK] Experiment '{experiment_name}' completed!")
    print(f"{'='*60}")
    print(f"Check MLflow UI: {mlflow.get_tracking_uri()}")


if __name__ == "__main__":
    run_experiment()