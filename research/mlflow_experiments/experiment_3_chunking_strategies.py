"""
Experiment 3: Chunking / Context Strategies
============================================
Compare different text extraction strategies (full, title_headings, title_only)
and chunking parameters for document representation.
Logs params, metrics, and artifacts to MLflow → MinIO.

Techniques tested:
1. Text extraction strategies: full, title_headings, title_only
2. Chunk size variation (for full strategy)
3. Impact of chunk overlap
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

from mlflow_config import setup_experiment
from utils.metrics import compute_retrieval_metrics
from utils.data_loader import load_test_queries
from utils.document_processor import get_text_by_strategy, DocumentProcessor


# === Configuration ===
EXPERIMENT_KEY = "chunking-strategies"
K_VALUES = [1, 3, 5, 8]

EMBEDDING_MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

# Text extraction strategies to test
TEXT_STRATEGIES = ["full", "title_headings", "title_only"]

# Chunking parameters to test (only for 'full' strategy)
CHUNK_SIZES = [256, 512, 1024]
CHUNK_OVERLAP = 50

NUM_QUERIES = None


def encode_search_evaluate(
    model: SentenceTransformer,
    doc_ids: List[str],
    doc_texts: List[str],
    query_texts: List[str],
    query_ids: List[str],
    ground_truth: Dict[str, List[str]],
    params: Dict[str, Any],
    run_name: str,
):
    """Encode, search, and evaluate, logging to MLflow."""
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(params)

        # Encode documents
        doc_embeddings = model.encode(
            doc_texts, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True
        )

        # Encode queries and search
        predictions = {}
        search_times = []

        for qi, qid in enumerate(query_ids):
            start_time = time.perf_counter()
            q_emb = model.encode([query_texts[qi]], normalize_embeddings=True)
            sims = cosine_similarity(q_emb, doc_embeddings)[0]
            top_indices = np.argsort(sims)[::-1][:max(K_VALUES)]
            search_time = time.perf_counter() - start_time
            search_times.append(search_time)
            predictions[qid] = [doc_ids[idx] for idx in top_indices]

        # Compute metrics
        metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
        metrics["avg_search_latency_s"] = float(np.mean(search_times))
        metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
        mlflow.log_metrics(metrics)

        # Log doc_text stats
        doc_lengths = [len(t.split()) for t in doc_texts]
        metrics["avg_doc_length_tokens"] = float(np.mean(doc_lengths))
        metrics["total_doc_tokens"] = int(np.sum(doc_lengths))
        mlflow.log_metrics({
            "avg_doc_length_tokens": float(np.mean(doc_lengths)),
            "total_doc_tokens": int(np.sum(doc_lengths)),
        })

        print(f"   [OK] {run_name}: "
              f"P@1={metrics['mean_P@1']:.3f}, R@1={metrics['mean_R@1']:.3f}, "
              f"P@5={metrics['mean_P@5']:.3f}, R@5={metrics['mean_R@5']:.3f}")

        return metrics


def run_experiment():
    """Run the chunking/context strategies experiment."""
    experiment_name = setup_experiment(EXPERIMENT_KEY)

    # Load test data (use real data for more realistic chunking experiment)
    docs, test_queries = load_test_queries(use_mock=True, num_queries=NUM_QUERIES)

    doc_ids = [doc.get("id", str(i)) for i, doc in enumerate(docs)]

    # Build ground truth dict
    ground_truth = {}
    for tq in test_queries:
        ground_truth[tq["query_id"]] = tq["relevant_doc_ids"]

    query_texts = [tq["query"] for tq in test_queries]
    query_ids = [tq["query_id"] for tq in test_queries]

    # Load embedding model once
    print(f"[PACKAGE] Loading embedding model: {EMBEDDING_MODEL_ID}")
    model = SentenceTransformer(EMBEDDING_MODEL_ID)

    # ============================
    # Part 1: Text extraction strategies comparison
    # ============================
    print(f"\n{'='*60}")
    print("[RESULTS] Part 1: Text Extraction Strategies")
    print(f"{'='*60}")

    for strategy in TEXT_STRATEGIES:
        run_name = f"strategy-{strategy}"
        print(f"\n   Testing strategy: {strategy}")

        doc_texts = [get_text_by_strategy(doc, strategy) for doc in docs]

        encode_search_evaluate(
            model, doc_ids, doc_texts, query_texts, query_ids, ground_truth,
            params={
                "extraction_strategy": strategy,
                "model_id": EMBEDDING_MODEL_ID,
                "num_documents": len(docs),
                "num_queries": len(test_queries),
                "k_values": json.dumps(K_VALUES),
                "experiment_part": "text-extraction-strategies",
            },
            run_name=run_name,
        )

    # ============================
    # Part 2: Chunk size variation (full strategy)
    # ============================
    print(f"\n{'='*60}")
    print("[RESULTS] Part 2: Chunk Size Variation")
    print(f"{'='*60}")

    base_texts = [get_text_by_strategy(doc, "full") for doc in docs]

    for chunk_size in CHUNK_SIZES:
        run_name = f"chunk_size={chunk_size}"
        print(f"\n   Testing chunk size: {chunk_size}")

        # Chunk all documents and flatten
        all_chunks = []
        chunk_to_doc_map = []

        for doc_idx, text in enumerate(base_texts):
            chunks = DocumentProcessor.chunk_text(
                text, chunk_size=chunk_size, overlap=CHUNK_OVERLAP
            )
            all_chunks.extend(chunks)
            chunk_to_doc_map.extend([doc_idx] * len(chunks))

        # For chunked evaluation, document IDs need to map back to original docs
        chunk_ids = [f"{doc_ids[doc_idx]}_chunk{ci}" for ci, doc_idx in enumerate(chunk_to_doc_map)]

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "chunk_size": chunk_size,
                "chunk_overlap": CHUNK_OVERLAP,
                "extraction_strategy": "full",
                "model_id": EMBEDDING_MODEL_ID,
                "num_documents": len(docs),
                "num_chunks": len(all_chunks),
                "num_queries": len(test_queries),
                "k_values": json.dumps(K_VALUES),
                "experiment_part": "chunk-size-variation",
            })

            # Encode chunks
            chunk_embeddings = model.encode(
                all_chunks, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True
            )

            # Search across chunks
            predictions = {}
            search_times = []

            for qi, qid in enumerate(query_ids):
                start_time = time.perf_counter()
                q_emb = model.encode([query_texts[qi]], normalize_embeddings=True)
                sims = cosine_similarity(q_emb, chunk_embeddings)[0]
                top_indices = np.argsort(sims)[::-1][:max(K_VALUES)]

                # Map chunk indices back to document IDs (deduplicate)
                seen_docs = set()
                sorted_doc_ids = []
                for idx in top_indices:
                    doc_id = doc_ids[chunk_to_doc_map[idx]]
                    if doc_id not in seen_docs:
                        seen_docs.add(doc_id)
                        sorted_doc_ids.append(doc_id)
                predictions[qid] = sorted_doc_ids

                search_time = time.perf_counter() - start_time
                search_times.append(search_time)

            metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
            metrics["avg_search_latency_s"] = float(np.mean(search_times))
            metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
            metrics["num_chunks"] = len(all_chunks)
            mlflow.log_metrics(metrics)

            avg_chunk_length = float(np.mean([len(c.split()) for c in all_chunks]))
            mlflow.log_metric("avg_chunk_length_tokens", avg_chunk_length)

            print(f"   [OK] {run_name}: "
                  f"P@1={metrics['mean_P@1']:.3f}, R@1={metrics['mean_R@1']:.3f}, "
                  f"chunks={len(all_chunks)}, avg_len={avg_chunk_length:.0f} tokens")

    print(f"\n[OK] Experiment '{experiment_name}' completed!")


if __name__ == "__main__":
    run_experiment()