import json
import os
import tempfile
import time

import mlflow
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

from utils.metrics import compute_retrieval_metrics


def build_ground_truth(test_queries):
    return {q["query_id"]: q["relevant_doc_ids"] for q in test_queries}


def log_mlflow_metrics(metrics):
    mlflow.log_metrics({k.replace("@", "_at_"): v for k, v in metrics.items()})


def run_dense_retrieval(model, doc_texts, doc_ids, query_texts, query_ids, ground_truth, k_values, batch_size=16):
    doc_emb = model.encode(
        doc_texts, batch_size=batch_size, show_progress_bar=True,
        convert_to_numpy=True, normalize_embeddings=True,
    )
    query_emb = model.encode(
        query_texts, batch_size=batch_size, show_progress_bar=True,
        convert_to_numpy=True, normalize_embeddings=True,
    )

    top_k = max(k_values)
    predictions = {}
    latencies = []

    for i, qid in enumerate(query_ids):
        t0 = time.perf_counter()
        sims = cosine_similarity(query_emb[i].reshape(1, -1), doc_emb)[0]
        idx = np.argsort(sims)[::-1][:top_k]
        latencies.append(time.perf_counter() - t0)
        predictions[qid] = [doc_ids[j] for j in idx]

    metrics = compute_retrieval_metrics(ground_truth, predictions, k_values)
    metrics["avg_search_latency_s"] = np.mean(latencies)
    metrics["p95_search_latency_s"] = np.percentile(latencies, 95)
    return {"metrics": metrics, "predictions": predictions, "doc_embeddings": doc_emb}


def run_chunk_retrieval(model, chunks, chunk_to_doc, doc_ids, query_texts, query_ids, ground_truth, k_values):
    chunk_emb = model.encode(
        chunks, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True,
    )

    top_k = max(k_values)
    predictions = {}
    latencies = []

    for i, qid in enumerate(query_ids):
        t0 = time.perf_counter()
        q_emb = model.encode([query_texts[i]], normalize_embeddings=True)
        sims = cosine_similarity(q_emb, chunk_emb)[0]
        top_idx = np.argsort(sims)[::-1][:top_k]

        seen = set()
        ranked_docs = []
        for idx in top_idx:
            did = doc_ids[chunk_to_doc[idx]]
            if did not in seen:
                seen.add(did)
                ranked_docs.append(did)

        latencies.append(time.perf_counter() - t0)
        predictions[qid] = ranked_docs

    metrics = compute_retrieval_metrics(ground_truth, predictions, k_values)
    metrics["avg_search_latency_s"] = np.mean(latencies)
    metrics["p95_search_latency_s"] = np.percentile(latencies, 95)
    metrics["num_chunks"] = len(chunks)
    if chunks:
        metrics["avg_chunk_length_tokens"] = np.mean([len(c.split()) for c in chunks])
    return {"metrics": metrics, "predictions": predictions}


def log_retrieval_run(params, metrics, predictions=None, extra=None):
    mlflow.log_params(params)
    log_mlflow_metrics(metrics)

    payload = {"metrics": metrics, "predictions": predictions or {}}
    if extra:
        payload.update(extra)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        path = f.name
    mlflow.log_artifact(path, artifact_path="results")
    os.unlink(path)
