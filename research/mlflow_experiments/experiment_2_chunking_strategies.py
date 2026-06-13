import json
import time

import mlflow
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

from mlflow_config import setup_experiment
from utils.data_loader import load_golden_eval_set
from utils.document_processor import chunk_text, get_text_by_strategy
from utils.eval_runner import build_ground_truth
from utils.metrics import compute_retrieval_metrics


EXPERIMENT_KEY = "chunking-strategies"
K_VALUES = [1, 3, 5, 8]
EMBEDDING_MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
TEXT_STRATEGIES = ["full", "title_headings", "title_only"]
CHUNK_SIZES = [256, 512, 1024]
CHUNK_OVERLAP = 50
NUM_QUERIES = None


def encode_search_evaluate(model, doc_ids, doc_texts, query_texts, query_ids, ground_truth, params, run_name, golden_stats):
    with mlflow.start_run(run_name=run_name):
        mlflow.log_params(params)
        doc_embeddings = model.encode(
            doc_texts, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True
        )

        predictions = {}
        search_times = []
        for qi, qid in enumerate(query_ids):
            start = time.perf_counter()
            q_emb = model.encode([query_texts[qi]], normalize_embeddings=True)
            sims = cosine_similarity(q_emb, doc_embeddings)[0]
            top_indices = np.argsort(sims)[::-1][: max(K_VALUES)]
            search_times.append(time.perf_counter() - start)
            predictions[qid] = [doc_ids[idx] for idx in top_indices]

        metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
        metrics["avg_search_latency_s"] = float(np.mean(search_times))
        metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
        doc_lengths = [len(t.split()) for t in doc_texts]
        metrics["avg_doc_length_tokens"] = float(np.mean(doc_lengths))
        mlflow.log_metrics({k.replace("@", "_at_"): v for k, v in metrics.items()})
        mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])
        print(f"   {run_name}: P@5={metrics['mean_P@5']:.3f} R@5={metrics['mean_R@5']:.3f}")
        return metrics


def run_experiment():
    experiment_name = setup_experiment(EXPERIMENT_KEY)
    docs, test_queries, golden_stats = load_golden_eval_set(num_queries=NUM_QUERIES)

    doc_ids = [doc["id"] for doc in docs]
    ground_truth = build_ground_truth(test_queries)
    query_texts = [tq["query"] for tq in test_queries]
    query_ids = [tq["query_id"] for tq in test_queries]

    model = SentenceTransformer(EMBEDDING_MODEL_ID)

    print(f"\n{'='*60}")
    print("text strategies")
    print(f"{'='*60}")

    for strategy in TEXT_STRATEGIES:
        doc_texts = [get_text_by_strategy(doc, strategy) for doc in docs]
        encode_search_evaluate(
            model, doc_ids, doc_texts, query_texts, query_ids, ground_truth,
            {
                "extraction_strategy": strategy,
                "model_id": EMBEDDING_MODEL_ID,
                "dataset": "Q_A_articles",
                "num_documents": len(docs),
                "num_queries": len(test_queries),
                "k_values": json.dumps(K_VALUES),
            },
            f"strategy-{strategy}",
            golden_stats,
        )

    print(f"\n{'='*60}")
    print("chunk sizes")
    print(f"{'='*60}")

    base_texts = [get_text_by_strategy(doc, "full") for doc in docs]

    for chunk_size in CHUNK_SIZES:
        all_chunks = []
        chunk_to_doc_map = []
        for doc_idx, text in enumerate(base_texts):
            chunks = chunk_text(text, chunk_size=chunk_size, overlap=CHUNK_OVERLAP)
            all_chunks.extend(chunks)
            chunk_to_doc_map.extend([doc_idx] * len(chunks))

        with mlflow.start_run(run_name=f"chunk_size={chunk_size}"):
            mlflow.log_params({
                "chunk_size": chunk_size,
                "chunk_overlap": CHUNK_OVERLAP,
                "extraction_strategy": "full",
                "model_id": EMBEDDING_MODEL_ID,
                "dataset": "Q_A_articles",
                "num_documents": len(docs),
                "num_chunks": len(all_chunks),
                "num_queries": len(test_queries),
                "k_values": json.dumps(K_VALUES),
            })

            chunk_embeddings = model.encode(
                all_chunks, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True
            )

            predictions = {}
            search_times = []
            for qi, qid in enumerate(query_ids):
                start = time.perf_counter()
                q_emb = model.encode([query_texts[qi]], normalize_embeddings=True)
                sims = cosine_similarity(q_emb, chunk_embeddings)[0]
                top_indices = np.argsort(sims)[::-1][: max(K_VALUES)]
                seen = set()
                sorted_doc_ids = []
                for idx in top_indices:
                    did = doc_ids[chunk_to_doc_map[idx]]
                    if did not in seen:
                        seen.add(did)
                        sorted_doc_ids.append(did)
                predictions[qid] = sorted_doc_ids
                search_times.append(time.perf_counter() - start)

            metrics = compute_retrieval_metrics(ground_truth, predictions, K_VALUES)
            metrics["avg_search_latency_s"] = float(np.mean(search_times))
            metrics["p95_search_latency_s"] = float(np.percentile(search_times, 95))
            metrics["num_chunks"] = len(all_chunks)
            avg_chunk_length = float(np.mean([len(c.split()) for c in all_chunks]))
            metrics["avg_chunk_length_tokens"] = avg_chunk_length
            mlflow.log_metrics({k.replace("@", "_at_"): v for k, v in metrics.items()})
            mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])
            print(f"   chunk={chunk_size}: P@5={metrics['mean_P@5']:.3f} chunks={len(all_chunks)}")

    print(f"\n[COMPLETE] {experiment_name}")


if __name__ == "__main__":
    run_experiment()
