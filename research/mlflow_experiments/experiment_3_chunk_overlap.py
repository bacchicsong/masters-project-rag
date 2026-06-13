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


EXPERIMENT_KEY = "chunk-overlap"
K_VALUES = [1, 3, 5, 8]
EMBEDDING_MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
CHUNK_SIZE = 512
CHUNK_OVERLAPS = [0, 50, 100, 200]
NUM_QUERIES = None


def run_experiment():
    experiment_name = setup_experiment(EXPERIMENT_KEY)
    docs, test_queries, golden_stats = load_golden_eval_set(num_queries=NUM_QUERIES)

    doc_ids = [doc["id"] for doc in docs]
    ground_truth = build_ground_truth(test_queries)
    query_texts = [tq["query"] for tq in test_queries]
    query_ids = [tq["query_id"] for tq in test_queries]

    model = SentenceTransformer(EMBEDDING_MODEL_ID)
    base_texts = [get_text_by_strategy(doc, "full") for doc in docs]

    print(f"\n{'='*60}")
    print(f"chunk overlap (size={CHUNK_SIZE})")
    print(f"{'='*60}")

    for overlap in CHUNK_OVERLAPS:
        all_chunks = []
        chunk_to_doc_map = []
        for doc_idx, text in enumerate(base_texts):
            chunks = chunk_text(text, chunk_size=CHUNK_SIZE, overlap=overlap)
            all_chunks.extend(chunks)
            chunk_to_doc_map.extend([doc_idx] * len(chunks))

        with mlflow.start_run(run_name=f"overlap={overlap}"):
            mlflow.log_params({
                "chunk_size": CHUNK_SIZE,
                "chunk_overlap": overlap,
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
            metrics["avg_search_latency_s"] = np.mean(search_times)
            metrics["p95_search_latency_s"] = np.percentile(search_times, 95)
            metrics["num_chunks"] = len(all_chunks)
            metrics["avg_chunk_length_tokens"] = np.mean([len(c.split()) for c in all_chunks])
            mlflow.log_metrics({k.replace("@", "_at_"): v for k, v in metrics.items()})
            mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])
            print(f"   overlap={overlap}: P@5={metrics['mean_P@5']} chunks={len(all_chunks)}")

    print(f"\n[COMPLETE] {experiment_name}")


if __name__ == "__main__":
    run_experiment()
