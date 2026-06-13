import json

import mlflow
import numpy as np
from sentence_transformers import SentenceTransformer

from mlflow_config import setup_experiment
from utils.data_loader import load_golden_eval_set
from utils.document_processor import chunk_text, get_text_by_strategy
from utils.eval_runner import build_ground_truth, log_mlflow_metrics, run_chunk_retrieval, run_dense_retrieval


EXPERIMENT_KEY = "chunking-strategies"
K_VALUES = [1, 3, 5, 8]
MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
TEXT_STRATEGIES = ["full", "title_headings", "title_only"]
CHUNK_SIZES = [256, 512, 1024]
CHUNK_OVERLAP = 50
NUM_QUERIES = None


def run_experiment():
    experiment_name = setup_experiment(EXPERIMENT_KEY)
    docs, test_queries, golden_stats = load_golden_eval_set(num_queries=NUM_QUERIES)

    doc_ids = [d["id"] for d in docs]
    ground_truth = build_ground_truth(test_queries)
    query_texts = [q["query"] for q in test_queries]
    query_ids = [q["query_id"] for q in test_queries]
    model = SentenceTransformer(MODEL_ID)

    base_params = {
        "model_id": MODEL_ID,
        "dataset": "Q_A_articles",
        "num_documents": len(docs),
        "num_queries": len(test_queries),
        "k_values": json.dumps(K_VALUES),
    }

    print("\n--- text strategies ---")
    for strategy in TEXT_STRATEGIES:
        doc_texts = [get_text_by_strategy(d, strategy) for d in docs]
        with mlflow.start_run(run_name=f"strategy-{strategy}"):
            mlflow.log_params({**base_params, "extraction_strategy": strategy})
            result = run_dense_retrieval(
                model, doc_texts, doc_ids, query_texts, query_ids, ground_truth, K_VALUES,
            )
            metrics = result["metrics"]
            metrics["avg_doc_length_tokens"] = np.mean([len(t.split()) for t in doc_texts])
            log_mlflow_metrics(metrics)
            mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])
            print(f"   {strategy}: P@5={metrics['mean_P@5']} R@5={metrics['mean_R@5']}")

    print("\n--- chunk sizes ---")
    base_texts = [get_text_by_strategy(d, "full") for d in docs]

    for chunk_size in CHUNK_SIZES:
        chunks, chunk_to_doc = [], []
        for doc_idx, text in enumerate(base_texts):
            parts = chunk_text(text, chunk_size=chunk_size, overlap=CHUNK_OVERLAP)
            chunks.extend(parts)
            chunk_to_doc.extend([doc_idx] * len(parts))

        with mlflow.start_run(run_name=f"chunk_size={chunk_size}"):
            mlflow.log_params({
                **base_params,
                "chunk_size": chunk_size,
                "chunk_overlap": CHUNK_OVERLAP,
                "extraction_strategy": "full",
                "num_chunks": len(chunks),
            })
            result = run_chunk_retrieval(
                model, chunks, chunk_to_doc, doc_ids, query_texts, query_ids, ground_truth, K_VALUES,
            )
            metrics = result["metrics"]
            log_mlflow_metrics(metrics)
            mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])
            print(f"   size={chunk_size}: P@5={metrics['mean_P@5']} chunks={len(chunks)}")

    print(f"\ndone: {experiment_name}")


if __name__ == "__main__":
    run_experiment()
