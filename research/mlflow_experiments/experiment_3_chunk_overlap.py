import json

import mlflow
from sentence_transformers import SentenceTransformer

from mlflow_config import setup_experiment
from utils.data_loader import load_golden_eval_set
from utils.document_processor import chunk_text, get_text_by_strategy
from utils.eval_runner import build_ground_truth, log_mlflow_metrics, run_chunk_retrieval


EXPERIMENT_KEY = "chunk-overlap"
K_VALUES = [1, 3, 5, 8]
MODEL_ID = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
CHUNK_SIZE = 512
CHUNK_OVERLAPS = [0, 50, 100, 200]
NUM_QUERIES = None


def run_experiment():
    experiment_name = setup_experiment(EXPERIMENT_KEY)
    docs, test_queries, golden_stats = load_golden_eval_set(num_queries=NUM_QUERIES)

    doc_ids = [d["id"] for d in docs]
    ground_truth = build_ground_truth(test_queries)
    query_texts = [q["query"] for q in test_queries]
    query_ids = [q["query_id"] for q in test_queries]
    model = SentenceTransformer(MODEL_ID)
    base_texts = [get_text_by_strategy(d, "full") for d in docs]

    base_params = {
        "chunk_size": CHUNK_SIZE,
        "extraction_strategy": "full",
        "model_id": MODEL_ID,
        "dataset": "Q_A_articles",
        "num_documents": len(docs),
        "num_queries": len(test_queries),
        "k_values": json.dumps(K_VALUES),
    }

    print(f"\n--- chunk overlap (size={CHUNK_SIZE}) ---")

    for overlap in CHUNK_OVERLAPS:
        chunks, chunk_to_doc = [], []
        for doc_idx, text in enumerate(base_texts):
            parts = chunk_text(text, chunk_size=CHUNK_SIZE, overlap=overlap)
            chunks.extend(parts)
            chunk_to_doc.extend([doc_idx] * len(parts))

        with mlflow.start_run(run_name=f"overlap={overlap}"):
            mlflow.log_params({**base_params, "chunk_overlap": overlap, "num_chunks": len(chunks)})
            result = run_chunk_retrieval(
                model, chunks, chunk_to_doc, doc_ids, query_texts, query_ids, ground_truth, K_VALUES,
            )
            metrics = result["metrics"]
            log_mlflow_metrics(metrics)
            mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])
            print(f"   overlap={overlap}: P@5={metrics['mean_P@5']} chunks={len(chunks)}")

    print(f"\ndone: {experiment_name}")


if __name__ == "__main__":
    run_experiment()
