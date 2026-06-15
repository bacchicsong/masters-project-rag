import json
import time

import mlflow
from sentence_transformers import SentenceTransformer

from mlflow_config import setup_experiment
from utils.data_loader import load_golden_eval_set
from utils.document_processor import get_text_by_strategy
from utils.eval_runner import build_ground_truth, log_retrieval_run, run_dense_retrieval


EXPERIMENT_KEY = "embedding-comparison"
K_VALUES = [1, 3, 5, 8]
NUM_QUERIES = None

EMBEDDING_MODELS = [
    ("paraphrase-multilingual-MiniLM-L12-v2", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"),
    ("multilingual-e5-small", "intfloat/multilingual-e5-small"),
    ("multilingual-e5-base", "intfloat/multilingual-e5-base"),
    ("distiluse-base-multilingual-cased-v2", "sentence-transformers/distiluse-base-multilingual-cased-v2"),
    ("sbert_large_nlu_rus", "sberbank-ai/sbert_large_nlu_rus"),
]


def run_experiment():
    experiment_name = setup_experiment(EXPERIMENT_KEY)
    docs, test_queries, golden_stats = load_golden_eval_set(num_queries=NUM_QUERIES)

    doc_ids = [d["id"] for d in docs]
    doc_texts = [get_text_by_strategy(d, "full") for d in docs]
    ground_truth = build_ground_truth(test_queries)
    query_texts = [q["query"] for q in test_queries]
    query_ids = [q["query_id"] for q in test_queries]

    base_params = {
        "strategy": "full",
        "dataset": "Q_A_articles",
        "num_documents": len(docs),
        "num_queries": len(test_queries),
        "k_values": json.dumps(K_VALUES),
    }

    for name, model_id in EMBEDDING_MODELS:
        print(f"\n--- {name} ---")

        with mlflow.start_run(run_name=name):
            t0 = time.perf_counter()
            model = SentenceTransformer(model_id)
            load_time = time.perf_counter() - t0

            t0 = time.perf_counter()
            result = run_dense_retrieval(
                model, doc_texts, doc_ids, query_texts, query_ids, ground_truth, K_VALUES,
            )
            eval_time = time.perf_counter() - t0
            metrics = result["metrics"]

            mlflow.log_metric("model_load_time_s", load_time)
            mlflow.log_metric("eval_time_s", eval_time)
            mlflow.log_metric("embedding_dim", result["doc_embeddings"].shape[1])
            mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])

            log_retrieval_run(
                params={**base_params, "model_name": name, "model_id": model_id},
                metrics=metrics,
                predictions=result["predictions"],
                extra={"golden_stats": golden_stats},
            )

            print(f"   P@5={metrics['mean_P@5']} R@5={metrics['mean_R@5']}")

    print(f"\ndone: {experiment_name}")


if __name__ == "__main__":
    run_experiment()
