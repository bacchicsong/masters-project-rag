import json
import time
from typing import List

import mlflow
from sentence_transformers import SentenceTransformer

from mlflow_config import setup_experiment
from utils.data_loader import load_golden_eval_set
from utils.document_processor import get_text_by_strategy
from utils.eval_runner import build_ground_truth, log_retrieval_run, run_dense_retrieval


EXPERIMENT_KEY = "embedding-comparison"
K_VALUES = [1, 3, 5, 8]
DEFAULT_STRATEGY = "full"
NUM_QUERIES = None

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
    {
        "name": "sbert_large_nlu_rus",
        "model_id": "sberbank-ai/sbert_large_nlu_rus",
        "expected_dim": 1024,
    },
]


def run_experiment():
    experiment_name = setup_experiment(EXPERIMENT_KEY)
    docs, test_queries, golden_stats = load_golden_eval_set(num_queries=NUM_QUERIES)

    doc_ids = [doc["id"] for doc in docs]
    doc_texts = [get_text_by_strategy(doc, DEFAULT_STRATEGY) for doc in docs]
    ground_truth = build_ground_truth(test_queries)
    query_texts = [tq["query"] for tq in test_queries]
    query_ids = [tq["query_id"] for tq in test_queries]

    for model_cfg in EMBEDDING_MODELS:
        model_name = model_cfg["name"]
        model_id = model_cfg["model_id"]

        print(f"\n{'='*60}")
        print(f"[EXP] {model_name}")
        print(f"{'='*60}")

        with mlflow.start_run(run_name=model_name):
            start = time.perf_counter()
            model = SentenceTransformer(model_id)
            load_time = time.perf_counter() - start

            start = time.perf_counter()
            result = run_dense_retrieval(
                model, doc_texts, doc_ids, query_texts, query_ids, ground_truth, K_VALUES
            )
            total_time = time.perf_counter() - start
            metrics = result["metrics"]

            mlflow.log_metric("model_load_time_s", load_time)
            mlflow.log_metric("eval_time_s", total_time)
            mlflow.log_metric("actual_embedding_dim", result["doc_embeddings"].shape[1])
            mlflow.log_metric("golden_matched_queries", golden_stats["matched_queries"])
            mlflow.log_metric("golden_used_queries", golden_stats["used_queries"])

            log_retrieval_run(
                params={
                    "model_name": model_name,
                    "model_id": model_id,
                    "strategy": DEFAULT_STRATEGY,
                    "dataset": "Q_A_articles",
                    "num_documents": len(docs),
                    "num_queries": len(test_queries),
                    "k_values": json.dumps(K_VALUES),
                },
                metrics=metrics,
                predictions=result["predictions"],
                extra={"golden_stats": golden_stats},
            )

            print(f"   P@5={metrics['mean_P@5']} R@5={metrics['mean_R@5']} MRR={metrics['MRR']}")

    print(f"\n[COMPLETE] {experiment_name}")


if __name__ == "__main__":
    run_experiment()
