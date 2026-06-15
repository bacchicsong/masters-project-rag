import os

import mlflow

MLFLOW_TRACKING_URI = "http://localhost:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"
os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

EXPERIMENT_BASE_NAME = "rag-experiments-golden"

EXPERIMENTS = {
    "embedding-comparison": f"{EXPERIMENT_BASE_NAME}-embedding-comparison",
    "chunking-strategies": f"{EXPERIMENT_BASE_NAME}-chunking-strategies",
    "chunk-overlap": f"{EXPERIMENT_BASE_NAME}-chunk-overlap",
}


def setup_experiment(experiment_key):
    if experiment_key not in EXPERIMENTS:
        raise ValueError(f"unknown experiment: {experiment_key}")
    name = EXPERIMENTS[experiment_key]
    mlflow.set_experiment(name)
    print(f"[OK] {name}")
    return name
