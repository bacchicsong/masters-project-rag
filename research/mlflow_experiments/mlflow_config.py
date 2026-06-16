import os
from pathlib import Path

import mlflow

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRACKING_URI = f"sqlite:///{(PROJECT_ROOT / 'research' / 'presentation_assets' / 'mlflow_tracking.db').as_posix()}"
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", DEFAULT_TRACKING_URI)
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

if MLFLOW_TRACKING_URI.startswith("http"):
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
