"""
MLflow tracking configuration for RAG experiments.
Sets up connection to MLflow server and MinIO (S3-compatible) for artifact storage.
"""
import os
import mlflow

# === MLflow Tracking Server ===
MLFLOW_TRACKING_URI = "http://localhost:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# === MinIO (S3) for artifacts ===
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"

# === Disable SSL verification for local MinIO ===
os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

# === Experiment names ===
EXPERIMENT_BASE_NAME = "rag-experiments"

# === Registered experiment name mapping ===
EXPERIMENTS = {
    "embedding-comparison": f"{EXPERIMENT_BASE_NAME}-embedding-comparison",
    "retrieval-strategies": f"{EXPERIMENT_BASE_NAME}-retrieval-strategies",
    "chunking-strategies": f"{EXPERIMENT_BASE_NAME}-chunking-strategies",
    "prompt-templates": f"{EXPERIMENT_BASE_NAME}-prompt-templates",
    "end-to-end": f"{EXPERIMENT_BASE_NAME}-end-to-end",
    "performance-benchmark": f"{EXPERIMENT_BASE_NAME}-performance-benchmark",
}


def setup_experiment(experiment_key: str) -> str:
    """
    Set up an MLflow experiment by name.

    Args:
        experiment_key: Key from EXPERIMENTS dict (e.g. "embedding-comparison")

    Returns:
        experiment_name: The full experiment name
    """
    if experiment_key not in EXPERIMENTS:
        raise ValueError(
            f"Unknown experiment key '{experiment_key}'. "
            f"Available: {list(EXPERIMENTS.keys())}"
        )

    experiment_name = EXPERIMENTS[experiment_key]
    mlflow.set_experiment(experiment_name)

    print(f"[OK] MLflow tracking URI: {mlflow.get_tracking_uri()}")
    print(f"[OK] Experiment: {experiment_name}")
    print(f"[OK] MLflow version: {mlflow.__version__}")

    return experiment_name


def get_tracking_uri() -> str:
    return mlflow.get_tracking_uri()