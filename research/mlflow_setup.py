"""
Настройка подключения к MLflow для экспериментов.
Импортируй этот файл в начале Jupyter-блокнота или используй как шаблон.
"""
import os
import mlflow

# === MLflow Tracking Server ===
MLFLOW_TRACKING_URI = "http://localhost:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# === MinIO (S3) для артефактов ===
os.environ["MLFLOW_S3_ENDPOINT_URL"] = "http://localhost:9000"
os.environ["AWS_ACCESS_KEY_ID"] = "minioadmin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "minioadmin"

# === Название эксперимента ===
EXPERIMENT_NAME = "rag-experiments"
mlflow.set_experiment(EXPERIMENT_NAME)

print(f"✅ MLflow tracking URI: {mlflow.get_tracking_uri()}")
print(f"✅ Experiment: {EXPERIMENT_NAME}")
print(f"✅ MLflow version: {mlflow.__version__}")