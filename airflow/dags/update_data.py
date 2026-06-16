from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
import urllib.request

from airflow import DAG
from airflow.operators.python import PythonOperator
from minio import Minio

sys.path.append("/opt/airflow/funcs")

from daily_parsing import main as run_parser, save_normalized_chunks


RAW_DIR = Path(os.getenv("PARSER_OUTPUT_DIR", "/opt/airflow/tbank_knowledge"))
NORMALIZED_DIR = Path(os.getenv("NORMALIZED_OUTPUT_DIR", "/opt/airflow/normalized"))
FASTAPI_INTERNAL_URL = os.getenv("FASTAPI_INTERNAL_URL", "http://app:8088/api/v1")
INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN", "local-dev-token")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_DATA_BUCKET = os.getenv("MINIO_DATA_BUCKET", "rag-data")
MINIO_DATA_PREFIX = os.getenv("MINIO_DATA_PREFIX", "parsed/")


def _minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def _post_internal(path: str) -> dict:
    req = urllib.request.Request(
        f"{FASTAPI_INTERNAL_URL}{path}",
        method="POST",
        headers={"X-Internal-Token": INTERNAL_API_TOKEN},
    )
    with urllib.request.urlopen(req, timeout=3600) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {}


def parse_and_normalize_task(**context) -> dict:
    articles = run_parser()
    output_file = NORMALIZED_DIR / f"tbank_articles_{context['ds_nodash']}.json"
    result = save_normalized_chunks(articles, output_file)
    print(json.dumps(result, ensure_ascii=False))
    return result


def upload_to_minio_task(**context) -> dict:
    parse_result = context["ti"].xcom_pull(task_ids="parse_and_normalize")
    output_file = Path(parse_result["output_file"])
    object_name = f"{MINIO_DATA_PREFIX}{output_file.name}"

    client = _minio_client()
    if not client.bucket_exists(MINIO_DATA_BUCKET):
        client.make_bucket(MINIO_DATA_BUCKET)

    client.fput_object(
        MINIO_DATA_BUCKET,
        object_name,
        str(output_file),
        content_type="application/json",
    )

    result = {
        "bucket": MINIO_DATA_BUCKET,
        "object_name": object_name,
        "chunks": parse_result["chunks"],
        "articles": parse_result["articles"],
    }
    print(json.dumps(result, ensure_ascii=False))
    return result


def ingest_qdrant_task() -> dict:
    result = _post_internal("/admin/ingest/minio")
    print(json.dumps(result, ensure_ascii=False))
    return result


def fine_tune_feedback_task() -> dict:
    result = _post_internal("/admin/fine-tune-feedback")
    print(json.dumps(result, ensure_ascii=False))
    return result


default_args = {
    "owner": "clprm",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 9),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "daily_data_update",
    default_args=default_args,
    description="Parse T-Bank articles, upload normalized chunks to MinIO, refresh Qdrant, fine-tune from feedback",
    schedule="0 0 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["tbank", "minio", "qdrant", "feedback"],
) as dag:
    parse_and_normalize = PythonOperator(
        task_id="parse_and_normalize",
        python_callable=parse_and_normalize_task,
        execution_timeout=timedelta(hours=2),
    )

    upload_to_minio = PythonOperator(
        task_id="upload_to_minio",
        python_callable=upload_to_minio_task,
        execution_timeout=timedelta(minutes=30),
    )

    ingest_qdrant = PythonOperator(
        task_id="ingest_qdrant",
        python_callable=ingest_qdrant_task,
        execution_timeout=timedelta(hours=2),
    )

    fine_tune_feedback = PythonOperator(
        task_id="fine_tune_feedback",
        python_callable=fine_tune_feedback_task,
        execution_timeout=timedelta(hours=4),
    )

    parse_and_normalize >> upload_to_minio >> ingest_qdrant >> fine_tune_feedback
