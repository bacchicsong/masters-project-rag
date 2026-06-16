import json
import time
import uuid
from typing import Any

from minio import Minio

from config.config import RAG_CONFIG
from infrastructure.db.qdrand import init_qdrant, insert_document, get_embedded_model
from infrastructure.di.dependencies import get_logger
from tools.prometheus_metrics import (
    RAG_DOCUMENTS_UPSERTED_TOTAL,
    RAG_INGESTION_DURATION,
    RAG_MINIO_OBJECTS_INGESTED_TOTAL,
)


def _minio_client() -> Minio:
    return Minio(
        RAG_CONFIG.MINIO_ENDPOINT,
        access_key=RAG_CONFIG.MINIO_ACCESS_KEY,
        secret_key=RAG_CONFIG.MINIO_SECRET_KEY,
        secure=RAG_CONFIG.MINIO_SECURE,
    )


def _coerce_documents(payload: Any, object_name: str) -> list[dict]:
    if isinstance(payload, list):
        docs = payload
    elif isinstance(payload, dict):
        docs = payload.get("documents") if isinstance(payload.get("documents"), list) else [payload]
    else:
        raise ValueError(f"Unsupported JSON payload in {object_name}: {type(payload)!r}")

    normalized = []
    for idx, doc in enumerate(docs, start=1):
        if not isinstance(doc, dict):
            continue
        text = doc.get("text") or doc.get("content")
        if not text:
            continue
        item = doc.copy()
        item["text"] = text
        meta = item.setdefault("meta", {})
        if isinstance(meta, dict):
            meta.setdefault("minio_object", object_name)
        item.setdefault("chunk_id", idx)
        normalized.append(item)
    return normalized


def _point_id(object_name: str, doc: dict) -> str:
    source = json.dumps(
        {
            "object": object_name,
            "chunk_id": doc.get("chunk_id"),
            "text": doc.get("text", ""),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, source))


def ingest_minio_to_qdrant() -> dict:
    logger = get_logger()
    client = _minio_client()
    qdrant = init_qdrant(logger)
    model = get_embedded_model()

    bucket = RAG_CONFIG.MINIO_DATA_BUCKET
    prefix = RAG_CONFIG.MINIO_DATA_PREFIX

    started = time.time()
    objects_processed = 0
    docs_upserted = 0

    with RAG_INGESTION_DURATION.time():
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
            if not obj.object_name.endswith(".json"):
                continue

            response = client.get_object(bucket, obj.object_name)
            try:
                payload = json.loads(response.read().decode("utf-8"))
            finally:
                response.close()
                response.release_conn()

            docs = _coerce_documents(payload, obj.object_name)
            for doc in docs:
                insert_document(
                    qdrant,
                    RAG_CONFIG.QDRANT_COLLECTION_NAME,
                    doc,
                    _point_id(obj.object_name, doc),
                    model=model,
                )
                docs_upserted += 1

            objects_processed += 1

    RAG_MINIO_OBJECTS_INGESTED_TOTAL.inc(objects_processed)
    RAG_DOCUMENTS_UPSERTED_TOTAL.inc(docs_upserted)
    elapsed = time.time() - started
    logger.info(
        "minio_ingestion_complete",
        extra={
            "bucket": bucket,
            "prefix": prefix,
            "objects_processed": objects_processed,
            "docs_upserted": docs_upserted,
            "duration_s": round(elapsed, 2),
        },
    )
    return {
        "bucket": bucket,
        "prefix": prefix,
        "objects_processed": objects_processed,
        "docs_upserted": docs_upserted,
        "duration_s": round(elapsed, 2),
    }


if __name__ == "__main__":
    print(json.dumps(ingest_minio_to_qdrant(), ensure_ascii=False, indent=2))
