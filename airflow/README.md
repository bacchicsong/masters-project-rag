# Airflow pipeline

Airflow is now part of the root `docker-compose.yaml`. Do not start a separate
compose file from this directory.

Start everything from the repository root:

```bash
docker compose up --build
```

UI: <http://localhost:8080>

Default local credentials:

- username: `admin`
- password: `admin`

The `daily_data_update` DAG runs:

1. Parse T-Bank articles.
2. Normalize parsed articles into the same chunk format as `data/*.json`.
3. Upload normalized JSON to MinIO bucket `rag-data`.
4. Ask FastAPI to ingest MinIO JSON objects into Qdrant.
5. Ask FastAPI to fine-tune the bi-encoder from stored feedback triplets.
