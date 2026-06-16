# Docker Compose profiles

The default compose startup is intentionally lightweight:

```bash
docker compose up -d
python check_services.py
```

Default runtime services:

- `app`
- `qdrant`
- `minio`
- `minio-init`

Optional heavy services are started by profile:

```bash
docker compose --profile airflow up -d
python check_services.py --mode airflow

docker compose --profile observability up -d
python check_services.py --mode observability

docker compose --profile training up -d mlflow
python check_services.py --mode training
```

Run fine-tuning as a batch job instead of keeping it in the default runtime:

```bash
docker compose --profile training run --rm fine-tune
```

Full integration check:

```bash
python check_services.py --mode all
```

Feedback is stored in `data/feedback/`.

The fine-tuned model is stored in the shared Docker volume mounted at:

```text
/app/models/fine_tuned_bi_encoder
```
