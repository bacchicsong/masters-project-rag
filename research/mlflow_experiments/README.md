# MLflow experiments

Четыре эксперимента: эмбеддинги, retrieval, чанкинг, overlap.  
Eval на вопросах из `research/data-collection/Q_A_articles.zip`, корпус — `data/tbank_articles_clean.json` (640 статей, 625 пар вопрос-статья).

MLflow: http://localhost:5000

## Запуск

```bash
docker compose up -d minio mlflow
cd research/mlflow_experiments
python run_all_experiments.py --limit 20
```

`--mock` если нужны фейковые данные, `--select 1 2` для выборочного запуска.

Метрики считаются для k = 1, 3, 5, 8. В таблицах ниже P@1, P@3, P@5 и R@5.

У нас на каждый вопрос одна релевантная статья, поэтому **R@k = доля запросов, где статья попала в top-k** (по сути hit rate). P@k при этом меньше: даже если статья в top-5, precision = 1/5 = 0.2 на этом запросе.

## Результаты (20 запросов, CPU)

Exp 1 — только MiniLM и e5-small из пяти моделей в конфиге.

### Exp 1 — embeddings

| модель | P@1 | P@3 | P@5 | R@5 | MRR |
|--------|-----|-----|-----|-----|-----|
| e5-small | 0.45 | 0.22 | 0.17 | 0.85 | 0.61 |
| MiniLM | 0.40 | 0.18 | 0.12 | 0.60 | 0.49 |

### Exp 2 — retrieval (MiniLM, full text)

| стратегия | P@1 | P@3 | P@5 | R@5 | MRR |
|-----------|-----|-----|-----|-----|-----|
| BM25 | 0.35 | 0.25 | 0.15 | 0.75 | 0.56 |
| hybrid k=20 | 0.40 | 0.20 | 0.13 | 0.65 | 0.55 |
| dense | 0.40 | 0.18 | 0.12 | 0.60 | 0.49 |

hybrid k=50/100 ≈ dense по P@5.

### Exp 3 — chunking (MiniLM)

document-level:

| стратегия | P@1 | P@3 | P@5 | R@5 |
|-----------|-----|-----|-----|-----|
| full | 0.40 | 0.18 | 0.12 | 0.60 |
| title_only | 0.20 | 0.07 | 0.05 | 0.25 |
| title_headings | 0.10 | 0.05 | 0.03 | 0.15 |

chunk size (full text, поиск по чанкам):

| size | P@1 | P@3 | P@5 | R@5 |
|------|-----|-----|-----|-----|
| 1024 | 0.40 | 0.17 | 0.11 | 0.55 |
| 512 | 0.20 | 0.13 | 0.09 | 0.45 |
| 256 | 0.15 | 0.12 | 0.08 | 0.40 |

### Exp 4 — chunk overlap (MiniLM, chunk_size=512, full text)

overlap: 0, 50, 100, 200 символов. Запуск: `python experiment_4_chunk_overlap.py` или `--select 4`.

Кратко: e5-small и BM25 выигрывают у dense/MiniLM; full text лучше урезанных стратегий; чанки хуже document-level.
