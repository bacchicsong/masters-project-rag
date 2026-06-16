"""Generate figures and summary tables for the defence presentation.

The module is split into three responsibilities:
1. EDA over the local corpora and golden set.
2. Export of experiment tables, with MLflow as the preferred source.
3. Architecture diagrams for the service and docker topology.

The script is deliberately self-contained so it can be called from a notebook
or directly from the command line without project-specific imports.
"""

from __future__ import annotations

import csv
import json
import os
import re
import statistics as stats
import urllib.request
import zipfile
from collections import Counter
from pathlib import Path

MPL_CONFIG_DIR = Path(__file__).resolve().parent / ".matplotlib"
MPL_CONFIG_DIR.mkdir(exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CONFIG_DIR))
os.environ.setdefault("MPLBACKEND", "Agg")

import matplotlib.pyplot as plt
from matplotlib import patches


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research" / "presentation_assets"

DATASETS = {
    "T-Bank articles": ROOT / "data" / "tbank_articles.json",
    "Theoretical texts": ROOT / "data" / "theoretical_texts.json",
}

DEFAULT_MLFLOW_SQLITE_URI = "sqlite:///C:/Users/Admin/Downloads/git_projects/masters-project-rag/research/presentation_assets/mlflow_tracking.db"
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", DEFAULT_MLFLOW_SQLITE_URI)
MLFLOW_EXPERIMENTS = {
    "rag-experiments-golden-embedding-comparison": "Embedding",
    "rag-experiments-golden-chunking-strategies": "Chunking",
    "rag-experiments-golden-chunk-overlap": "Overlap",
}

# Fallback values taken from research/mlflow_experiments/README.md and used when
# no MLflow runs are available locally.
FALLBACK_EXPERIMENT_RESULTS = [
    {
        "experiment": "rag-experiments-golden-embedding-comparison",
        "group": "Embedding",
        "run_name": "multilingual-e5-small",
        "model": "multilingual-e5-small",
        "params": "full document",
        "P@1": 0.45,
        "P@3": 0.22,
        "P@5": 0.17,
        "R@5": 0.85,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-embedding-comparison",
        "group": "Embedding",
        "run_name": "MiniLM",
        "model": "MiniLM",
        "params": "full document",
        "P@1": 0.40,
        "P@3": 0.18,
        "P@5": 0.12,
        "R@5": 0.60,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunking-strategies",
        "group": "Chunking",
        "run_name": "strategy-full",
        "model": "MiniLM",
        "params": "full document",
        "P@1": 0.40,
        "P@3": 0.18,
        "P@5": 0.12,
        "R@5": 0.60,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunking-strategies",
        "group": "Chunking",
        "run_name": "strategy-title_only",
        "model": "MiniLM",
        "params": "title only",
        "P@1": 0.20,
        "P@3": 0.07,
        "P@5": 0.05,
        "R@5": 0.25,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunking-strategies",
        "group": "Chunking",
        "run_name": "strategy-title_headings",
        "model": "MiniLM",
        "params": "title + headings",
        "P@1": 0.10,
        "P@3": 0.05,
        "P@5": 0.03,
        "R@5": 0.15,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunking-strategies",
        "group": "Chunk size",
        "run_name": "chunk_size=1024",
        "model": "MiniLM",
        "params": "1024 chars, overlap=50",
        "P@1": 0.40,
        "P@3": 0.17,
        "P@5": 0.11,
        "R@5": 0.55,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunking-strategies",
        "group": "Chunk size",
        "run_name": "chunk_size=512",
        "model": "MiniLM",
        "params": "512 chars, overlap=50",
        "P@1": 0.20,
        "P@3": 0.13,
        "P@5": 0.09,
        "R@5": 0.45,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunking-strategies",
        "group": "Chunk size",
        "run_name": "chunk_size=256",
        "model": "MiniLM",
        "params": "256 chars, overlap=50",
        "P@1": 0.15,
        "P@3": 0.12,
        "P@5": 0.08,
        "R@5": 0.40,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunk-overlap",
        "group": "Overlap",
        "run_name": "overlap=0",
        "model": "MiniLM",
        "params": "512 chars, overlap=0",
        "P@1": 0.15,
        "P@3": 0.07,
        "P@5": 0.05,
        "R@5": 0.25,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunk-overlap",
        "group": "Overlap",
        "run_name": "overlap=50",
        "model": "MiniLM",
        "params": "512 chars, overlap=50",
        "P@1": 0.20,
        "P@3": 0.13,
        "P@5": 0.09,
        "R@5": 0.45,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunk-overlap",
        "group": "Overlap",
        "run_name": "overlap=100",
        "model": "MiniLM",
        "params": "512 chars, overlap=100",
        "P@1": 0.25,
        "P@3": 0.17,
        "P@5": 0.12,
        "R@5": 0.60,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
    {
        "experiment": "rag-experiments-golden-chunk-overlap",
        "group": "Overlap",
        "run_name": "overlap=200",
        "model": "MiniLM",
        "params": "512 chars, overlap=200",
        "P@1": 0.25,
        "P@3": 0.13,
        "P@5": 0.08,
        "R@5": 0.40,
        "R@8": None,
        "MRR": None,
        "source": "README fallback",
    },
]


def load_chunks(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    idx = int(p * (len(values) - 1))
    return sorted(values)[idx]


def summarize_dataset(name: str, path: Path) -> dict:
    rows = load_chunks(path)
    texts = [row.get("text", "") or "" for row in rows]
    lengths = [len(text) for text in texts]
    words = [len(text.split()) for text in texts]
    metas = [row.get("meta") or {} for row in rows]
    source_counts = Counter(meta.get("source_file", "unknown") for meta in metas)
    translated = [bool(meta.get("translated_from_english")) for meta in metas]
    expansion = [
        (meta.get("character_count", 0) / max(meta.get("original_character_count", 1), 1))
        for meta in metas
    ]
    return {
        "dataset": name,
        "chunks": len(rows),
        "missing_text": sum(1 for text in texts if not text),
        "duplicate_texts": len(texts) - len(set(texts)),
        "duplicate_share": round((len(texts) - len(set(texts))) / len(texts), 4),
        "unique_sources": len(source_counts),
        "translated_share": round(sum(translated) / len(translated), 4),
        "chars_min": min(lengths),
        "chars_p25": percentile(lengths, 0.25),
        "chars_median": stats.median(lengths),
        "chars_mean": round(stats.mean(lengths), 1),
        "chars_p95": percentile(lengths, 0.95),
        "chars_max": max(lengths),
        "words_median": stats.median(words),
        "words_mean": round(stats.mean(words), 1),
        "words_max": max(words),
        "expansion_ratio_mean": round(stats.mean(expansion), 3),
        "expansion_ratio_median": round(stats.median(expansion), 3),
    }


def extract_questions_from_zip(zip_path: Path) -> list[dict]:
    question_rows = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.endswith(".json") or "__MACOSX" in name or "/._" in name:
                continue
            data = json.loads(zf.read(name))
            content = data.get("content", "")
            article_title = Path(name).stem
            matches = re.findall(r"##\s*Вопрос\s*\d+:\s*(.+?)\n", content)
            if not matches:
                matches = re.findall(r"##\s*Question\s*\d+:\s*(.+?)\n", content)
            for question in matches:
                question_rows.append(
                    {
                        "article": article_title,
                        "question": question.strip(),
                        "question_chars": len(question.strip()),
                        "question_words": len(question.strip().split()),
                    }
                )
    return question_rows


def summarize_golden_set(zip_path: Path) -> tuple[dict, list[dict]]:
    questions = extract_questions_from_zip(zip_path)
    by_article = Counter(row["article"] for row in questions)
    question_lengths = [row["question_words"] for row in questions]
    summary = {
        "articles_with_qa": len(by_article),
        "question_markers": len(questions),
        "avg_questions_per_article": round(stats.mean(by_article.values()), 2) if by_article else 0,
        "median_question_words": round(stats.median(question_lengths), 2) if question_lengths else 0,
        "mean_question_words": round(stats.mean(question_lengths), 2) if question_lengths else 0,
        "max_question_words": max(question_lengths) if question_lengths else 0,
    }
    return summary, questions


def try_export_mlflow_runs() -> tuple[list[dict], dict]:
    status = {
        "tracking_uri": MLFLOW_URI,
        "available": False,
        "experiments_found": 0,
        "runs_exported": 0,
        "error": None,
    }
    try:
        import mlflow
    except Exception as exc:  # pragma: no cover - environment dependent
        status["error"] = f"mlflow import failed: {exc}"
        return [], status

    try:  # pragma: no cover - environment dependent
        if MLFLOW_URI.startswith("http"):
            urllib.request.urlopen(f"{MLFLOW_URI.rstrip('/')}/health", timeout=3)
        mlflow.set_tracking_uri(MLFLOW_URI)
        experiments = mlflow.search_experiments()
        rows = []
        for exp in experiments:
            if exp.name not in MLFLOW_EXPERIMENTS:
                continue
            status["experiments_found"] += 1
            runs_df = mlflow.search_runs(
                experiment_ids=[exp.experiment_id],
                order_by=["attributes.start_time DESC"],
                output_format="pandas",
            )
            if runs_df.empty:
                continue
            for _, run in runs_df.iterrows():
                p1 = run.get("metrics.mean_P_at_1")
                p3 = run.get("metrics.mean_P_at_3")
                p5 = run.get("metrics.mean_P_at_5")
                r5 = run.get("metrics.mean_R_at_5")
                r8 = run.get("metrics.mean_R_at_8")
                mrr = run.get("metrics.MRR")
                if any(str(value).lower() == "nan" for value in (p1, p3, p5, r5, r8, mrr)):
                    continue

                run_name = run.get("tags.mlflow.runName") or ""
                model = run.get("params.model_name") or run.get("params.model_id") or run_name
                group = MLFLOW_EXPERIMENTS[exp.name]
                params = "full document"

                if exp.name.endswith("chunking-strategies"):
                    if run_name.startswith("chunk_size="):
                        group = "Chunk size"
                        chunk_size = run_name.split("=", 1)[1]
                        overlap = run.get("params.chunk_overlap") or 50
                        params = f"{chunk_size} chars, overlap={overlap}"
                    elif run_name.startswith("strategy-"):
                        strategy = run_name.split("strategy-", 1)[1]
                        params = {
                            "full": "full document",
                            "title_only": "title only",
                            "title_headings": "title + headings",
                        }.get(strategy, strategy)
                elif exp.name.endswith("chunk-overlap"):
                    overlap = run_name.split("=", 1)[1] if "=" in run_name else run.get("params.chunk_overlap")
                    chunk_size = run.get("params.chunk_size") or 512
                    params = f"{chunk_size} chars, overlap={overlap}"

                rows.append(
                    {
                        "experiment": exp.name,
                        "group": group,
                        "run_name": run_name,
                        "model": model,
                        "params": params,
                        "P@1": p1,
                        "P@3": p3,
                        "P@5": p5,
                        "R@5": r5,
                        "R@8": r8,
                        "MRR": mrr,
                        "source": "MLflow",
                    }
                )
        status["available"] = True
        status["runs_exported"] = len(rows)
        return rows, status
    except Exception as exc:  # pragma: no cover - environment dependent
        status["error"] = str(exc)
        return [], status


def select_experiment_rows() -> tuple[list[dict], dict]:
    mlflow_rows, mlflow_status = try_export_mlflow_runs()
    rows = mlflow_rows if mlflow_rows else FALLBACK_EXPERIMENT_RESULTS
    return rows, mlflow_status


def plot_length_histograms() -> None:
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.2), constrained_layout=True)
    for ax, (name, path) in zip(axes, DATASETS.items()):
        lengths = [len(row.get("text", "") or "") for row in load_chunks(path)]
        ax.hist(lengths, bins=40, color="#2563EB", alpha=0.82)
        ax.axvline(stats.median(lengths), color="#111827", linestyle="--", linewidth=1.5)
        ax.set_title(name)
        ax.set_xlabel("Chunk length, characters")
        ax.set_ylabel("Count")
    fig.savefig(OUT / "eda_text_lengths.png", dpi=180)
    plt.close(fig)


def plot_dataset_quality(dataset_rows: list[dict]) -> None:
    fig, axes = plt.subplots(1, 3, figsize=(13, 4.2), constrained_layout=True)

    names = [row["dataset"] for row in dataset_rows]
    duplicate_share = [row["duplicate_share"] for row in dataset_rows]
    translated_share = [row["translated_share"] for row in dataset_rows]
    expansion_ratio = [row["expansion_ratio_mean"] for row in dataset_rows]

    axes[0].bar(names, duplicate_share, color=["#DC2626", "#16A34A"])
    axes[0].set_title("Duplicate share")
    axes[0].set_ylim(0, 1)
    axes[0].set_ylabel("Share")

    axes[1].bar(names, translated_share, color=["#0EA5E9", "#7C3AED"])
    axes[1].set_title("Translated-from-English share")
    axes[1].set_ylim(0, 1)

    axes[2].bar(names, expansion_ratio, color=["#F59E0B", "#4F46E5"])
    axes[2].set_title("Mean char expansion ratio")
    axes[2].set_ylabel("translated chars / original chars")

    for ax in axes:
        ax.tick_params(axis="x", rotation=10)

    fig.savefig(OUT / "eda_dataset_quality.png", dpi=180)
    plt.close(fig)


def plot_theoretical_sources() -> None:
    theoretical = load_chunks(DATASETS["Theoretical texts"])
    counts = Counter((row.get("meta") or {}).get("source_file", "unknown") for row in theoretical)
    top = counts.most_common(7)

    fig, ax = plt.subplots(figsize=(10, 4.8), constrained_layout=True)
    labels = [Path(name).name for name, _ in top]
    values = [value for _, value in top]
    ax.barh(labels[::-1], values[::-1], color="#0F766E")
    ax.set_title("Top source documents in the theoretical corpus")
    ax.set_xlabel("Chunks")
    fig.savefig(OUT / "eda_theoretical_sources.png", dpi=180)
    plt.close(fig)


def plot_golden_set(questions: list[dict]) -> None:
    by_article = Counter(row["article"] for row in questions)
    per_article = list(by_article.values())
    question_words = [row["question_words"] for row in questions]

    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.2), constrained_layout=True)
    axes[0].hist(per_article, bins=range(1, max(per_article) + 2), color="#7C3AED", rwidth=0.85)
    axes[0].set_title("Questions per article in golden set")
    axes[0].set_xlabel("Questions per article")
    axes[0].set_ylabel("Articles")

    axes[1].hist(question_words, bins=20, color="#EA580C", alpha=0.85)
    axes[1].axvline(stats.median(question_words), color="#111827", linestyle="--", linewidth=1.5)
    axes[1].set_title("Query length distribution")
    axes[1].set_xlabel("Words in question")
    axes[1].set_ylabel("Count")

    fig.savefig(OUT / "eda_golden_set.png", dpi=180)
    plt.close(fig)


def plot_experiment_results(rows: list[dict]) -> None:
    def short_model_name(name: str) -> str:
        mapping = {
            "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2": "MiniLM",
            "paraphrase-multilingual-MiniLM-L12-v2": "MiniLM",
            "distiluse-base-multilingual-cased-v2": "distiluse",
            "multilingual-e5-base": "e5-base",
            "multilingual-e5-small": "e5-small",
        }
        return mapping.get(name, name)

    group_order = {"Embedding": 0, "Chunking": 1, "Chunk size": 2, "Overlap": 3}
    rows = sorted(rows, key=lambda row: (group_order.get(row["group"], 99), -(float(row["R@5"] or 0))))
    labels = [f"{row['group']}: {short_model_name(row['model'])}, {row['params']}" for row in rows]
    recall = [row["R@5"] or 0 for row in rows]
    precision = [row["P@1"] or 0 for row in rows]

    fig, ax = plt.subplots(figsize=(11.5, 6.5), constrained_layout=True)
    y = range(len(rows))
    ax.barh([i - 0.18 for i in y], recall, height=0.34, label="R@5", color="#10B981")
    ax.barh([i + 0.18 for i in y], precision, height=0.34, label="P@1", color="#4F46E5")
    ax.set_yticks(list(y), labels)
    ax.invert_yaxis()
    ax.set_xlim(0, 1.0)
    ax.set_xlabel("Metric value")
    ax.set_title("Offline retrieval experiments")
    ax.legend(loc="lower right")
    fig.savefig(OUT / "experiment_metrics.png", dpi=180)
    plt.close(fig)


def add_box(ax, xy, width, height, text, fc, ec="#111827", text_size=11):
    rect = patches.FancyBboxPatch(
        xy,
        width,
        height,
        boxstyle="round,pad=0.02,rounding_size=0.03",
        facecolor=fc,
        edgecolor=ec,
        linewidth=1.4,
    )
    ax.add_patch(rect)
    ax.text(
        xy[0] + width / 2,
        xy[1] + height / 2,
        text,
        ha="center",
        va="center",
        fontsize=text_size,
        color="#111827",
        wrap=True,
    )


def add_arrow(ax, start, end, text=None, text_offset=(0, 0)):
    arrow = patches.FancyArrowPatch(
        start,
        end,
        arrowstyle="->",
        mutation_scale=16,
        linewidth=1.4,
        color="#334155",
    )
    ax.add_patch(arrow)
    if text:
        ax.text(
            (start[0] + end[0]) / 2 + text_offset[0],
            (start[1] + end[1]) / 2 + text_offset[1],
            text,
            fontsize=9,
            ha="center",
            va="center",
            color="#334155",
        )


def plot_architecture_overview() -> None:
    fig, ax = plt.subplots(figsize=(12, 7), constrained_layout=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    ax.text(0.17, 0.86, "User entrypoints", ha="center", fontsize=10, color="#475569", weight="bold")
    ax.text(0.49, 0.86, "Application orchestration", ha="center", fontsize=10, color="#475569", weight="bold")
    ax.text(0.83, 0.86, "Knowledge and generation", ha="center", fontsize=10, color="#475569", weight="bold")
    ax.text(0.35, 0.37, "Feedback and training loop", ha="center", fontsize=10, color="#475569", weight="bold")

    add_box(ax, (0.04, 0.68), 0.18, 0.12, "Users\nTelegram / API", "#DBEAFE")
    add_box(ax, (0.28, 0.68), 0.18, 0.12, "FastAPI app\ncontroller + middleware", "#E0F2FE")
    add_box(ax, (0.52, 0.68), 0.18, 0.12, "QueryUsecase\nencode -> search -> rerank", "#DCFCE7")
    add_box(ax, (0.76, 0.68), 0.18, 0.12, "GigaChat\nanswer generation", "#FDE68A")

    add_box(ax, (0.52, 0.44), 0.18, 0.12, "SentenceTransformer\nquery embedding", "#EDE9FE")
    add_box(ax, (0.76, 0.44), 0.18, 0.12, "Qdrant\nvector search top-20", "#FCE7F3")
    add_box(ax, (0.52, 0.20), 0.18, 0.12, "Cross-encoder\nre-rank top-10", "#FEE2E2")
    add_box(ax, (0.28, 0.20), 0.18, 0.12, "Feedback storage\nJSONL triplets", "#FFEDD5")
    add_box(ax, (0.04, 0.20), 0.18, 0.12, "Fine-tuning\nCosineSimilarityLoss", "#D1FAE5")

    add_arrow(ax, (0.22, 0.74), (0.28, 0.74), "HTTP / Telegram")
    add_arrow(ax, (0.46, 0.74), (0.52, 0.74), "request")
    add_arrow(ax, (0.61, 0.68), (0.61, 0.56), "embed")
    add_arrow(ax, (0.70, 0.50), (0.76, 0.50), "dense retrieval")
    add_arrow(ax, (0.85, 0.44), (0.61, 0.32), "top-20 candidates", (-0.01, 0.03))
    add_arrow(ax, (0.70, 0.26), (0.76, 0.68), "top-10 context", (0.05, 0.0))
    add_arrow(ax, (0.76, 0.74), (0.70, 0.74), "answer")
    add_arrow(ax, (0.52, 0.26), (0.46, 0.26), "like / dislike")
    add_arrow(ax, (0.28, 0.26), (0.22, 0.26), "triplets")
    add_arrow(ax, (0.22, 0.26), (0.52, 0.50), "updated bi-encoder", (0.03, 0.04))

    ax.text(0.5, 0.93, "RAG application logic", ha="center", fontsize=16, color="#111827", weight="bold")
    fig.savefig(OUT / "architecture_overview.png", dpi=180)
    plt.close(fig)


def plot_container_topology() -> None:
    fig, ax = plt.subplots(figsize=(12, 7), constrained_layout=True)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")

    ax.text(0.47, 0.88, "Online serving contour", ha="center", fontsize=10, color="#475569", weight="bold")
    ax.text(0.47, 0.60, "Monitoring and experiments contour", ha="center", fontsize=10, color="#475569", weight="bold")
    ax.text(0.47, 0.32, "Artifact storage contour", ha="center", fontsize=10, color="#475569", weight="bold")

    add_box(ax, (0.08, 0.75), 0.18, 0.11, "app\nFastAPI + Telegram", "#DBEAFE")
    add_box(ax, (0.38, 0.75), 0.18, 0.11, "qdrant", "#FCE7F3")
    add_box(ax, (0.68, 0.75), 0.18, 0.11, "loader\nfill_qdrant.py", "#DCFCE7")

    add_box(ax, (0.08, 0.47), 0.18, 0.11, "prometheus", "#FEE2E2")
    add_box(ax, (0.38, 0.47), 0.18, 0.11, "grafana", "#FEF3C7")
    add_box(ax, (0.68, 0.47), 0.18, 0.11, "mlflow", "#EDE9FE")

    add_box(ax, (0.23, 0.19), 0.18, 0.11, "minio", "#D1FAE5")
    add_box(ax, (0.53, 0.19), 0.18, 0.11, "minio-init", "#FFEDD5")

    add_arrow(ax, (0.26, 0.80), (0.38, 0.80), "search / upsert")
    add_arrow(ax, (0.68, 0.80), (0.56, 0.80), "initial corpus load")
    add_arrow(ax, (0.17, 0.75), (0.17, 0.58), "/metrics")
    add_arrow(ax, (0.26, 0.52), (0.38, 0.52), "dashboards")
    add_arrow(ax, (0.68, 0.47), (0.41, 0.25), "artifact root", (-0.01, 0.02))
    add_arrow(ax, (0.41, 0.25), (0.53, 0.25), "buckets / models")
    add_arrow(ax, (0.62, 0.25), (0.68, 0.47), "prepare storage")

    ax.text(0.5, 0.93, "Docker service topology", ha="center", fontsize=16, color="#111827", weight="bold")
    ax.text(
        0.5,
        0.06,
        "Startup logic: qdrant starts before loader/app, minio starts before minio-init/mlflow, prometheus reads app metrics, grafana reads prometheus.",
        ha="center",
        fontsize=10,
        color="#334155",
        wrap=True,
    )
    fig.savefig(OUT / "container_topology.png", dpi=180)
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)

    dataset_rows = [summarize_dataset(name, path) for name, path in DATASETS.items()]
    golden_summary, question_rows = summarize_golden_set(ROOT / "research" / "data-collection" / "Q_A_articles.zip")
    experiment_rows, mlflow_status = select_experiment_rows()

    write_csv(OUT / "dataset_summary.csv", dataset_rows)
    write_csv(OUT / "experiment_results.csv", experiment_rows)
    write_csv(OUT / "golden_questions.csv", question_rows)
    (OUT / "golden_set_summary.json").write_text(
        json.dumps(golden_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (OUT / "mlflow_export_status.json").write_text(
        json.dumps(mlflow_status, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    plot_length_histograms()
    plot_dataset_quality(dataset_rows)
    plot_theoretical_sources()
    plot_golden_set(question_rows)
    plot_experiment_results(experiment_rows)
    plot_architecture_overview()
    plot_container_topology()

    print(f"Saved presentation assets to {OUT}")


if __name__ == "__main__":
    main()
