"""Build reproducible tables and figure assets for the thesis presentation.

The script is intentionally lightweight: it uses the local JSON/ZIP files and
the experiment results documented in ``research/mlflow_experiments/README.md``.
It does not require running transformer models, so it can be used before the
defence to refresh EDA plots quickly.
"""

from __future__ import annotations

import csv
import json
import statistics as stats
import zipfile
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "research" / "presentation_assets"

DATASETS = {
    "T-Bank articles": ROOT / "data" / "tbank_articles.json",
    "Theoretical texts": ROOT / "data" / "theoretical_texts.json",
}

EXPERIMENT_RESULTS = [
    {"group": "Embedding", "model": "multilingual-e5-small", "params": "full document", "P@1": 0.45, "P@3": 0.22, "P@5": 0.17, "R@5": 0.85},
    {"group": "Embedding", "model": "MiniLM", "params": "full document", "P@1": 0.40, "P@3": 0.18, "P@5": 0.12, "R@5": 0.60},
    {"group": "Chunking", "model": "MiniLM", "params": "full document", "P@1": 0.40, "P@3": 0.18, "P@5": 0.12, "R@5": 0.60},
    {"group": "Chunking", "model": "MiniLM", "params": "title only", "P@1": 0.20, "P@3": 0.07, "P@5": 0.05, "R@5": 0.25},
    {"group": "Chunking", "model": "MiniLM", "params": "title + headings", "P@1": 0.10, "P@3": 0.05, "P@5": 0.03, "R@5": 0.15},
    {"group": "Chunk size", "model": "MiniLM", "params": "1024 chars, overlap=50", "P@1": 0.40, "P@3": 0.17, "P@5": 0.11, "R@5": 0.55},
    {"group": "Chunk size", "model": "MiniLM", "params": "512 chars, overlap=50", "P@1": 0.20, "P@3": 0.13, "P@5": 0.09, "R@5": 0.45},
    {"group": "Chunk size", "model": "MiniLM", "params": "256 chars, overlap=50", "P@1": 0.15, "P@3": 0.12, "P@5": 0.08, "R@5": 0.40},
    {"group": "Overlap", "model": "MiniLM", "params": "512 chars, overlap=0", "P@1": 0.15, "P@3": 0.07, "P@5": 0.05, "R@5": 0.25},
    {"group": "Overlap", "model": "MiniLM", "params": "512 chars, overlap=50", "P@1": 0.20, "P@3": 0.13, "P@5": 0.09, "R@5": 0.45},
    {"group": "Overlap", "model": "MiniLM", "params": "512 chars, overlap=100", "P@1": 0.25, "P@3": 0.17, "P@5": 0.12, "R@5": 0.60},
    {"group": "Overlap", "model": "MiniLM", "params": "512 chars, overlap=200", "P@1": 0.25, "P@3": 0.13, "P@5": 0.08, "R@5": 0.40},
]


def load_chunks(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def summarize_dataset(name: str, path: Path) -> dict:
    rows = load_chunks(path)
    texts = [row.get("text", "") or "" for row in rows]
    lengths = [len(text) for text in texts]
    words = [len(text.split()) for text in texts]
    return {
        "dataset": name,
        "chunks": len(rows),
        "missing_text": sum(1 for text in texts if not text),
        "duplicate_texts": len(texts) - len(set(texts)),
        "chars_min": min(lengths),
        "chars_median": stats.median(lengths),
        "chars_mean": round(stats.mean(lengths), 1),
        "chars_p95": sorted(lengths)[int(0.95 * (len(lengths) - 1))],
        "chars_max": max(lengths),
        "words_median": stats.median(words),
        "words_mean": round(stats.mean(words), 1),
        "words_max": max(words),
    }


def summarize_golden_set() -> dict:
    zip_path = ROOT / "research" / "data-collection" / "Q_A_articles.zip"
    json_files = 0
    question_markers = 0
    content_lengths = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if not name.endswith(".json") or "__MACOSX" in name or "/._" in name:
                continue
            json_files += 1
            data = json.loads(zf.read(name))
            content = data.get("content", "")
            content_lengths.append(len(content))
            question_markers += content.count("## Вопрос")
    return {
        "articles_with_qa": json_files,
        "question_markers": question_markers,
        "avg_qa_file_chars": round(stats.mean(content_lengths), 1) if content_lengths else 0,
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def plot_length_histograms() -> None:
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), constrained_layout=True)
    for ax, (name, path) in zip(axes, DATASETS.items()):
        lengths = [len(row.get("text", "") or "") for row in load_chunks(path)]
        ax.hist(lengths, bins=40, color="#3B82F6", alpha=0.82)
        ax.axvline(stats.median(lengths), color="#111827", linestyle="--", linewidth=1)
        ax.set_title(name)
        ax.set_xlabel("Chunk length, characters")
        ax.set_ylabel("Count")
    fig.savefig(OUT / "eda_text_lengths.png", dpi=180)
    plt.close(fig)


def plot_experiment_results() -> None:
    rows = EXPERIMENT_RESULTS
    labels = [f"{row['group']}: {row['model']}, {row['params']}" for row in rows]
    r5 = [row["R@5"] for row in rows]
    p1 = [row["P@1"] for row in rows]
    fig, ax = plt.subplots(figsize=(10, 6), constrained_layout=True)
    y = range(len(rows))
    ax.barh([i - 0.18 for i in y], r5, height=0.34, label="R@5", color="#10B981")
    ax.barh([i + 0.18 for i in y], p1, height=0.34, label="P@1", color="#6366F1")
    ax.set_yticks(list(y), labels)
    ax.invert_yaxis()
    ax.set_xlim(0, 1.0)
    ax.set_xlabel("Metric value")
    ax.legend(loc="lower right")
    fig.savefig(OUT / "experiment_metrics.png", dpi=180)
    plt.close(fig)


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    dataset_rows = [summarize_dataset(name, path) for name, path in DATASETS.items()]
    golden = summarize_golden_set()
    write_csv(OUT / "dataset_summary.csv", dataset_rows)
    write_csv(OUT / "experiment_results.csv", EXPERIMENT_RESULTS)
    (OUT / "golden_set_summary.json").write_text(json.dumps(golden, ensure_ascii=False, indent=2), encoding="utf-8")
    plot_length_histograms()
    plot_experiment_results()
    print(f"Saved presentation assets to {OUT}")


if __name__ == "__main__":
    main()
