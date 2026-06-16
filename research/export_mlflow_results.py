"""Export MLflow experiment runs used in the presentation.

This script does not run experiments by itself. It connects to the configured
tracking server and writes `research/presentation_assets/experiment_results.csv`
when runs are available, otherwise it records the failure reason in
`mlflow_export_status.json`.
"""

from pathlib import Path

from presentation_assets import OUT, select_experiment_rows, write_csv


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    rows, status = select_experiment_rows()
    write_csv(OUT / "experiment_results.csv", rows)
    (OUT / "mlflow_export_status.json").write_text(
        __import__("json").dumps(status, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    source = "MLflow" if status.get("runs_exported") else "fallback"
    print(f"Exported {len(rows)} rows from {source}")


if __name__ == "__main__":
    main()
