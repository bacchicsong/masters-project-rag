"""
Orchestrator: Run all MLflow experiments sequentially.
Ensures that MLflow server and MinIO are available before starting.
Creates the MinIO bucket if it doesn't exist.

Usage:
    python run_all_experiments.py                    # Run all experiments
    python run_all_experiments.py --select 1 3 5     # Run only experiments 1, 3, 5
    python run_all_experiments.py --skip 2 4         # Skip experiments 2, 4
    python run_all_experiments.py --quick            # Run with minimal configurations only
"""
import argparse
import subprocess
import sys
import time
from typing import List, Optional


# Experiment registry: (number, name, module_path, description)
EXPERIMENTS = [
    (1, "Embedding Model Comparison",
     "experiment_1_embedding_comparison",
     "Compare different embedding models (MiniLM, e5, distiluse) for retrieval quality"),
    (2, "Retrieval Strategies",
     "experiment_2_retrieval_strategies",
     "Compare dense vs sparse (BM25) vs hybrid retrieval approaches"),
    (3, "Chunking / Context Strategies",
     "experiment_3_chunking_strategies",
     "Test text extraction strategies and chunk size effects"),
    (4, "Prompt Templates",
     "experiment_4_prompt_templates",
     "Compare prompt templates and generation parameters"),
    (5, "End-to-End RAG Evaluation",
     "experiment_5_end_to_end",
     "Full pipeline evaluation combining retrieval + generation"),
    (6, "Performance Benchmark",
     "experiment_6_performance_benchmark",
     "Profile resource utilization and latency for RAG components"),
]


def check_mlflow_server() -> bool:
    """Check if MLflow server is running by querying the health endpoint."""
    import urllib.request
    try:
        response = urllib.request.urlopen("http://localhost:5000/health", timeout=3)
        return response.status == 200
    except Exception:
        return False


def ensure_minio_bucket():
    """Ensure the mlflow-artifacts bucket exists in MinIO."""
    try:
        import boto3
        from botocore.config import Config

        s3 = boto3.client(
            "s3",
            endpoint_url="http://localhost:9000",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            config=Config(s3={"addressing_style": "path"}),
        )

        bucket_name = "mlflow-artifacts"
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"[OK] Created bucket '{bucket_name}'")
        except Exception as e:
            if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                print(f"[OK] Bucket '{bucket_name}' already exists")
            else:
                print(f"[WARN]  Bucket check: {e}")

        # List buckets
        response = s3.list_buckets()
        buckets = [b["Name"] for b in response["Buckets"]]
        print(f"[PACKAGE] Available buckets: {buckets}")

    except Exception as e:
        print(f"[WARN]  Could not connect to MinIO: {e}")
        print("   Make sure MinIO is running: docker compose up -d minio")


def print_header():
    """Print the experiment header."""
    print("\n" + "=" * 70)
    print("[START] RAG EXPERIMENTS - MLflow Tracking")
    print("=" * 70)
    print(f"MLflow Server : http://localhost:5000")
    print(f"MinIO Console : http://localhost:9001")
    print(f"Artifact Store: s3://mlflow-artifacts/")
    print("=" * 70 + "\n")


def print_experiment_list():
    """Print available experiments."""
    print("Available experiments:")
    print("-" * 70)
    for num, name, module, desc in EXPERIMENTS:
        print(f"  [{num}] {name}")
        print(f"       {desc}")
    print("-" * 70 + "\n")


def run_experiment(experiment_num: int, quick_mode: bool = False) -> bool:
    """Run a single experiment by number."""
    for num, name, module, desc in EXPERIMENTS:
        if num == experiment_num:
            print(f"\n{'#' * 70}")
            print(f"# RUNNING EXPERIMENT {num}: {name}")
            print(f"# {desc}")
            print(f"{'#' * 70}\n")

            try:
                # Import and run the experiment module
                mod = __import__(module, fromlist=["run_experiment"])
                if hasattr(mod, "run_experiment"):
                    mod.run_experiment()
                    print(f"\n[OK] Experiment {num} completed successfully!\n")
                    return True
                else:
                    print(f"[FAIL] Module {module} has no run_experiment() function")
                    return False
            except Exception as e:
                print(f"\n[FAIL] Experiment {num} failed: {e}")
                import traceback
                traceback.print_exc()
                return False

    print(f"[FAIL] Unknown experiment number: {experiment_num}")
    return False


def main():
    parser = argparse.ArgumentParser(
        description="Run RAG MLflow experiments"
    )
    parser.add_argument(
        "--select", type=int, nargs="+",
        help="Run only specific experiments (e.g., --select 1 3 5)"
    )
    parser.add_argument(
        "--skip", type=int, nargs="+",
        help="Skip specific experiments (e.g., --skip 2 4)"
    )
    parser.add_argument(
        "--quick", action="store_true",
        help="Run with minimal configurations (faster execution)"
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List available experiments and exit"
    )

    args = parser.parse_args()

    print_header()

    if args.list:
        print_experiment_list()
        return

    # Print experiment list
    print_experiment_list()

    # Determine which experiments to run
    if args.select:
        selected = set(args.select)
        experiments_to_run = [
            e for e in EXPERIMENTS if e[0] in selected
        ]
    elif args.skip:
        skipped = set(args.skip)
        experiments_to_run = [
            e for e in EXPERIMENTS if e[0] not in skipped
        ]
    else:
        experiments_to_run = EXPERIMENTS

    if not experiments_to_run:
        print("[FAIL] No experiments selected to run.")
        return

    print(f"[LIST] Will run {len(experiments_to_run)} experiment(s):")
    for num, name, _, _ in experiments_to_run:
        print(f"   [{num}] {name}")
    print()

    if args.quick:
        print("[FAST] Quick mode enabled - running with minimal configurations\n")

    # Check MLflow server
    print("[MAG] Checking MLflow server...")
    if check_mlflow_server():
        print("[OK] MLflow server is running\n")
    else:
        print("[WARN]  MLflow server not detected at http://localhost:5000")
        print("   Make sure it's running: docker compose up -d mlflow minio")
        proceed = input("   Continue anyway? (y/N): ").strip().lower()
        if proceed != "y":
            print("[FAIL] Aborted.")
            return

    # Ensure MinIO bucket exists
    print("[MAG] Checking MinIO...")
    ensure_minio_bucket()
    print()

    # Run experiments
    successful = 0
    failed = 0

    for num, name, module, desc in experiments_to_run:
        print(f"\n{'=' * 70}")
        print(f"▶ EXPERIMENT {num}: {name}")
        print(f"{'=' * 70}")

        try:
            mod = __import__(module, fromlist=["run_experiment"])
            if hasattr(mod, "run_experiment"):
                mod.run_experiment()
                successful += 1
            else:
                print(f"[FAIL] Module {module} has no run_experiment()")
                failed += 1
        except Exception as e:
            print(f"\n[FAIL] Experiment {num} failed: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    # Summary
    print("\n" + "=" * 70)
    print("[CHART] EXPERIMENT SUMMARY")
    print("=" * 70)
    print(f"   Total: {len(experiments_to_run)}")
    print(f"   [OK] Successful: {successful}")
    print(f"   [FAIL] Failed: {failed}")
    print(f"\n   MLflow UI: http://localhost:5000")
    print(f"   MinIO Console: http://localhost:9001 (admin/admin)")
    print("=" * 70)


if __name__ == "__main__":
    main()