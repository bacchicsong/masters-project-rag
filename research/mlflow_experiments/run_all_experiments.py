import argparse
import importlib
import urllib.request

import utils.data_loader as data_loader
from utils.data_loader import load_golden_eval_set


EXPERIMENTS = [
    (1, "embeddings", "experiment_1_embedding_comparison"),
    (2, "chunking", "experiment_2_chunking_strategies"),
    (3, "overlap", "experiment_3_chunk_overlap"),
]


def mlflow_up():
    try:
        return urllib.request.urlopen("http://localhost:5000/health", timeout=3).status == 200
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--select", type=int, nargs="+")
    parser.add_argument("--skip", type=int, nargs="+")
    parser.add_argument("--mock", action="store_true")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        for num, name, _ in EXPERIMENTS:
            print(f"  {num}: {name}")
        return

    if args.mock:
        data_loader.USE_MOCK = True
    else:
        try:
            _, _, stats = load_golden_eval_set(num_queries=args.limit)
            print(f"golden set: {stats['used_queries']} queries\n")
        except Exception as e:
            print(f"[WARN] golden set: {e}\n")

    if args.limit:
        for module in ("experiment_1_embedding_comparison", "experiment_2_chunking_strategies", "experiment_3_chunk_overlap"):
            importlib.import_module(module).NUM_QUERIES = args.limit

    if args.select:
        selected = set(args.select)
        to_run = [e for e in EXPERIMENTS if e[0] in selected]
    elif args.skip:
        skipped = set(args.skip)
        to_run = [e for e in EXPERIMENTS if e[0] not in skipped]
    else:
        to_run = EXPERIMENTS

    if not to_run:
        print("nothing to run")
        return

    if not mlflow_up():
        print("[WARN] mlflow not on localhost:5000")

    ok, fail = 0, 0
    for num, name, module in to_run:
        print(f"\n=== exp {num}: {name} ===")
        try:
            importlib.import_module(module).run_experiment()
            ok += 1
        except Exception as e:
            print(f"failed: {e}")
            fail += 1

    print(f"\nfinished: {ok} ok, {fail} failed")


if __name__ == "__main__":
    main()
