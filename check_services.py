import argparse
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


SEPARATOR = "=" * 72
BOT_TOKEN_RE = re.compile(r"bot[0-9]+:[A-Za-z0-9_-]+")
FEEDBACK_DIR = Path("data") / "feedback"


def print_header(title: str):
    print(SEPARATOR)
    print(f"  {title}")
    print(SEPARATOR)


def redact_secrets(text: str) -> str:
    return BOT_TOKEN_RE.sub("bot<redacted>", text)


def fetch(url: str, timeout: int = 5):
    try:
        response = urllib.request.urlopen(url, timeout=timeout)
        return response.status, response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body


def check_http(name: str, url: str, expected=(200,), timeout: int = 5) -> bool:
    print(f"- {name}: {url}")
    try:
        status, body = fetch(url, timeout=timeout)
        if status in expected:
            preview = body[:180].replace("\n", " ")
            print(f"  OK {status} {preview}")
            return True
        print(f"  FAIL unexpected status {status}: {body[:300]}")
    except Exception as e:
        print(f"  FAIL {e}")
    return False


def check_fastapi() -> bool:
    ok = False
    for attempt in range(1, 4):
        try:
            status, body = fetch("http://localhost:8088/api/v1/health", timeout=5)
            if status == 200:
                print(f"- FastAPI health: OK {body}")
                ok = True
                break
        except Exception as e:
            if attempt == 3:
                print(f"- FastAPI health: FAIL {e}")
            else:
                print(f"- FastAPI health: attempt {attempt}/3 failed, retrying...")
                time.sleep(10)
    return ok


def check_rag_readiness() -> bool:
    print("- RAG readiness")
    try:
        status, body = fetch("http://localhost:8088/api/v1/health", timeout=5)
        if status != 200:
            print(f"  FAIL status {status}: {body[:300]}")
            return False
        data = json.loads(body)
        model_status = data.get("embedding_model", {})
        state = model_status.get("state")
        model_name = model_status.get("model_name", "unknown")
        dimension = model_status.get("dimension")
        print(f"  embedding_model state={state} model={model_name} dim={dimension}")
        if state == "loaded":
            if dimension != 384:
                print("  FAIL embedding dimension is not 384; Qdrant collection vectors are 384-dimensional")
                return False
            return True
        if state == "failed":
            print(f"  FAIL embedding model failed: {model_status.get('error')}")
            return False
        print("  FAIL embedding model is not loaded; Telegram answers may hang on the first query")
        return False
    except Exception as e:
        print(f"  FAIL {e}")
        return False


def check_qdrant_collection() -> bool:
    print("- Qdrant collection definitions")
    try:
        status, body = fetch("http://localhost:6333/collections/definitions", timeout=5)
        if status != 200:
            print(f"  FAIL status {status}: {body[:300]}")
            return False
        data = json.loads(body)
        result = data.get("result", {})
        points_count = result.get("points_count", 0) or 0
        print(
            "  OK "
            f"points={points_count} "
            f"status={result.get('status', 'N/A')}"
        )
        if points_count <= 0:
            print("  FAIL collection is empty")
            return False

        req = urllib.request.Request(
            "http://localhost:6333/collections/definitions/points/scroll",
            data=json.dumps({"limit": 1, "with_payload": True, "with_vector": False}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            sample = json.loads(response.read().decode("utf-8"))
        points = sample.get("result", {}).get("points", [])
        if not points or not points[0].get("payload", {}).get("text"):
            print("  FAIL could not read a payload sample with text")
            return False
        text_preview = points[0]["payload"]["text"][:100].replace("\n", " ")
        text_preview = text_preview.encode("ascii", "backslashreplace").decode("ascii")
        print(f"  OK sample text: {text_preview}")
        return True
    except Exception as e:
        print(f"  FAIL {e}")
        return False


def check_prometheus_targets() -> bool:
    print("- Prometheus targets")
    try:
        status, body = fetch("http://localhost:9090/api/v1/targets", timeout=5)
        if status != 200:
            print(f"  FAIL status {status}: {body[:300]}")
            return False
        data = json.loads(body)
        active = data.get("data", {}).get("activeTargets", [])
        for target in active:
            labels = target.get("labels", {})
            print(
                "  "
                f"{labels.get('job', 'unknown')} "
                f"{target.get('scrapeUrl')} "
                f"health={target.get('health')}"
            )
        return any(t.get("health") == "up" for t in active)
    except Exception as e:
        print(f"  FAIL {e}")
        return False


def check_docker_ps() -> bool:
    print("- Docker containers")
    try:
        result = subprocess.run(
            ["docker", "ps", "-a", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"],
            capture_output=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
        output = result.stdout or result.stderr
        for line in output.strip().splitlines():
            print(f"  {line}")
        return result.returncode == 0
    except Exception as e:
        print(f"  FAIL {e}")
        return False


def check_fastapi_logs() -> bool:
    print("- FastAPI recent logs")
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", "20", "fastapi_app"],
            capture_output=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
        logs = (result.stdout or "") + (result.stderr or "")
        for line in logs.strip().splitlines()[-5:]:
            line = redact_secrets(line).encode("ascii", "backslashreplace").decode("ascii")
            print(f"  {line}")
        return result.returncode == 0
    except Exception as e:
        print(f"  FAIL {e}")
        return False


def check_telegram_bot() -> bool:
    print("- Telegram bot startup")
    try:
        env_result = subprocess.run(
            ["docker", "exec", "fastapi_app", "python", "-c", "import os; print(os.getenv('TELEGRAM_BOT_TOKEN','')); print(os.getenv('ENABLE_TELEGRAM_BOT',''))"],
            capture_output=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
        env_lines = (env_result.stdout or "").splitlines()
        token_set = bool(env_lines[0].strip()) if env_lines else False
        enabled = (env_lines[1].strip().lower() == "true") if len(env_lines) > 1 else False

        logs_result = subprocess.run(
            ["docker", "logs", "--tail", "500", "fastapi_app"],
            capture_output=True,
            timeout=60,
            encoding="utf-8",
            errors="replace",
        )
        logs = (logs_result.stdout or "") + (logs_result.stderr or "")
        logs = redact_secrets(logs)
        latest_start = logs.rsplit("Started server process", 1)[-1]

        if not token_set:
            print("  WARN TELEGRAM_BOT_TOKEN is not set; skipping bot polling check")
            return True
        if not enabled:
            print("  FAIL TELEGRAM_BOT_TOKEN is set but ENABLE_TELEGRAM_BOT is not true")
            return False
        if "Failed to start Telegram bot" in latest_start:
            print("  FAIL Telegram bot startup failed")
            return False
        if "Telegram bot disabled" in latest_start:
            print("  FAIL Telegram bot is disabled")
            return False
        if "Telegram bot started and polling." not in latest_start:
            print("  FAIL Telegram bot did not reach polling state")
            return False
        if (
            "Lazy initializing Telegram bot RAG components..." in latest_start
            and "Telegram bot RAG components initialized." not in latest_start
        ):
            print("  FAIL Telegram bot received a message but RAG initialization did not finish")
            return False
        if (
            "Loading embedding model for query processing..." in latest_start
            and "Embedding model loaded for query processing." not in latest_start
        ):
            print("  FAIL Telegram bot started query processing but embedding model did not finish loading")
            return False

        print("  OK Telegram bot started and polling")
        return True
    except Exception as e:
        print(f"  FAIL {e}")
        return False


def _count_jsonl(path: Path, required_fields: set[str]) -> tuple[bool, int]:
    if not path.exists():
        return True, 0

    count = 0
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            count += 1
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  FAIL {path}:{line_no} invalid JSON: {exc}")
                return False, count
            missing = required_fields - set(payload)
            if missing:
                print(f"  FAIL {path}:{line_no} missing fields: {sorted(missing)}")
                return False, count
    return True, count


def check_feedback_storage() -> bool:
    print("- Feedback storage")
    feedback_file = FEEDBACK_DIR / "feedback.jsonl"
    events_file = FEEDBACK_DIR / "feedback_events.jsonl"

    if not FEEDBACK_DIR.exists():
        print(f"  WARN {FEEDBACK_DIR} does not exist yet; no user feedback has been saved")
        return True

    triplets_ok, triplet_count = _count_jsonl(
        feedback_file,
        {"query", "query_id", "positive_doc", "negative_doc", "timestamp"},
    )
    events_ok, event_count = _count_jsonl(
        events_file,
        {"query_id", "query", "answer", "liked", "timestamp", "triplets_created"},
    )
    if not triplets_ok or not events_ok:
        return False

    print(f"  OK events={event_count} triplets={triplet_count}")
    if event_count == 0:
        print("  WARN no feedback events yet; fine-tuning will skip until users rate answers")
    elif triplet_count == 0:
        print("  WARN feedback exists but no triplets yet; disliked answers may need relevant_doc_ids")
    return True


def check_fine_tuned_model_path() -> bool:
    print("- Fine-tuned model path")
    model_dir = Path("models") / "fine_tuned_bi_encoder"
    docker_volume_note = "models_data:/app/models"
    if model_dir.exists():
        config_file = model_dir / "config_sentence_transformers.json"
        print(f"  OK local model directory exists: {model_dir}")
        if config_file.exists():
            print(f"  OK sentence-transformers config found: {config_file}")
        return True
    print(
        "  WARN fine-tuned model is not present yet; "
        f"training job writes to Docker volume {docker_volume_note}"
    )
    return True


def selected_checks(modes: list[str]):
    check_groups = {
        "runtime": [
            ("FastAPI", check_fastapi),
            ("RAG readiness", check_rag_readiness),
            ("FastAPI metrics", lambda: check_http("FastAPI metrics", "http://localhost:8088/metrics")),
            ("Qdrant health", lambda: check_http("Qdrant health", "http://localhost:6333/healthz")),
            ("Qdrant collection", check_qdrant_collection),
            ("MinIO console", lambda: check_http("MinIO console", "http://localhost:9001")),
            ("MinIO S3 API", lambda: check_http("MinIO S3 API", "http://localhost:9000", expected=(200, 403))),
            ("Feedback storage", check_feedback_storage),
            ("Docker ps", check_docker_ps),
            ("FastAPI logs", check_fastapi_logs),
            ("Telegram bot", check_telegram_bot),
        ],
        "airflow": [
            ("Airflow health", lambda: check_http("Airflow health", "http://localhost:8080/health")),
            ("Docker ps", check_docker_ps),
        ],
        "observability": [
            ("Prometheus", lambda: check_http("Prometheus", "http://localhost:9090/-/healthy")),
            ("Prometheus targets", check_prometheus_targets),
            ("Grafana", lambda: check_http("Grafana", "http://localhost:3000/api/health")),
            ("Docker ps", check_docker_ps),
        ],
        "training": [
            ("MLflow", lambda: check_http("MLflow", "http://localhost:5000")),
            ("Feedback storage", check_feedback_storage),
            ("Fine-tuned model path", check_fine_tuned_model_path),
            ("Docker ps", check_docker_ps),
        ],
    }

    if "all" in modes:
        modes = ["runtime", "airflow", "observability", "training"]

    checks = []
    seen = set()
    for mode in modes:
        for name, check in check_groups[mode]:
            key = (mode, name) if name != "Docker ps" else ("shared", name)
            if key in seen:
                continue
            seen.add(key)
            checks.append((name, check))
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description="Health checks for the RAG stack.")
    parser.add_argument(
        "--mode",
        action="append",
        choices=["runtime", "airflow", "observability", "training", "all"],
        default=None,
        help=(
            "Check group to run. Can be repeated. "
            "Default: runtime. Use --mode all for the full stack."
        ),
    )
    args = parser.parse_args()
    modes = args.mode or ["runtime"]

    print_header(f"Service Health Check ({', '.join(modes)})")

    checks = [(name, check()) for name, check in selected_checks(modes)]

    print_header("Summary")
    failed = [name for name, ok in checks if not ok]
    if failed:
        print(f"FAIL: {', '.join(failed)}")
        return 1
    print("OK: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
