import json
import subprocess
import sys
import time
import urllib.error
import urllib.request


SEPARATOR = "=" * 72


def print_header(title: str):
    print(SEPARATOR)
    print(f"  {title}")
    print(SEPARATOR)


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


def check_qdrant_collection() -> bool:
    print("- Qdrant collection definitions")
    try:
        status, body = fetch("http://localhost:6333/collections/definitions", timeout=5)
        if status != 200:
            print(f"  FAIL status {status}: {body[:300]}")
            return False
        data = json.loads(body)
        result = data.get("result", {})
        print(
            "  OK "
            f"points={result.get('points_count', 'N/A')} "
            f"status={result.get('status', 'N/A')}"
        )
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
            timeout=15,
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
            timeout=15,
            encoding="utf-8",
            errors="replace",
        )
        logs = (result.stdout or "") + (result.stderr or "")
        for line in logs.strip().splitlines()[-5:]:
            print(f"  {line}")
        return result.returncode == 0
    except Exception as e:
        print(f"  FAIL {e}")
        return False


def main() -> int:
    print_header("Service Health Check")

    checks = [
        ("FastAPI", check_fastapi()),
        ("FastAPI metrics", check_http("FastAPI metrics", "http://localhost:8088/metrics")),
        ("Qdrant health", check_http("Qdrant health", "http://localhost:6333/healthz")),
        ("Qdrant collection", check_qdrant_collection()),
        ("MinIO console", check_http("MinIO console", "http://localhost:9001")),
        ("MinIO S3 API", check_http("MinIO S3 API", "http://localhost:9000", expected=(200, 403))),
        ("Airflow health", check_http("Airflow health", "http://localhost:8080/health")),
        ("MLflow", check_http("MLflow", "http://localhost:5000")),
        ("Grafana", check_http("Grafana", "http://localhost:3000/api/health")),
        ("Prometheus", check_http("Prometheus", "http://localhost:9090/-/healthy")),
        ("Prometheus targets", check_prometheus_targets()),
        ("Docker ps", check_docker_ps()),
        ("FastAPI logs", check_fastapi_logs()),
    ]

    print_header("Summary")
    failed = [name for name, ok in checks if not ok]
    if failed:
        print(f"FAIL: {', '.join(failed)}")
        return 1
    print("OK: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
