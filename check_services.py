import urllib.request, urllib.parse, json, subprocess, time

# ── Configuration ───────────────────────────────────────────────────
FASTAPI_URL = "http://localhost:8088/api/v1/health"
FASTAPI_TIMEOUT = 5
FASTAPI_MAX_RETRIES = 3
FASTAPI_RETRY_DELAY = 10  # seconds between retries

# ── FastAPI health (with retries) ──────────────────────────────────
fastapi_ok = False
for attempt in range(1, FASTAPI_MAX_RETRIES + 1):
    try:
        resp = urllib.request.urlopen(FASTAPI_URL, timeout=FASTAPI_TIMEOUT)
        body = resp.read().decode()
        print(f"FASTAPI: OK {resp.status} {body}")
        fastapi_ok = True
        break
    except Exception as e:
        if attempt < FASTAPI_MAX_RETRIES:
            print(f"FASTAPI: attempt {attempt}/{FASTAPI_MAX_RETRIES} failed — retrying in {FASTAPI_RETRY_DELAY}s...")
            time.sleep(FASTAPI_RETRY_DELAY)
        else:
            print(f"FASTAPI FAIL (after {FASTAPI_MAX_RETRIES} attempts): {e}")

# ── Qdrant ──────────────────────────────────────────────────────────
try:
    resp = urllib.request.urlopen("http://localhost:6333/healthz", timeout=5)
    print("QDRANT: OK " + str(resp.status))
except Exception as e:
    print("QDRANT FAIL: " + str(e))

# ── Qdrant collection size ──────────────────────────────────────────
try:
    req = urllib.request.Request("http://localhost:6333/collections/definitions")
    resp = urllib.request.urlopen(req, timeout=5)
    data = json.loads(resp.read().decode())
    points_count = data.get("result", {}).get("points_count", "N/A")
    vectors_size = data.get("result", {}).get("config", {}).get("params", {}).get("vectors", {}).get("size", "N/A")
    print(f"QDRANT COLLECTION 'definitions': {points_count} points, vector_size={vectors_size}")
except Exception as e:
    print(f"QDRANT COLLECTION INFO: {e}")

# ── MinIO ──────────────────────────────────────────────────────────
try:
    resp = urllib.request.urlopen("http://localhost:9001", timeout=5)
    print("MINIO: OK " + str(resp.status))
except Exception as e:
    print("MINIO FAIL: " + str(e))

# ── MLflow ─────────────────────────────────────────────────────────
try:
    resp = urllib.request.urlopen("http://localhost:5000", timeout=5)
    print("MLFLOW: OK " + str(resp.status))
except Exception as e:
    print("MLFLOW FAIL: " + str(e))

# ── Telegram ───────────────────────────────────────────────────────
r = subprocess.run(["docker", "logs", "fastapi_app"], capture_output=True, text=True, timeout=10)
logs = r.stdout + r.stderr
if "Starting Telegram bot" in logs:
    print("TELEGRAM BOT: RUNNING")
elif "TELEGRAM_BOT_TOKEN not set" in logs:
    print("TELEGRAM BOT: TOKEN MISSING")
else:
    if "Uvicorn running on" in logs:
        print("TELEGRAM BOT: still loading model...")
    else:
        print("TELEGRAM BOT: UNKNOWN - no startup message yet")

# ── Show last 5 log lines ─────────────────────────────────────────
print("\n=== FastAPI last 5 log lines ===")
lines = logs.strip().split("\n")
for l in lines[-5:]:
    print(l)