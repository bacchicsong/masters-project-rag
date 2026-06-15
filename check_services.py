import urllib.request, urllib.parse, json, subprocess, time, sys

SEPARATOR = "=" * 60

print(SEPARATOR)
print("  Service Health Check")
print(SEPARATOR)
print()

# ── Configuration ───────────────────────────────────────────────────
FASTAPI_URL = "http://localhost:8088/api/v1/health"
FASTAPI_TIMEOUT = 5
FASTAPI_MAX_RETRIES = 3
FASTAPI_RETRY_DELAY = 10  # seconds between retries

# ── FastAPI health (with retries) ──────────────────────────────────
print(f"[1/6] FastAPI ({FASTAPI_URL})")
fastapi_ok = False
for attempt in range(1, FASTAPI_MAX_RETRIES + 1):
    try:
        resp = urllib.request.urlopen(FASTAPI_URL, timeout=FASTAPI_TIMEOUT)
        body = resp.read().decode()
        print(f"  ✅ OK: {resp.status} | Response: {body}")
        fastapi_ok = True
        break
    except Exception as e:
        if attempt < FASTAPI_MAX_RETRIES:
            print(f"  ⏳ Attempt {attempt}/{FASTAPI_MAX_RETRIES} failed — retrying in {FASTAPI_RETRY_DELAY}s...")
            time.sleep(FASTAPI_RETRY_DELAY)
        else:
            print(f"  ❌ FAIL (after {FASTAPI_MAX_RETRIES} attempts): {e}")
print()

# ── Qdrant ──────────────────────────────────────────────────────────
print("[2/6] Qdrant (http://localhost:6333/healthz)")
try:
    resp = urllib.request.urlopen("http://localhost:6333/healthz", timeout=5)
    print(f"  ✅ OK: {resp.status}")
except Exception as e:
    print(f"  ❌ FAIL: {e}")

# ── Qdrant collection size ──────────────────────────────────────────
print("[2.1] Qdrant Collection 'definitions'")
try:
    req = urllib.request.Request("http://localhost:6333/collections/definitions")
    resp = urllib.request.urlopen(req, timeout=5)
    data = json.loads(resp.read().decode())
    result = data.get("result", {})
    points_count = result.get("points_count", "N/A")
    vectors_size = result.get("config", {}).get("params", {}).get("vectors", {}).get("size", "N/A")
    status = result.get("status", "N/A")
    print(f"  ✅ Points: {points_count} | Vector size: {vectors_size} | Status: {status}")
except Exception as e:
    print(f"  ⚠️  Could not read collection info: {e}")
print()

# ── MinIO ──────────────────────────────────────────────────────────
print("[3/6] MinIO (http://localhost:9001)")
try:
    resp = urllib.request.urlopen("http://localhost:9001", timeout=5)
    print(f"  ✅ OK: {resp.status}")
except Exception as e:
    print(f"  ❌ FAIL: {e}")

# ── MinIO S3 API ──────────────────────────────────────────────────
print("[3.1] MinIO S3 API (http://localhost:9000)")
try:
    resp = urllib.request.urlopen("http://localhost:9000", timeout=5)
    print(f"  ✅ OK: {resp.status}")
except Exception as e:
    print(f"  ⚠️  FAIL: {e}")
print()

# ── MLflow ─────────────────────────────────────────────────────────
print("[4/6] MLflow (http://localhost:5000)")
try:
    resp = urllib.request.urlopen("http://localhost:5000", timeout=5)
    print(f"  ✅ OK: {resp.status}")
except Exception as e:
    print(f"  ❌ FAIL: {e}")
print()

# ── Telegram ───────────────────────────────────────────────────────
print("[5/6] Telegram Bot (docker logs)")
try:
    r = subprocess.run(
        ["docker", "logs", "fastapi_app"],
        capture_output=True,
        timeout=15,
        encoding="utf-8",
        errors="replace",
    )
    logs = (r.stdout or "") + (r.stderr or "")
    if "Starting Telegram bot" in logs:
        print(f"  ✅ RUNNING")
    elif "Starting Telegram bot in background" in logs:
        print(f"  ✅ RUNNING (background)")
    elif "TELEGRAM_BOT_TOKEN not set" in logs:
        print(f"  ⚠️  TOKEN MISSING")
    elif "Failed to start Telegram bot" in logs:
        print(f"  ❌ FAILED TO START")
    else:
        if "Uvicorn running on" in logs:
            print(f"  ⏳ Still loading model...")
        else:
            print(f"  ❓ UNKNOWN — no startup message yet")
except Exception as e:
    print(f"  ❌ Error reading logs: {e}")
    logs = ""
print()

# ── Docker containers status (bonus) ───────────────────────────────
print("[6/6] Docker Container Status")
try:
    r = subprocess.run(
        ["docker", "ps", "-a", "--format", "table {{.Names}}\t{{.Status}}\t{{.Ports}}"],
        capture_output=True,
        timeout=15,
        encoding="utf-8",
        errors="replace",
    )
    output = r.stdout or r.stderr or ""
    for line in output.strip().split("\n"):
        print(f"  {line}")
except Exception as e:
    print(f"  ❌ Error: {e}")
print()

# ── Show last 5 log lines ─────────────────────────────────────────
print(SEPARATOR)
print("  FastAPI Last 5 Log Lines")
print(SEPARATOR)
if logs:
    lines = logs.strip().split("\n")
    for l in lines[-5:]:
        print(l)
else:
    print("  (no logs available)")