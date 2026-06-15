import urllib.request, urllib.parse, json, subprocess

# FastAPI health
try:
    resp = urllib.request.urlopen('http://localhost:8088/api/v1/health', timeout=5)
    print('FASTAPI: ' + str(resp.status) + ' ' + resp.read().decode())
except Exception as e:
    print('FASTAPI FAIL: ' + str(e))

# Qdrant
try:
    resp = urllib.request.urlopen('http://localhost:6333/healthz', timeout=5)
    print('QDRANT: OK ' + str(resp.status))
except Exception as e:
    print('QDRANT FAIL: ' + str(e))

# MinIO
try:
    resp = urllib.request.urlopen('http://localhost:9001', timeout=5)
    print('MINIO: OK ' + str(resp.status))
except Exception as e:
    print('MINIO FAIL: ' + str(e))

# MLflow
try:
    resp = urllib.request.urlopen('http://localhost:5000', timeout=5)
    print('MLFLOW: OK ' + str(resp.status))
except Exception as e:
    print('MLFLOW FAIL: ' + str(e))

# Telegram
r = subprocess.run(['docker', 'logs', 'fastapi_app'], capture_output=True, text=True, timeout=10)
logs = r.stdout + r.stderr
if 'Starting Telegram bot' in logs:
    print('TELEGRAM BOT: RUNNING')
elif 'TELEGRAM_BOT_TOKEN not set' in logs:
    print('TELEGRAM BOT: TOKEN MISSING')
else:
    if 'Uvicorn running on' in logs:
        print('TELEGRAM BOT: still loading model...')
    else:
        print('TELEGRAM BOT: UNKNOWN - no startup message yet')

# Show last 5 log lines
print('\n=== FastAPI last 5 log lines ===')
lines = logs.strip().split('\n')
for l in lines[-5:]:
    print(l)