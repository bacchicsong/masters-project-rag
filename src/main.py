import asyncio
import logging
import time
import uuid
import json

from fastapi import FastAPI, Request
from fastapi.responses import Response
from starlette.middleware.base import BaseHTTPMiddleware
from telegram.ext import Application

from config.config import RAG_CONFIG
from domain.query.delivery.controller import router
from infrastructure.db.qdrand import get_embedded_model
from infrastructure.telegram_bot import start_telegram_bot, stop_telegram_bot
from prometheus_fastapi_instrumentator import Instrumentator

# ── Structured JSON Logging Setup ──────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("app_logger")


class JsonFormatter(logging.Formatter):
    """Format log records as JSON lines (one JSON object per line)."""

    # Standard LogRecord attributes that we handle explicitly
    _RESERVED = frozenset({
        "name", "msg", "args", "created", "relativeCreated", "msecs",
        "levelname", "levelno", "pathname", "filename", "module",
        "funcName", "lineno", "exc_info", "exc_text", "stack_info",
        "message", "message", "thread", "threadName", "process",
        "processName", "process", "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Pull in all extra= fields that were passed via logger.info("msg", extra={...})
        for key, value in record.__dict__.items():
            if key not in self._RESERVED and not key.startswith("_"):
                log_entry[key] = value

        # Include exception info if present
        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry, ensure_ascii=False)


# Apply JSON formatter to our app logger (and its handlers)
for handler in logger.handlers:
    handler.setFormatter(JsonFormatter())
# Ensure the root logger also uses JSON for uvicorn access logs
root_logger = logging.getLogger()
for handler in root_logger.handlers:
    handler.setFormatter(JsonFormatter())


# ── Request Logging Middleware ─────────────────────────────────────────
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Log every request + response with method, path, status, duration."""

    async def dispatch(self, request: Request, call_next):
        request_id = str(uuid.uuid4())[:8]
        request.state.request_id = request_id

        # Parse body for logging (only if needed — avoid on large uploads)
        body = None
        content_type = request.headers.get("content-type", "")
        if "multipart/form-data" not in content_type:
            try:
                body_bytes = await request.body()
                if body_bytes:
                    body = body_bytes.decode("utf-8", errors="replace")[:500]
            except Exception:
                body = "<unreadable>"

        start = time.monotonic()
        try:
            response: Response = await call_next(request)
        except Exception as exc:
            duration = time.monotonic() - start
            logger.error(
                "request",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "query": str(request.query_params),
                    "client_ip": request.client.host if request.client else None,
                    "body": body,
                    "status_code": 500,
                    "duration_ms": round(duration * 1000, 2),
                    "error": str(exc),
                },
            )
            raise

        duration = time.monotonic() - start

        # Capture response body for error responses (optional)
        response_body = None
        if response.status_code >= 400:
            try:
                body_chunks = [chunk async for chunk in response.body_iterator]
                response_body = b"".join(body_chunks).decode("utf-8", errors="replace")[:500]
                # Reconstruct the response with same body
                response = Response(
                    content=b"".join(body_chunks),
                    status_code=response.status_code,
                    headers=dict(response.headers),
                    media_type=response.media_type,
                )
            except Exception:
                pass

        log_data = {
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "query": str(request.query_params),
            "client_ip": request.client.host if request.client else None,
            "body": body,
            "status_code": response.status_code,
            "duration_ms": round(duration * 1000, 2),
        }
        if response_body:
            log_data["response_body"] = response_body

        if response.status_code < 400:
            logger.info("request", extra=log_data)
        else:
            logger.warning("request", extra=log_data)

        return response


# ── App Initialisation ─────────────────────────────────────────────────
app = FastAPI(docs_url="/swagger")

app.include_router(router)

# Add the request logging middleware
app.add_middleware(RequestLoggingMiddleware)

# Setup Prometheus metrics instrumentation
instrumentator = Instrumentator(
    should_group_status_codes=True,
    should_ignore_untemplated=True,
    should_instrument_requests_inprogress=True,
    excluded_handlers=["/metrics", "/health"],
)
instrumentator.instrument(app).expose(app, endpoint="/metrics", tags=["monitoring"])

telegram_app: Application | None = None


async def _preload_embedding_model():
    logger.info("Preloading embedding model in background...")
    try:
        await asyncio.to_thread(get_embedded_model)
        logger.info("Embedding model preloaded.")
    except Exception:
        logger.exception("Embedding model preload failed.")


@app.on_event("startup")
async def startup():
    global telegram_app
    if RAG_CONFIG.ENABLE_TELEGRAM_BOT and RAG_CONFIG.TELEGRAM_BOT_TOKEN:
        logger.info("Starting Telegram bot in background task...")
        # Fire-and-forget: bot init (model loading) happens in the background
        # so uvicorn starts accepting requests immediately
        async def _init_bot():
            global telegram_app
            retry_delay = 30
            while telegram_app is None:
                try:
                    telegram_app = await start_telegram_bot()
                except Exception:
                    logger.exception(
                        "Failed to start Telegram bot; retrying",
                        extra={"retry_delay_s": retry_delay},
                    )
                    await asyncio.sleep(retry_delay)

        asyncio.create_task(_init_bot())
    else:
        logger.warning("Telegram bot disabled or TELEGRAM_BOT_TOKEN not set.")

    if RAG_CONFIG.PRELOAD_EMBEDDING_MODEL:
        asyncio.create_task(_preload_embedding_model())

    logger.info("FastAPI is ready to accept requests.")


@app.on_event("shutdown")
async def shutdown():
    global telegram_app
    if telegram_app:
        await stop_telegram_bot(telegram_app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=RAG_CONFIG.APP_HOST, port=RAG_CONFIG.APP_PORT)
