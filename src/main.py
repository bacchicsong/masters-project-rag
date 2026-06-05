import asyncio
import logging

from fastapi import FastAPI
from telegram.ext import Application

from config.config import RAG_CONFIG
from domain.query.delivery.controller import router
from infrastructure.telegram_bot import start_telegram_bot, stop_telegram_bot

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app_logger")

app = FastAPI(docs_url="/swagger")

app.include_router(router)

telegram_app: Application | None = None


@app.on_event("startup")
async def startup():
    global telegram_app
    if RAG_CONFIG.TELEGRAM_BOT_TOKEN:
        logger.info("Starting Telegram bot...")
        telegram_app = await start_telegram_bot()
    else:
        logger.warning("TELEGRAM_BOT_TOKEN not set — bot won't start.")


@app.on_event("shutdown")
async def shutdown():
    global telegram_app
    if telegram_app:
        await stop_telegram_bot(telegram_app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=RAG_CONFIG.APP_HOST, port=RAG_CONFIG.APP_PORT)
