import logging

from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)

from config.config import RAG_CONFIG
from domain.query.query import Query
from domain.query.usecase.query_usecase import QueryUsecase
from infrastructure.db.qdrand import init_qdrant


logger = logging.getLogger("app_logger")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привет! Я финансовый ассистент по инструментам Московской Биржи.\n\n"
        "Задай мне вопрос, например:\n"
        "Что такое акции?\n"
        "Расскажи про ОФЗ\n"
        "Что такое ETF?"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_text = update.message.text

    usecase: QueryUsecase = context.bot_data.get("query_usecase")
    if not usecase:
        await update.message.reply_text("❌ Ошибка: RAG-система ещё не инициализирована.")
        return

    await update.message.chat.send_action(action="typing")

    try:
        query = Query(query_topic=user_text)
        result = await usecase.processes_query(query)
        await update.message.reply_text(result.text)
    except Exception as e:
        logger.error(f"Telegram bot error: {e}", exc_info=True)
        await update.message.reply_text("⚠️ Произошла ошибка при обработке запроса.")


async def start_telegram_bot() -> Application:
    logger.info("Initializing Telegram bot RAG components...")
    qdrant_client = init_qdrant(logger)
    usecase = QueryUsecase(qdrant=qdrant_client, logger=logger, config=RAG_CONFIG)

    application = (
        Application.builder()
        .token(RAG_CONFIG.TELEGRAM_BOT_TOKEN)
        .build()
    )

    application.bot_data["query_usecase"] = usecase

    application.add_handler(CommandHandler("start", start))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    await application.initialize()
    await application.start()
    await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)

    logger.info("Telegram bot started and polling.")
    return application


async def stop_telegram_bot(application: Application):
    logger.info("Stopping Telegram bot...")
    await application.updater.stop()
    await application.stop()
    await application.shutdown()
