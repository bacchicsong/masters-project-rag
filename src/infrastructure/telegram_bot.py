import logging
import asyncio

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

from config.config import RAG_CONFIG
from domain.query.query import Query
from domain.query.usecase.query_usecase import QueryUsecase
from domain.query.delivery.dto.dto import FeedbackRequestDTO
from infrastructure.db.qdrand import init_qdrant


logger = logging.getLogger("app_logger")

# Callback data prefix for feedback buttons
FEEDBACK_PREFIX = "feedback"
RAG_INIT_TIMEOUT_SECONDS = 120
RAG_QUERY_TIMEOUT_SECONDS = 180


def _build_query_usecase() -> QueryUsecase:
    qdrant_client = init_qdrant(logger)
    return QueryUsecase(qdrant=qdrant_client, logger=logger, config=RAG_CONFIG)


def _process_query_sync(usecase: QueryUsecase, user_text: str):
    return asyncio.run(usecase.processes_query(Query(query_topic=user_text)))


async def get_or_create_query_usecase(context: ContextTypes.DEFAULT_TYPE) -> QueryUsecase:
    usecase: QueryUsecase | None = context.bot_data.get("query_usecase")
    if usecase:
        return usecase

    lock = context.bot_data.setdefault("query_usecase_lock", asyncio.Lock())
    async with lock:
        usecase = context.bot_data.get("query_usecase")
        if usecase:
            return usecase
        logger.info("Lazy initializing Telegram bot RAG components...")
        usecase = await asyncio.wait_for(
            asyncio.to_thread(_build_query_usecase),
            timeout=RAG_INIT_TIMEOUT_SECONDS,
        )
        context.bot_data["query_usecase"] = usecase
        logger.info("Telegram bot RAG components initialized.")
        return usecase


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

    await update.message.chat.send_action(action="typing")

    try:
        if not context.bot_data.get("query_usecase"):
            await update.message.reply_text("Инициализирую RAG-систему, первый ответ может занять немного больше времени.")
        usecase = await get_or_create_query_usecase(context)
        result = await asyncio.wait_for(
            asyncio.to_thread(_process_query_sync, usecase, user_text),
            timeout=RAG_QUERY_TIMEOUT_SECONDS,
        )

        # Build inline keyboard with feedback buttons
        keyboard = [
            [
                InlineKeyboardButton("👍 Понравилось", callback_data=f"{FEEDBACK_PREFIX}:like:{result.query_id}"),
                InlineKeyboardButton("👎 Не понравилось", callback_data=f"{FEEDBACK_PREFIX}:dislike:{result.query_id}"),
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await update.message.reply_text(result.text, reply_markup=reply_markup)
    except Exception as e:
        logger.error(f"Telegram bot error: {e}", exc_info=True)
        if isinstance(e, asyncio.TimeoutError):
            await update.message.reply_text(
                "⚠️ RAG-система не успела подготовить ответ. Проверьте логи FastAPI: вероятно, модель эмбеддингов еще скачивается или зависла при инициализации."
            )
        else:
            await update.message.reply_text("⚠️ Произошла ошибка при обработке запроса.")


async def handle_feedback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle feedback button clicks (like/dislike)."""
    query = update.callback_query
    await query.answer()

    data = query.data
    # Expected format: "feedback:like:<query_id>" or "feedback:dislike:<query_id>"
    parts = data.split(":")
    if len(parts) != 3:
        logger.warning(f"Invalid callback data format: {data}")
        return

    _, action, query_id = parts

    usecase: QueryUsecase = context.bot_data.get("query_usecase")
    if not usecase:
        await query.message.reply_text("Оценку пока нельзя сохранить: RAG-система ещё не обработала запрос в этом процессе.")
        return

    liked = action == "like"

    try:
        feedback = FeedbackRequestDTO(
            query_id=query_id,
            liked=liked,
        )
        # save_feedback creates triplets — the return value (triplet count)
        # is intentionally NOT shown to the user to avoid service messages
        # like "(Создано 1 обучающих триплетов)"
        usecase.save_feedback(feedback)

        # Show a user-friendly confirmation without technical details
        if liked:
            confirmation = "✅ Спасибо за оценку! Рад быть полезным."
        else:
            confirmation = "✅ Принято, постараемся стать лучше!"

        # Edit the message to replace buttons with confirmation
        original_text = query.message.text or ""
        await query.edit_message_text(
            text=original_text,
            reply_markup=None,  # Remove inline keyboard
        )
        # Send a short confirmation as a new message
        await query.message.reply_text(confirmation)

    except Exception as e:
        logger.error(f"Feedback handling error: {e}", exc_info=True)
        await query.message.reply_text("⚠️ Не удалось сохранить оценку.")


async def start_telegram_bot() -> Application:
    logger.info("Starting Telegram bot without eager RAG model loading...")

    application = (
        Application.builder()
        .token(RAG_CONFIG.TELEGRAM_BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .get_updates_connect_timeout(30)
        .get_updates_read_timeout(60)
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(handle_feedback, pattern=f"^{FEEDBACK_PREFIX}:"))
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
