"""
Tests for Telegram bot handlers with mocked updates.
"""
import pytest
from unittest.mock import MagicMock, AsyncMock

from telegram import Update, Message, Chat, User
from telegram.ext import ContextTypes

from infrastructure.telegram_bot import start, handle_message


@pytest.fixture
def mock_update():
    update = MagicMock(spec=Update)
    update.message = MagicMock(spec=Message)
    update.message.reply_text = AsyncMock()
    update.message.chat = MagicMock()
    update.message.chat.send_action = AsyncMock()
    update.message.text = "Что такое ПИФ?"
    return update


@pytest.fixture
def mock_context():
    context = MagicMock(spec=ContextTypes.DEFAULT_TYPE)
    context.bot_data = {}
    return context


@pytest.mark.asyncio
async def test_start_command(mock_update, mock_context):
    """Start command should send welcome message."""
    await start(mock_update, mock_context)
    mock_update.message.reply_text.assert_awaited_once()
    text = mock_update.message.reply_text.call_args[0][0]
    assert "Привет" in text
    assert "финансовый ассистент" in text


@pytest.mark.asyncio
async def test_handle_message_no_usecase(mock_update, mock_context):
    """Message with no usecase should show error."""
    mock_context.bot_data = {}
    await handle_message(mock_update, mock_context)
    mock_update.message.reply_text.assert_awaited_once_with(
        "❌ Ошибка: RAG-система ещё не инициализирована."
    )


@pytest.mark.asyncio
async def test_handle_message_success(mock_update, mock_context):
    """Message should process and reply with result."""
    mock_usecase = MagicMock()
    mock_usecase.processes_query = AsyncMock()
    mock_usecase.processes_query.return_value.text = "ПИФ - это паевой инвестиционный фонд."
    mock_context.bot_data = {"query_usecase": mock_usecase}

    await handle_message(mock_update, mock_context)
    mock_update.message.chat.send_action.assert_awaited_once_with(action="typing")
    mock_update.message.reply_text.assert_awaited_once_with(
        "ПИФ - это паевой инвестиционный фонд."
    )


@pytest.mark.asyncio
async def test_handle_message_error(mock_update, mock_context):
    """Message should show error on exception."""
    mock_usecase = MagicMock()
    mock_usecase.processes_query = AsyncMock(side_effect=RuntimeError("API error"))
    mock_context.bot_data = {"query_usecase": mock_usecase}

    await handle_message(mock_update, mock_context)
    mock_update.message.reply_text.assert_awaited_once_with(
        "⚠️ Произошла ошибка при обработке запроса."
    )