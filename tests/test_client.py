"""
Tests for FastAPIClient.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from tools.client import FastAPIClient
from domain.query.delivery.dto.dto import QueryResponseDTO


class MockResponse:
    """Synchronous mock response for httpx."""
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code

    def json(self):
        return self._json_data

    def raise_for_status(self):
        pass


@pytest.fixture
def mock_async_client():
    """Create a mock that behaves like an async context manager."""
    mock_response = MockResponse({"text": "test answer"})
    
    class MockAsyncClient:
        """Replacement for httpx.AsyncClient that returns controlled responses."""
        def __init__(self, **kwargs):
            pass
        
        async def __aenter__(self):
            return self
        
        async def __aexit__(self, *args):
            pass
        
        async def post(self, url, **kwargs):
            return MockResponse({"text": "test answer"})
    
    return MockAsyncClient


class TestFastAPIClient:
    @pytest.fixture
    def client(self):
        return FastAPIClient(
            base_url="http://localhost:8088",
            api_token="test-token-123",
        )

    @pytest.mark.asyncio
    async def test_forward_success(self, client, mock_async_client):
        """forward() should return QueryResponseDTO on success."""
        with patch("tools.client.httpx.AsyncClient", mock_async_client):
            result = await client.forward(query_topic="Что такое ПИФ?")

        assert isinstance(result, QueryResponseDTO)
        assert result.text == "test answer"

    @pytest.mark.asyncio
    async def test_forward_with_system_prompt(self, client, mock_async_client):
        """forward() should include system_prompt when provided."""
        with patch("tools.client.httpx.AsyncClient", mock_async_client):
            result = await client.forward(
                query_topic="Что такое акция?",
                system_prompt="Ты - финансовый эксперт.",
            )
        assert result.text == "test answer"

    @pytest.mark.asyncio
    async def test_forward_sends_correct_auth_header(self, client, mock_async_client):
        """forward() should send X-Token header (matching server's verify_token)."""

        captured_headers = {}

        class HeaderCapturingClient:
            def __init__(self, **kwargs):
                pass
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def post(self, url, headers=None, **kwargs):
                captured_headers.update(headers or {})
                return MockResponse({"text": "answer"})

        with patch("tools.client.httpx.AsyncClient", HeaderCapturingClient):
            await client.forward(query_topic="Тест")

        assert captured_headers.get("X-Token") == "test-token-123"