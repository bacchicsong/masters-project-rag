"""
Tests for the FastAPI controller endpoints.
Uses mocked dependencies to avoid external service calls.
"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from domain.query.delivery.controller import router
from domain.query.delivery.dto.dto import (
    QueryResponseDTO,
    HistoryResponseDTO,
    StatsResponseDTO,
    FeedbackRequestDTO,
    FeedbackResponseDTO,
    HistoryItemDTO,
)
from domain.query.query import Query as DomainQuery, QueryResults


@pytest.fixture
def mock_usecase():
    """Create a mock IQueryUsecase."""
    mock = MagicMock()
    mock.processes_query = AsyncMock()
    mock.history = []
    mock.save_feedback = MagicMock(return_value=0)
    return mock


@pytest.fixture
def app(mock_usecase):
    """Create a FastAPI test app with mocked dependencies."""
    app = FastAPI()
    app.include_router(router)

    # Override the dependency
    from infrastructure.di.dependencies import get_query_usecase, verify_token
    app.dependency_overrides[get_query_usecase] = lambda: mock_usecase
    app.dependency_overrides[verify_token] = lambda: "test-token"
    yield app
    app.dependency_overrides.clear()


@pytest.fixture
def client(app):
    return TestClient(app)


class TestForward:
    def test_forward_success(self, client, mock_usecase):
        """POST /api/v1/forward should return the generated text."""
        mock_usecase.processes_query.return_value = QueryResults(
            text="Ответ на вопрос про ПИФ."
        )
        response = client.post(
            "/api/v1/forward",
            data={"query_topic": "Что такое ПИФ?"},
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["text"] == "Ответ на вопрос про ПИФ."

    def test_forward_with_system_prompt(self, client, mock_usecase):
        """POST /api/v1/forward should accept optional system_prompt."""
        mock_usecase.processes_query.return_value = QueryResults(
            text="Ответ с кастомным промптом."
        )
        response = client.post(
            "/api/v1/forward",
            data={
                "query_topic": "Что такое акция?",
                "system_prompt": "Ты - финансовый эксперт.",
            },
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 200

    def test_forward_empty_query(self, client, mock_usecase):
        """POST /api/v1/forward should reject empty query_topic."""
        mock_usecase.processes_query.side_effect = ValueError(
            "Query topic cannot be empty."
        )
        response = client.post(
            "/api/v1/forward",
            data={"query_topic": ""},
            headers={"X-Token": "test-token"},
        )
        assert response.status_code in (400, 422)

    def test_forward_missing_query(self, client):
        """POST /api/v1/forward should reject missing query_topic."""
        response = client.post(
            "/api/v1/forward",
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 422

    def test_forward_unauthorized(self, client):
        """POST /api/v1/forward should reject missing auth token."""
        response = client.post(
            "/api/v1/forward",
            data={"query_topic": "Что такое ПИФ?"},
        )
        assert response.status_code == 400

    def test_forward_internal_error(self, client, mock_usecase):
        """POST /api/v1/forward should return 500 on unexpected error."""
        mock_usecase.processes_query.side_effect = RuntimeError("Unexpected error")
        response = client.post(
            "/api/v1/forward",
            data={"query_topic": "Что такое ОФЗ?"},
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 500


class TestHistory:
    def test_history_empty(self, client, mock_usecase):
        """GET /api/v1/history should return message when empty."""
        mock_usecase.history = []
        response = client.get(
            "/api/v1/history",
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "details" in data

    def test_history_with_data(self, client, mock_usecase):
        """GET /api/v1/history should return history items."""
        mock_usecase.history = [
            HistoryItemDTO(query="Что такое ПИФ?", response="Ответ", timestamp=1000.0, duration=0.5)
        ]
        response = client.get(
            "/api/v1/history?history_depth=10",
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, dict)
        assert "history" in data
        assert len(data["history"]) == 1
        assert data["history"][0]["query"] == "Что такое ПИФ?"


class TestStats:
    def test_stats_empty(self, client, mock_usecase):
        """GET /api/v1/stats should return message when empty."""
        mock_usecase.history = []
        response = client.get(
            "/api/v1/stats",
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 200
        assert "details" in response.json()

    def test_stats_with_data(self, client, mock_usecase):
        """GET /api/v1/stats should compute stats correctly."""
        mock_usecase.history = [
            HistoryItemDTO(query="q1", response="r1", timestamp=1000.0, duration=1.0),
            HistoryItemDTO(query="q2 longer", response="r2", timestamp=1001.0, duration=2.0),
            HistoryItemDTO(query="q3", response="r3", timestamp=1002.0, duration=3.0),
        ]
        response = client.get(
            "/api/v1/stats?history_depth=10",
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["total_queries"] == 3
        assert data["mean_time"] == 2.0
        assert "quantiles" in data
        assert data["query_stats"]["avg_query_len"] == pytest.approx(
            (2 + 9 + 2) / 3
        )


class TestHealth:
    def test_health_check(self, client):
        """POST /api/v1/health should return ok."""
        response = client.post("/api/v1/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestFeedback:
    def test_feedback_success(self, client, mock_usecase):
        """POST /api/v1/feedback should return status and triplet count."""
        mock_usecase.save_feedback.return_value = 2
        response = client.post(
            "/api/v1/feedback",
            json={"query_id": "q1", "liked": True, "relevant_doc_ids": [1, 2]},
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["triplet_count"] == 2

    def test_feedback_invalid(self, client):
        """POST /api/v1/feedback should reject invalid payload."""
        response = client.post(
            "/api/v1/feedback",
            json={"query_id": "q1"},
            headers={"X-Token": "test-token"},
        )
        assert response.status_code == 422