"""
Tests for QueryUsecase with mocked dependencies.
"""
import pytest
from unittest.mock import MagicMock, AsyncMock, patch, PropertyMock


class TestQueryUsecase:
    @pytest.fixture
    def mock_qdrant(self):
        mock = MagicMock()
        mock.query_points.return_value = MagicMock(points=[])
        return mock

    @pytest.fixture
    def mock_model(self):
        mock = MagicMock()
        mock.encode.return_value = [0.1, 0.2, 0.3]
        return mock

    @pytest.fixture
    def usecase(self, mock_qdrant, mock_model):
        with patch(
            "domain.query.usecase.query_usecase.get_embedded_model",
            return_value=mock_model,
        ), patch(
            "domain.query.usecase.query_usecase.CrossEncoder",
        ):
            from domain.query.usecase.query_usecase import QueryUsecase
            from config.config import RAG_CONFIG

            usecase = QueryUsecase(
                qdrant=mock_qdrant, logger=MagicMock(), config=RAG_CONFIG
            )
            # Override cross_encoder with mock
            usecase.cross_encoder = MagicMock()
            usecase.cross_encoder.predict.return_value = [0.5, 0.3]
            return usecase

    @pytest.mark.asyncio
    async def test_processes_query_empty_topic(self, usecase):
        """Should raise ValueError for empty query topic."""
        from domain.query.query import Query

        with pytest.raises(ValueError, match="Query topic cannot be empty."):
            await usecase.processes_query(Query(query_topic=""))

    @pytest.mark.asyncio
    async def test_processes_query_adds_to_history(self, usecase, mock_model, mock_qdrant):
        """Successful query should append to history."""
        from domain.query.query import Query

        with patch.object(usecase, "_get_giga_token", return_value="fake-token"), patch.object(
            usecase, "_call_gigachat_api", return_value="test answer"
        ):
            result = await usecase.processes_query(Query(query_topic="Что такое ПИФ?"))

        assert result.text == "test answer"
        assert len(usecase.history) == 1
        # History items are HistoryItemDTO objects (not subscriptable dicts)
        item = usecase.history[0]
        assert item.query == "Что такое ПИФ?"
        assert item.response == "test answer"

    @pytest.mark.asyncio
    async def test_processes_query_saves_context_for_feedback(self, usecase):
        """Query context should be stored for later feedback."""
        from domain.query.query import Query

        with patch.object(usecase, "_get_giga_token", return_value="fake-token"), patch.object(
            usecase, "_call_gigachat_api", return_value="test answer"
        ):
            result = await usecase.processes_query(Query(query_topic="Что такое ОФЗ?"))

        assert result.query_id in usecase._query_context
        ctx = usecase._query_context[result.query_id]
        assert ctx["query"] == "Что такое ОФЗ?"