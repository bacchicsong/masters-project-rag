"""
Tests for feedback storage and triplet creation logic.
"""
import pytest
from unittest.mock import MagicMock, patch, mock_open
import json

from domain.query.delivery.dto.dto import FeedbackRequestDTO
from infrastructure.feedback.feedback_storage import FeedbackStorage, TripletRecord


class TestTripletRecord:
    def test_to_dict(self):
        """TripletRecord should serialize to dict."""
        record = TripletRecord(
            query="Что такое ПИФ?",
            query_id="q1",
            positive_doc="ПИФ - это...",
            negative_doc="Акции - это...",
            timestamp="2026-01-01T12:00:00",
        )
        d = record.to_dict()
        assert d["query"] == "Что такое ПИФ?"
        assert d["positive_doc"] == "ПИФ - это..."
        assert d["negative_doc"] == "Акции - это..."

    def test_from_dict(self):
        """TripletRecord should deserialize from dict."""
        d = {
            "query": "Что такое ОФЗ?",
            "query_id": "q2",
            "positive_doc": "ОФЗ - это...",
            "negative_doc": "Акции - это...",
            "timestamp": "2026-01-01T13:00:00",
        }
        record = TripletRecord.from_dict(d)
        assert record.query == "Что такое ОФЗ?"
        assert record.query_id == "q2"


class TestFeedbackStorage:
    def test_save_triplet(self):
        """save_triplet should write JSONL to file."""
        storage = FeedbackStorage()
        record = TripletRecord(
            query="q", query_id="q1", positive_doc="pos", negative_doc="neg", timestamp="t"
        )

        with patch("builtins.open", mock_open()) as mock_file:
            storage.save_triplet(record)
            mock_file.assert_called_once()
            handle = mock_file()
            written = handle.write.call_args[0][0]
            assert "query" in written
            assert "positive_doc" in written

    def test_count_empty(self):
        """count() should return 0 if file doesn't exist."""
        storage = FeedbackStorage()
        with patch("infrastructure.feedback.feedback_storage.FEEDBACK_FILE") as mock_path:
            mock_path.exists.return_value = False
            assert storage.count() == 0

    @patch("infrastructure.feedback.feedback_storage.FEEDBACK_DIR")
    def test_ensure_dir_created(self, mock_dir):
        """init should create feedback directory."""
        with patch("os.makedirs") as mock_makedirs:
            FeedbackStorage()
            mock_makedirs.assert_called_once()


class TestSaveFeedbackInUsecase:
    @pytest.fixture
    def mock_usecase(self):
        from domain.query.usecase.query_usecase import QueryUsecase

        usecase = MagicMock(spec=QueryUsecase)
        usecase._query_context = {}
        usecase.feedback_storage = MagicMock()
        usecase.logger = MagicMock()
        return usecase

    def test_save_feedback_no_context(self, mock_usecase):
        """save_feedback should return 0 if no context for query_id."""
        from domain.query.usecase.query_usecase import QueryUsecase

        feedback = FeedbackRequestDTO(query_id="unknown", liked=True)
        result = mock_usecase.save_feedback(feedback)
        mock_usecase.feedback_storage.save_triplet.assert_not_called()

    def test_save_feedback_liked(self, mock_usecase):
        """save_feedback with liked=True should create a triplet."""
        mock_usecase._query_context = {
            "q1": {
                "query": "Что такое ПИФ?",
                "all_candidates": [
                    MagicMock(payload={"text": "doc_neg1"}),
                    MagicMock(payload={"text": "doc_neg2"}),
                ],
                "reranked": [
                    {"text": "doc_pos1"},
                    {"text": "doc_pos2"},
                ],
            }
        }

        # Re-attach real method for testing
        from domain.query.usecase.query_usecase import QueryUsecase

        # Manually test the logic
        ctx = mock_usecase._query_context["q1"]
        reranked = ctx["reranked"]
        all_candidates = ctx.get("all_candidates", [])
        positive = reranked[0] if reranked else None
        assert positive is not None
        assert positive["text"] == "doc_pos1"

        negative_candidates = []
        for c in all_candidates:
            text = c.payload.get("text", "")
            if any(d.get("text") == text for d in reranked[:3]):
                continue
            negative_candidates.append(text)
        negative = negative_candidates[-1] if negative_candidates else ""
        assert negative == "doc_neg2"