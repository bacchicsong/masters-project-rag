"""
Tests for Pydantic DTOs (request/response models).
"""
import pytest
from pydantic import ValidationError


class TestQueryResponseDTO:
    def test_valid(self):
        from domain.query.delivery.dto.dto import QueryResponseDTO

        dto = QueryResponseDTO(text="some answer")
        assert dto.text == "some answer"

    def test_empty_text(self):
        from domain.query.delivery.dto.dto import QueryResponseDTO

        dto = QueryResponseDTO(text="")
        assert dto.text == ""


class TestFeedbackRequestDTO:
    def test_valid_minimal(self):
        from domain.query.delivery.dto.dto import FeedbackRequestDTO

        dto = FeedbackRequestDTO(query_id="q1", liked=True)
        assert dto.query_id == "q1"
        assert dto.liked is True
        assert dto.relevant_doc_ids is None

    def test_valid_with_relevant_docs(self):
        from domain.query.delivery.dto.dto import FeedbackRequestDTO

        dto = FeedbackRequestDTO(query_id="q1", liked=False, relevant_doc_ids=[1, 2, 3])
        assert dto.relevant_doc_ids == [1, 2, 3]

    def test_missing_query_id(self):
        from domain.query.delivery.dto.dto import FeedbackRequestDTO

        with pytest.raises(ValidationError):
            FeedbackRequestDTO(liked=True)

    def test_missing_liked(self):
        from domain.query.delivery.dto.dto import FeedbackRequestDTO

        with pytest.raises(ValidationError):
            FeedbackRequestDTO(query_id="q1")


class TestFeedbackResponseDTO:
    def test_valid(self):
        from domain.query.delivery.dto.dto import FeedbackResponseDTO

        dto = FeedbackResponseDTO(status="ok", triplet_count=3)
        assert dto.status == "ok"
        assert dto.triplet_count == 3


class TestStatsResponseDTO:
    def test_valid(self):
        from domain.query.delivery.dto.dto import StatsResponseDTO

        dto = StatsResponseDTO(
            total_queries=10,
            mean_time=2.5,
            quantiles={"50%": 2.0, "95%": 5.0, "99%": 8.0},
            query_stats={"avg_query_len": 15.0, "max_query_len": 30, "min_query_len": 5},
        )
        assert dto.total_queries == 10
        assert dto.mean_time == 2.5