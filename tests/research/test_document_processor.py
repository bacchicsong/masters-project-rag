"""
Tests for data_loader utility.
"""
import pytest
from unittest.mock import patch, mock_open, MagicMock
import json

from research.mlflow_experiments.utils.data_loader import (
    load_documents,
    load_test_queries,
)


class TestLoadDocuments:
    def test_empty_directory(self):
        """load_documents should handle missing directory."""
        with patch("pathlib.Path.exists", return_value=False):
            docs = load_documents(data_dir="/nonexistent")
            assert docs == []

    @patch("research.mlflow_experiments.utils.data_loader.Path.glob")
    @patch("research.mlflow_experiments.utils.data_loader.Path.exists")
    def test_load_single_dict_doc(self, mock_exists, mock_glob):
        """Single dict document should be loaded."""
        mock_exists.return_value = True
        mock_file = MagicMock()
        mock_file.name = "test.json"
        mock_glob.return_value = [mock_file]

        doc_data = {"title": "Test", "sections": [{"heading": "H1", "content": ["C1"]}]}

        with patch("builtins.open", mock_open(read_data=json.dumps(doc_data))):
            docs = load_documents(data_dir="data")
            assert len(docs) == 1
            assert docs[0]["title"] == "Test"

    @patch("research.mlflow_experiments.utils.data_loader.Path.glob")
    @patch("research.mlflow_experiments.utils.data_loader.Path.exists")
    def test_load_list_of_dicts(self, mock_exists, mock_glob):
        """List of dict documents should be loaded."""
        mock_exists.return_value = True
        mock_file = MagicMock()
        mock_file.name = "test.json"
        mock_glob.return_value = [mock_file]

        doc_data = [
            {"title": "Doc1", "sections": []},
            {"title": "Doc2", "sections": [{"heading": "H1", "content": ["C1"]}]},
        ]

        with patch("builtins.open", mock_open(read_data=json.dumps(doc_data))):
            docs = load_documents(data_dir="data")
            assert len(docs) == 2


class TestLoadTestQueries:
    def test_mock_returns_data(self):
        """load_test_queries with use_mock=True should return synthetic data."""
        docs, queries = load_test_queries(use_mock=True)
        assert len(docs) == 8
        assert len(queries) == 10

    def test_mock_with_limit(self):
        """load_test_queries with num_queries should limit results."""
        docs, queries = load_test_queries(use_mock=True, num_queries=3)
        assert len(queries) == 3
        assert queries[0]["query_id"] == "q1"
        assert queries[2]["query_id"] == "q3"

    def test_mock_queries_have_required_fields(self):
        """Mock queries should have query_id, query, and relevant_doc_ids."""
        docs, queries = load_test_queries(use_mock=True)
        for q in queries:
            assert "query_id" in q
            assert "query" in q
            assert "relevant_doc_ids" in q
            assert len(q["relevant_doc_ids"]) > 0

    def test_real_data_returns_docs(self):
        """load_test_queries with use_mock=False should attempt real data."""
        with patch(
            "research.mlflow_experiments.utils.data_loader.load_documents",
            return_value=[{"title": "Real Doc"}],
        ):
            docs, queries = load_test_queries(use_mock=False)
            assert len(docs) == 1
            assert len(queries) == 5