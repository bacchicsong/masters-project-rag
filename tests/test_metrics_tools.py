"""
Tests for src/tools/metrics.py (Precision@k, Recall@k, evaluate_system).
"""
import pytest
from tools.metrics import calculate_precision_at_k, calculate_recall_at_k, evaluate_system


class TestPrecisionAtK:
    def test_exact_match(self):
        """All retrieved docs are relevant."""
        result = calculate_precision_at_k(
            actual=[1, 2, 3], predicted=[1, 2, 3, 4, 5], k=3
        )
        assert result == 1.0

    def test_partial_match(self):
        """Some retrieved docs are relevant."""
        result = calculate_precision_at_k(
            actual=[1, 4], predicted=[1, 2, 3, 4, 5], k=3
        )
        assert result == 1 / 3

    def test_no_match(self):
        """No retrieved docs are relevant."""
        result = calculate_precision_at_k(
            actual=[6, 7], predicted=[1, 2, 3, 4, 5], k=3
        )
        assert result == 0.0

    def test_k_larger_than_predicted(self):
        """k is larger than predicted list."""
        result = calculate_precision_at_k(actual=[1], predicted=[1], k=5)
        assert result == 1 / 5

    def test_k_zero(self):
        assert calculate_precision_at_k(actual=[1], predicted=[1], k=0) == 0.0


class TestRecallAtK:
    def test_all_retrieved(self):
        assert calculate_recall_at_k(actual=[1, 2], predicted=[1, 2, 3], k=2) == 1.0

    def test_partial(self):
        assert calculate_recall_at_k(actual=[1, 2, 3], predicted=[1, 4, 5], k=2) == 1 / 3

    def test_no_match(self):
        assert calculate_recall_at_k(actual=[1, 2], predicted=[3, 4], k=2) == 0.0

    def test_empty_actual(self):
        assert calculate_recall_at_k(actual=[], predicted=[1, 2], k=3) == 0.0


class TestEvaluateSystem:
    def test_perfect(self):
        gt = {"q1": [1, 2], "q2": [3]}
        pred = {"q1": [1, 2, 3], "q2": [3, 1, 2]}
        avg_p, avg_r = evaluate_system(gt, pred, k=2)
        # q1: P@2=1.0, R@2=1.0; q2: P@2=0.5 (3 is 1 of 2 in top-2), R@2=1.0
        assert avg_p == 0.75
        assert avg_r == 1.0

    def test_partial(self):
        gt = {"q1": [1, 2], "q2": [3, 4]}
        pred = {"q1": [1, 5, 6], "q2": [7, 8, 3]}
        avg_p, avg_r = evaluate_system(gt, pred, k=2)
        assert avg_p == 0.25
        assert avg_r == 0.25

    def test_missing_prediction(self):
        gt = {"q1": [1], "q2": [2]}
        pred = {"q1": [1]}
        avg_p, avg_r = evaluate_system(gt, pred, k=1)
        assert avg_p == 1.0
        assert avg_r == 1.0

    def test_empty_ground_truth(self):
        avg_p, avg_r = evaluate_system({}, {"q1": [1]}, k=1)
        assert avg_p == 0.0
        assert avg_r == 0.0