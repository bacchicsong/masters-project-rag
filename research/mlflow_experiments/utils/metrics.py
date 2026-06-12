"""
Evaluation metrics for RAG retrieval experiments.
"""
from typing import List, Dict, Set, Union, Optional
import numpy as np


def precision_at_k(
    relevant: List[Union[int, str]],
    retrieved: List[Union[int, str]],
    k: int
) -> float:
    """
    Precision@k: fraction of retrieved documents that are relevant.

    Args:
        relevant: List of relevant document IDs
        retrieved: List of retrieved document IDs (ordered)
        k: Cut-off rank

    Returns:
        Precision@k value (0.0 to 1.0)
    """
    if k <= 0 or len(retrieved) == 0:
        return 0.0

    relevant_set = set(relevant)
    top_k = retrieved[:k]
    hits = sum(1 for doc in top_k if doc in relevant_set)

    return hits / k


def recall_at_k(
    relevant: List[Union[int, str]],
    retrieved: List[Union[int, str]],
    k: int
) -> float:
    """
    Recall@k: fraction of relevant documents that are retrieved.

    Args:
        relevant: List of relevant document IDs
        retrieved: List of retrieved document IDs (ordered)
        k: Cut-off rank

    Returns:
        Recall@k value (0.0 to 1.0)
    """
    if not relevant or k <= 0:
        return 0.0

    relevant_set = set(relevant)
    top_k = retrieved[:k]
    hits = sum(1 for doc in top_k if doc in relevant_set)

    return hits / len(relevant_set)


def mean_reciprocal_rank(
    queries_relevant: List[List[Union[int, str]]],
    queries_retrieved: List[List[Union[int, str]]]
) -> float:
    """
    Mean Reciprocal Rank (MRR): average of reciprocal ranks of the first relevant document.

    Args:
        queries_relevant: List of relevant document IDs per query
        queries_retrieved: List of retrieved document IDs per query (ordered)

    Returns:
        MRR value
    """
    if not queries_relevant or not queries_retrieved:
        return 0.0

    reciprocal_ranks = []

    for relevant, retrieved in zip(queries_relevant, queries_retrieved):
        relevant_set = set(relevant)

        for rank, doc_id in enumerate(retrieved, start=1):
            if doc_id in relevant_set:
                reciprocal_ranks.append(1.0 / rank)
                break
        else:
            reciprocal_ranks.append(0.0)

    return float(np.mean(reciprocal_ranks)) if reciprocal_ranks else 0.0


def compute_retrieval_metrics(
    ground_truth: Dict[str, List[str]],
    predictions: Dict[str, List[str]],
    k_values: Optional[List[int]] = None
) -> Dict[str, float]:
    """
    Compute comprehensive retrieval metrics across all queries.

    Args:
        ground_truth: Dict mapping query_id -> list of relevant doc IDs
        predictions: Dict mapping query_id -> list of retrieved doc IDs (ordered)
        k_values: List of k values for P@k and R@k (default: [1, 3, 5, 8])

    Returns:
        Dict with aggregated metrics (mean_P@k, mean_R@k, MRR)
    """
    if k_values is None:
        k_values = [1, 3, 5, 8]

    metrics = {}

    # Per-k metrics
    for k in k_values:
        p_list = []
        r_list = []

        for q_id in ground_truth:
            if q_id not in predictions:
                continue

            relevant = ground_truth[q_id]
            retrieved = predictions[q_id]

            if not relevant:
                continue

            p_list.append(precision_at_k(relevant, retrieved, k))
            r_list.append(recall_at_k(relevant, retrieved, k))

        metrics[f"mean_P@{k}"] = float(np.mean(p_list)) if p_list else 0.0
        metrics[f"mean_R@{k}"] = float(np.mean(r_list)) if r_list else 0.0

    # MRR
    all_relevant = list(ground_truth.values())
    all_retrieved = [predictions[q_id] for q_id in ground_truth if q_id in predictions]
    metrics["MRR"] = mean_reciprocal_rank(all_relevant, all_retrieved)

    return metrics