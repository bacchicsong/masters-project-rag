"""
Evaluation metrics for RAG system experiments.
- Retrieval metrics: Precision@k, Recall@k, MRR, NDCG
- Generation metrics: response length, latency, overlap
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


def compute_generation_metrics(
    responses: List[str],
    reference_answers: Optional[List[str]] = None,
    latencies: Optional[List[float]] = None
) -> Dict[str, float]:
    """
    Compute generation quality metrics.

    Args:
        responses: List of generated response texts
        reference_answers: Optional list of reference answers (for reference-based metrics)
        latencies: Optional list of generation latencies in seconds

    Returns:
        Dict with metrics
    """
    metrics = {}

    # Response length statistics
    response_lengths = [len(r.split()) for r in responses]
    metrics["avg_response_length_tokens"] = float(np.mean(response_lengths))
    metrics["min_response_length_tokens"] = float(np.min(response_lengths))
    metrics["max_response_length_tokens"] = float(np.max(response_lengths))
    metrics["std_response_length_tokens"] = float(np.std(response_lengths))

    # Response character lengths
    char_lengths = [len(r) for r in responses]
    metrics["avg_response_length_chars"] = float(np.mean(char_lengths))

    # Latency metrics
    if latencies:
        metrics["avg_latency_seconds"] = float(np.mean(latencies))
        metrics["min_latency_seconds"] = float(np.min(latencies))
        metrics["max_latency_seconds"] = float(np.max(latencies))
        metrics["std_latency_seconds"] = float(np.std(latencies))
        metrics["p95_latency_seconds"] = float(np.percentile(latencies, 95))
        metrics["p99_latency_seconds"] = float(np.percentile(latencies, 99))

    return metrics


def compute_performance_metrics(
    wall_time: float,
    peak_memory_kib: float,
    rss_before: Optional[float] = None,
    rss_after: Optional[float] = None,
    gpu_before: Optional[float] = None,
    gpu_after: Optional[float] = None,
    num_docs: Optional[int] = None,
) -> Dict[str, float]:
    """
    Compute performance/resource metrics.

    Args:
        wall_time: Wall clock time in seconds
        peak_memory_kib: Peak Python memory allocation in KiB
        rss_before: RSS memory before in MB
        rss_after: RSS memory after in MB
        gpu_before: GPU memory before in MB
        gpu_after: GPU memory after in MB
        num_docs: Number of documents processed (for throughput)

    Returns:
        Dict with performance metrics
    """
    metrics = {
        "wall_time_s": wall_time,
        "peak_memory_kib": peak_memory_kib,
        "peak_memory_mb": peak_memory_kib / 1024.0,
    }

    if rss_before is not None and rss_after is not None:
        metrics["rss_delta_mb"] = rss_after - rss_before

    if gpu_before is not None and gpu_after is not None:
        metrics["gpu_delta_mb"] = gpu_after - gpu_before

    if num_docs and wall_time > 0:
        metrics["throughput_docs_per_sec"] = num_docs / wall_time

    return metrics