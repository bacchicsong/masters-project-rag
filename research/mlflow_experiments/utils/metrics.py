import numpy as np


def compute_retrieval_metrics(ground_truth, predictions, k_values=None):
    if k_values is None:
        k_values = [1, 3, 5, 8]

    metrics = {}

    for k in k_values:
        p_scores, r_scores = [], []
        for qid, relevant in ground_truth.items():
            retrieved = predictions.get(qid)
            if not relevant or not retrieved:
                continue
            rel = set(relevant)
            top = retrieved[:k]
            hits = sum(1 for doc in top if doc in rel)
            p_scores.append(hits / k)
            r_scores.append(hits / len(rel))
        metrics[f"mean_P@{k}"] = np.mean(p_scores) if p_scores else 0
        metrics[f"mean_R@{k}"] = np.mean(r_scores) if r_scores else 0

    rr = []
    for qid, relevant in ground_truth.items():
        retrieved = predictions.get(qid)
        if not retrieved:
            rr.append(0)
            continue
        rel = set(relevant)
        rank = next((i for i, doc in enumerate(retrieved, 1) if doc in rel), None)
        rr.append(1 / rank if rank else 0)
    metrics["MRR"] = np.mean(rr) if rr else 0

    return metrics
