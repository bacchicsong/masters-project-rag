"""
Custom Prometheus metrics for the RAG pipeline.

Exposed alongside the auto-generated HTTP metrics from
prometheus-fastapi-instrumentator on the same /metrics endpoint.
"""

from prometheus_client import Counter, Histogram

# -- RAG query pipeline ------------------------------------------------

RAG_QUERY_DURATION = Histogram(
    "rag_query_duration_seconds",
    "End-to-end latency of a RAG query (encode -> search -> rerank -> generate)",
    buckets=(0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)

RAG_QUERIES_TOTAL = Counter(
    "rag_queries_total",
    "Total number of RAG queries processed",
)

# -- Qdrant search -----------------------------------------------------

RAG_QDRANT_SEARCH_DURATION = Histogram(
    "rag_qdrant_search_duration_seconds",
    "Latency of a Qdrant vector-search call",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# -- Cross-encoder re-ranking ------------------------------------------

RAG_RERANK_DURATION = Histogram(
    "rag_rerank_duration_seconds",
    "Latency of the cross-encoder re-ranking step",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# -- GigaChat LLM ------------------------------------------------------

RAG_GIGACHAT_CALL_DURATION = Histogram(
    "rag_gigachat_call_duration_seconds",
    "Latency of a GigaChat API call (token exchange + completion)",
    buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0),
)

RAG_GIGACHAT_ERRORS = Counter(
    "rag_gigachat_errors_total",
    "Number of failed GigaChat API calls",
)

# -- Embedding ---------------------------------------------------------

RAG_EMBEDDING_DURATION = Histogram(
    "rag_embedding_duration_seconds",
    "Latency of sentence-transformer encoding",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

# -- Feedback ----------------------------------------------------------

RAG_FEEDBACK_TOTAL = Counter(
    "rag_feedback_total",
    "Number of feedback submissions",
    labelnames=["liked"],
)