"""
Experiment 5: End-to-end RAG Evaluation
========================================
Evaluate the complete RAG pipeline combining retrieval + generation.
Tests different combinations of:
- Embedding models × Retrieval strategies
- Context sizes
- Generation parameters

Logs composite metrics and artifact snapshots to MLflow → MinIO.
"""
import json
import time
import tempfile
import os
from typing import List, Dict, Any, Optional

import mlflow
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from rank_bm25 import BM25Okapi

from mlflow_config import setup_experiment
from utils.metrics import compute_retrieval_metrics, compute_generation_metrics, compute_performance_metrics
from utils.data_loader import load_test_queries
from utils.document_processor import get_text_by_strategy, DocumentProcessor


# === Configuration ===
EXPERIMENT_KEY = "end-to-end"
K_VALUES = [1, 3, 5]

# Pipeline configurations to test
PIPELINE_CONFIGS = [
    {
        "name": "dense-full-context",
        "retrieval_type": "dense",
        "model_id": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        "strategy": "full",
        "context_docs": 3,
        "max_response_tokens": 256,
        "temperature": 0.3,
    },
    {
        "name": "hybrid-full-context",
        "retrieval_type": "hybrid",
        "model_id": "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2",
        "strategy": "full",
        "bm25_k": 50,
        "context_docs": 3,
        "max_response_tokens": 256,
        "temperature": 0.3,
    },
    {
        "name": "dense-title-context",
        "retrieval_type": "dense",
        "model_id": "intfloat/multilingual-e5-small",
        "strategy": "title_headings",
        "context_docs": 5,
        "max_response_tokens": 128,
        "temperature": 0.1,
    },
    {
        "name": "hybrid-detailed",
        "retrieval_type": "hybrid",
        "model_id": "sentence-transformers/distiluse-base-multilingual-cased-v2",
        "strategy": "full",
        "bm25_k": 100,
        "context_docs": 5,
        "max_response_tokens": 512,
        "temperature": 0.7,
    },
]

TEST_QUESTIONS = [
    "Что такое акция и какие они бывают?",
    "Как работает индивидуальный инвестиционный счет?",
    "Какие налоги нужно платить при инвестировании?",
    "Что такое дивиденды по акциям?",
    "В чем разница между ETF и ПИФом?",
    "Что такое ОФЗ и насколько это надежный инструмент?",
    "Как открыть брокерский счет?",
    "Что такое ликвидность ценных бумаг?",
]


def simulate_generation(
    context: str,
    question: str,
    max_tokens: int,
    temperature: float,
) -> str:
    """
    Simulate LLM generation for E2E evaluation.
    In production, replace with actual GigaChat/Qwen call.
    """
    context_length = len(context)
    response_length = min(
        max_tokens,
        int(context_length * 0.15 + np.random.uniform(20, 50))
    )
    response = f"[Симулированный E2E ответ ({response_length} символов). "
    response += f"Вопрос: {question[:50]}... "
    response += f"Контекст из {context_length} символов]"
    return response[:response_length]


def build_context(
    docs: List[Dict[str, Any]],
    retrieved_indices: List[int],
    strategy: str,
    max_docs: int = 3,
    include_url: bool = True,
) -> str:
    """Build context string from retrieved documents."""
    context_parts = []

    for i, idx in enumerate(retrieved_indices[:max_docs]):
        doc = docs[idx]
        title = doc.get("title", "No title")
        text = get_text_by_strategy(doc, strategy)

        doc_context = f"[Документ {i + 1}] {title}\n{text}"
        if include_url and doc.get("url"):
            doc_context += f"\nИсточник: {doc['url']}"

        context_parts.append(doc_context)

    return "\n\n".join(context_parts)


def run_experiment():
    """Run the end-to-end RAG evaluation experiment."""
    experiment_name = setup_experiment(EXPERIMENT_KEY)

    # Load test data
    docs, test_queries = load_test_queries(use_mock=True)
    all_questions = [tq["query"] for tq in test_queries] + TEST_QUESTIONS

    doc_texts_cache = {}  # Cache document texts by strategy

    for pipeline in PIPELINE_CONFIGS:
        pipeline_name = pipeline["name"]
        print(f"\n{'='*60}")
        print(f"[EXP] Pipeline: {pipeline_name}")
        print(f"{'='*60}")

        strategy = pipeline["strategy"]
        model_id = pipeline["model_id"]

        # Cache document texts for this strategy
        if strategy not in doc_texts_cache:
            doc_texts_cache[strategy] = [get_text_by_strategy(doc, strategy) for doc in docs]

        doc_texts = doc_texts_cache[strategy]
        doc_ids = [doc.get("id", str(i)) for i, doc in enumerate(docs)]

        # Load/setup embedding model
        print(f"   Loading model: {model_id}")
        model = SentenceTransformer(model_id)

        # Encode documents
        print(f"   Encoding {len(docs)} documents...")
        doc_embeddings = model.encode(
            doc_texts, show_progress_bar=True, convert_to_numpy=True, normalize_embeddings=True
        )

        # Setup BM25 for hybrid
        bm25 = None
        if pipeline["retrieval_type"] == "hybrid":
            tokenized_docs = [t.lower().split() for t in doc_texts]
            bm25 = BM25Okapi(tokenized_docs)

        with mlflow.start_run(run_name=pipeline_name):
            # Log pipeline configuration
            mlflow.log_params({
                "pipeline_name": pipeline_name,
                "retrieval_type": pipeline["retrieval_type"],
                "embedding_model": model_id,
                "strategy": strategy,
                "context_docs": pipeline["context_docs"],
                "max_response_tokens": pipeline["max_response_tokens"],
                "temperature": pipeline["temperature"],
                "num_documents": len(docs),
                "num_queries": len(all_questions),
            })

            # ========== RETRIEVAL EVALUATION ==========
            retrieval_times = []
            generation_times = []
            all_responses = []
            all_latencies = []

            for qi, question in enumerate(all_questions):
                # --- Retrieval phase ---
                start_time = time.perf_counter()

                if pipeline["retrieval_type"] == "hybrid":
                    bm25_k = pipeline.get("bm25_k", 50)
                    tokenized_query = question.lower().split()
                    bm25_scores = bm25.get_scores(tokenized_query)
                    candidates = min(bm25_k, len(doc_embeddings))
                    candidate_indices = np.argsort(bm25_scores)[::-1][:candidates]
                    q_emb = model.encode([question], normalize_embeddings=True)
                    candidate_embeddings = doc_embeddings[candidate_indices]
                    sims = cosine_similarity(q_emb, candidate_embeddings)[0]
                    top_indices = candidate_indices[np.argsort(sims)[::-1][:max(K_VALUES)]]
                else:
                    q_emb = model.encode([question], normalize_embeddings=True)
                    sims = cosine_similarity(q_emb, doc_embeddings)[0]
                    top_indices = np.argsort(sims)[::-1][:max(K_VALUES)]

                retrieval_time = time.perf_counter() - start_time
                retrieval_times.append(retrieval_time)

                # --- Generation phase (simulated) ---
                context = build_context(docs, top_indices.tolist(), strategy,
                                        max_docs=pipeline["context_docs"])

                gen_start_time = time.perf_counter()
                response = simulate_generation(
                    context, question,
                    max_tokens=pipeline["max_response_tokens"],
                    temperature=pipeline["temperature"],
                )
                gen_time = time.perf_counter() - gen_start_time
                generation_times.append(gen_time)

                all_responses.append(response)
                all_latencies.append(retrieval_time + gen_time)

            # ========== LOG METRICS ==========
            # Retrieval metrics
            retrieval_metrics = {
                "avg_retrieval_time_s": float(np.mean(retrieval_times)),
                "p95_retrieval_time_s": float(np.percentile(retrieval_times, 95)),
                "min_retrieval_time_s": float(np.min(retrieval_times)),
                "max_retrieval_time_s": float(np.max(retrieval_times)),
            }
            mlflow.log_metrics(retrieval_metrics)

            # Generation metrics
            gen_metrics = compute_generation_metrics(all_responses, latencies=generation_times)
            mlflow.log_metrics({
                f"gen_{k}": v for k, v in gen_metrics.items()
            })

            # Overall E2E metrics
            overall_metrics = {
                "avg_e2e_latency_s": float(np.mean(all_latencies)),
                "p95_e2e_latency_s": float(np.percentile(all_latencies, 95)),
                "throughput_queries_per_sec": len(all_questions) / max(sum(all_latencies), 0.001),
            }
            mlflow.log_metrics(overall_metrics)

            # Log pipeline config as artifact
            mlflow.log_text(
                json.dumps(pipeline, ensure_ascii=False, indent=2),
                f"pipeline_config_{pipeline_name}.json"
            )

            # Log sample responses as artifact
            sample_output = []
            for qi, question in enumerate(all_questions[:5]):
                sample_output.append({
                    "question": question,
                    "response_length_chars": len(all_responses[qi]),
                    "response_length_tokens": len(all_responses[qi].split()),
                    "latency_s": all_latencies[qi],
                })

            mlflow.log_text(
                json.dumps(sample_output, ensure_ascii=False, indent=2),
                f"sample_responses_{pipeline_name}.json"
            )

            print(f"\n[CHART] Pipeline '{pipeline_name}':")
            print(f"   Avg retrieval: {retrieval_metrics['avg_retrieval_time_s']:.4f}s")
            print(f"   Avg generation: {gen_metrics.get('avg_latency_seconds', 0):.4f}s")
            print(f"   Avg E2E latency: {overall_metrics['avg_e2e_latency_s']:.4f}s")
            print(f"   Throughput: {overall_metrics['throughput_queries_per_sec']:.2f} q/s")

    print(f"\n[OK] Experiment '{experiment_name}' completed!")


if __name__ == "__main__":
    run_experiment()