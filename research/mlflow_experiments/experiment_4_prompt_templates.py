"""
Experiment 4: Prompt Template Experiments
==========================================
Compare different prompt templates and generation parameters for the RAG generator.
Since this experiment requires an LLM (GigaChat or local model like Qwen),
it simulates generation metrics using the hybrid retriever + configurable prompt templates.

Techniques tested:
1. Different system prompt styles (default, concise, detailed)
2. Temperature variation
3. Max tokens variation
4. Context document count variation
"""
import json
import time
import tempfile
import os
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import mlflow
import numpy as np

from mlflow_config import setup_experiment
from utils.metrics import compute_generation_metrics
from utils.data_loader import load_test_queries
from utils.document_processor import get_text_by_strategy, DocumentProcessor


# === Configuration ===
EXPERIMENT_KEY = "prompt-templates"

# Prompt template definitions
PROMPT_TEMPLATES = {
    "default": {
        "system": "Ты - полезный AI-ассистент, который отвечает на вопросы на основе предоставленного контекста. Используй только информацию из контекста. Отвечай кратко и по существу.",
        "user": "Контекст:\n{context}\n\nВопрос: {question}\n\nОтвет:",
    },
    "concise": {
        "system": "Ты - AI-ассистент. Отвечай кратко на основе контекста.",
        "user": "Контекст:\n{context}\n\nВопрос: {question}\n\nКраткий ответ:",
    },
    "detailed": {
        "system": "Ты - экспертный AI-ассистент в области финансов и инвестиций. Дай подробный, структурированный ответ. Приведи примеры из контекста.",
        "user": "Контекст:\n{context}\n\nВопрос: {question}\n\nПодробный ответ:",
    },
    "russian_finance": {
        "system": "Ты - финансовый консультант по инструментам Московской Биржи. Отвечай на русском языке, используя профессиональную терминологию.",
        "user": "Контекст:\n{context}\n\nВопрос инвестора: {question}\n\nОтвет консультанта:",
    },
}

# Generation parameters to sweep
TEMPERATURES = [0.1, 0.3, 0.7]
MAX_TOKENS = [128, 256, 512]
CONTEXT_DOCS_COUNTS = [1, 3, 5]

# Test queries for prompt evaluation
TEST_QUESTIONS = [
    "Что такое акция?",
    "Как работает ИИС?",
    "В чем разница между обыкновенными и привилегированными акциями?",
    "Какие налоги платит инвестор?",
    "Что такое дивиденды и как их получить?",
    "Как открыть брокерский счет?",
    "Что такое ОФЗ?",
    "Как работает ETF?",
]


def format_prompt(template: Dict[str, str], context: str, question: str) -> Dict[str, str]:
    """Format a prompt using the given template."""
    system_msg = template["system"]
    user_msg = template["user"].format(context=context, question=question)
    return {"system": system_msg, "user": user_msg}


def estimate_generation_latency(
    prompt_length_chars: int,
    max_tokens: int,
    temperature: float,
    base_latency_per_token: float = 0.05,
) -> float:
    """
    Estimate generation latency based on prompt length and parameters.
    Used as a proxy when no actual LLM is available.
    Higher temperature = slightly more variance/latency.
    Longer prompts = more processing time.
    """
    estimated_tokens = min(max_tokens, prompt_length_chars // 4)
    latency = estimated_tokens * base_latency_per_token * (1 + temperature * 0.2)
    # Add prompt processing overhead
    latency += prompt_length_chars * 0.0001
    return latency


def simulate_generation(
    prompt: Dict[str, str],
    max_tokens: int,
    temperature: float,
) -> str:
    """
    Simulate LLM generation for evaluation.
    In production, this would call GigaChat API or a local model.
    For now, returns a simulated response length estimate.
    """
    # Simulate response length proportional to max_tokens and prompt complexity
    prompt_length = len(prompt["system"]) + len(prompt["user"])
    response_length = min(
        max_tokens,
        int(prompt_length * 0.3 * (1 + np.random.uniform(-0.1, 0.1)))
    )

    # Generate simulated response text (placeholder)
    response = f"[Симулированный ответ из {response_length} символов. "
    response += f"Prompt template: {prompt['system'][:50]}... "
    response += f"Temperature: {temperature}]"
    response = response[:response_length]

    return response


def run_experiment():
    """Run the prompt template experiments."""
    experiment_name = setup_experiment(EXPERIMENT_KEY)

    # Load documents for context building
    docs, _ = load_test_queries(use_mock=True)
    doc_processor = DocumentProcessor()

    print(f"\n{'='*60}")
    print("[RESULTS] Part 1: Prompt Template Comparison")
    print(f"{'='*60}")

    # Part 1: Compare all prompt templates with default params
    for template_name, template in PROMPT_TEMPLATES.items():
        run_name = f"template-{template_name}"
        print(f"\n   Testing template: {template_name}")

        responses = []
        latencies = []
        prompt_lengths = []

        for question in TEST_QUESTIONS:
            # Build simulated context from documents
            context_parts = []
            for doc in docs[:2]:  # Use first 2 docs as context
                text = get_text_by_strategy(doc, "full")
                context_parts.append(f"[{doc.get('title', 'Doc')}]: {text[:200]}")

            context = "\n\n".join(context_parts)

            # Format prompt
            prompt = format_prompt(template, context, question)
            prompt_text = f"{prompt['system']}\n{prompt['user']}"
            prompt_lengths.append(len(prompt_text))

            # Simulate generation
            start_time = time.perf_counter()
            response = simulate_generation(prompt, max_tokens=256, temperature=0.3)
            latency = time.perf_counter() - start_time
            latencies.append(latency)
            responses.append(response)

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "prompt_template": template_name,
                "system_prompt": template["system"][:100],
                "num_questions": len(TEST_QUESTIONS),
                "temperature": 0.3,
                "max_tokens": 256,
                "context_docs": 2,
            })

            metrics = compute_generation_metrics(responses, latencies=latencies)
            metrics["avg_prompt_length_chars"] = float(np.mean(prompt_lengths))
            mlflow.log_metrics(metrics)

            # Log template content as artifact
            mlflow.log_text(
                json.dumps(template, ensure_ascii=False, indent=2),
                f"template_{template_name}.json"
            )

            print(f"   [OK] {run_name}: avg_response={metrics['avg_response_length_tokens']:.0f} tokens, "
                  f"avg_latency={metrics.get('avg_latency_seconds', 0):.3f}s")

    # Part 2: Temperature sweep (with a single template)
    print(f"\n{'='*60}")
    print("[RESULTS] Part 2: Temperature Sweep (template=default)")
    print(f"{'='*60}")

    for temperature in TEMPERATURES:
        run_name = f"temperature={temperature}"
        print(f"\n   Testing temperature: {temperature}")

        responses = []
        latencies = []

        for question in TEST_QUESTIONS[:5]:  # Use subset for speed
            context_parts = []
            for doc in docs[:2]:
                text = get_text_by_strategy(doc, "full")
                context_parts.append(f"[{doc.get('title', 'Doc')}]: {text[:200]}")
            context = "\n\n".join(context_parts)

            prompt = format_prompt(PROMPT_TEMPLATES["default"], context, question)

            start_time = time.perf_counter()
            response = simulate_generation(prompt, max_tokens=256, temperature=temperature)
            latency = time.perf_counter() - start_time
            latencies.append(latency)
            responses.append(response)

            # Add artificial delay for higher temperatures (simulates more sampling time)
            time.sleep(temperature * 0.01)

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "prompt_template": "default",
                "temperature": temperature,
                "max_tokens": 256,
                "context_docs": 2,
                "num_questions": len(TEST_QUESTIONS[:5]),
                "experiment_part": "temperature-sweep",
            })

            metrics = compute_generation_metrics(responses, latencies=latencies)
            mlflow.log_metrics(metrics)

            print(f"   [OK] {run_name}: avg_response={metrics['avg_response_length_tokens']:.0f} tokens, "
                  f"avg_latency={metrics.get('avg_latency_seconds', 0):.3f}s")

    # Part 3: Context doc count variation
    print(f"\n{'='*60}")
    print("[RESULTS] Part 3: Context Document Count Variation")
    print(f"{'='*60}")

    for num_docs in CONTEXT_DOCS_COUNTS:
        run_name = f"context_docs={num_docs}"
        print(f"\n   Testing context docs: {num_docs}")

        responses = []
        latencies = []
        context_lengths = []

        for question in TEST_QUESTIONS[:5]:
            context_parts = []
            for doc in docs[:num_docs]:
                text = get_text_by_strategy(doc, "full")
                context_parts.append(f"[{doc.get('title', 'Doc')}]: {text[:300]}")
            context = "\n\n".join(context_parts)
            context_lengths.append(len(context))

            prompt = format_prompt(PROMPT_TEMPLATES["default"], context, question)

            start_time = time.perf_counter()
            response = simulate_generation(prompt, max_tokens=256, temperature=0.3)
            latency = time.perf_counter() - start_time
            latencies.append(latency)
            responses.append(response)

        with mlflow.start_run(run_name=run_name):
            mlflow.log_params({
                "prompt_template": "default",
                "temperature": 0.3,
                "max_tokens": 256,
                "context_docs": num_docs,
                "num_questions": len(TEST_QUESTIONS[:5]),
                "experiment_part": "context-docs-variation",
            })

            metrics = compute_generation_metrics(responses, latencies=latencies)
            metrics["avg_context_length_chars"] = float(np.mean(context_lengths))
            mlflow.log_metrics(metrics)

            print(f"   [OK] {run_name}: avg_response={metrics['avg_response_length_tokens']:.0f} tokens, "
                  f"avg_context={metrics.get('avg_context_length_chars', 0):.0f} chars")

    print(f"\n[OK] Experiment '{experiment_name}' completed!")


if __name__ == "__main__":
    run_experiment()