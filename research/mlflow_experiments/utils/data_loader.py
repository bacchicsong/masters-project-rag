"""
Data loading utilities for RAG experiments.
Loads documents from JSON files, creates test queries with ground truth.
"""
import json
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple


def load_documents(
    data_dir: str = "data",
    max_docs: Optional[int] = None,
    file_pattern: str = "*.json"
) -> List[Dict[str, Any]]:
    """
    Load documents from JSON files in a directory.

    Args:
        data_dir: Directory containing JSON files (relative to project root)
        max_docs: Maximum number of documents to load (None = all)
        file_pattern: Glob pattern for file matching

    Returns:
        List of document dicts with 'title', 'sections', 'url' keys
    """
    # Resolve relative to the research directory or given dir
    project_root = Path(__file__).resolve().parent.parent.parent.parent  # goes to project root
    json_dir = project_root / data_dir if not Path(data_dir).is_absolute() else Path(data_dir)

    all_docs: List[Dict[str, Any]] = []

    if not json_dir.exists():
        print(f"[WARN] Data directory not found: {json_dir}")
        return all_docs

    for file_path in sorted(json_dir.glob(file_pattern)):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            if isinstance(data, dict):
                if "sections" in data:
                    all_docs.append(data)
                else:
                    # Treat as a single document
                    all_docs.append(data)
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "sections" in item:
                        all_docs.append(item)
                    elif isinstance(item, dict):
                        all_docs.append(item)

            if max_docs and len(all_docs) >= max_docs:
                all_docs = all_docs[:max_docs]
                break

        except Exception as e:
            print(f"[WARN] Error loading {file_path.name}: {e}")

    print(f"[FILE] Loaded {len(all_docs)} documents from {json_dir}")
    return all_docs


def load_test_queries(
    use_mock: bool = True,
    num_queries: Optional[int] = None
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Load or generate test queries with ground truth document associations.

    Args:
        use_mock: If True, use synthetic mock data. If False, attempt to load real test data.
        num_queries: Limit number of queries (None = all)

    Returns:
        Tuple of (documents, test_queries)
        - documents: list of document dicts
        - test_queries: list of dicts with 'query', 'query_id', 'relevant_doc_ids' keys
    """
    if use_mock:
        return _load_mock_data(num_queries)
    else:
        return _load_real_data(num_queries)


def _load_mock_data(num_queries: Optional[int] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Generate synthetic documents and test queries for controlled evaluation."""
    docs = [
        {
            "id": "doc_1",
            "title": "Брокерский счет",
            "url": "docs/broker",
            "sections": [
                {"heading": "Что такое брокерский счет", "content": ["Брокерский счет позволяет торговать акциями и валютой самостоятельно.", "Для открытия счета нужен паспорт."]},
            ]
        },
        {
            "id": "doc_2",
            "title": "Индивидуальный Инвестиционный Счет (ИИС)",
            "url": "docs/iis",
            "sections": [
                {"heading": "Преимущества ИИС", "content": ["ИИС дает право на налоговый вычет 13% от взносов (тип А).", "Деньги нельзя снимать 3 года без потери льгот."]},
            ]
        },
        {
            "id": "doc_3",
            "title": "Налогообложение инвестиций",
            "url": "docs/taxes",
            "sections": [
                {"heading": "Налог на доход", "content": ["Налог на доход от инвестиций составляет 13% для резидентов.", "Брокер выступает налоговым агентом и удерживает налог автоматически."]},
            ]
        },
        {
            "id": "doc_4",
            "title": "Дивиденды по акциям",
            "url": "docs/dividends",
            "sections": [
                {"heading": "Дивидендная доходность", "content": ["Дивиденды — это часть прибыли компании, распределяемая между акционерами.", "Налог на дивиденды удерживается у источника выплаты."]},
            ]
        },
        {
            "id": "doc_5",
            "title": "ETF и БПИФ",
            "url": "docs/etf",
            "sections": [
                {"heading": "Что такое ETF", "content": ["ETF (Exchange Traded Fund) — биржевой инвестиционный фонд.", "БПИФ — российский аналог ETF."]},
            ]
        },
        {
            "id": "doc_6",
            "title": "ОФЗ (Облигации Федерального Займа)",
            "url": "docs/ofz",
            "sections": [
                {"heading": "Государственные облигации", "content": ["ОФЗ — долговые ценные бумаги, выпускаемые Министерством финансов РФ.", "Считаются одним из самых надежных инструментов."]},
            ]
        },
        {
            "id": "doc_7",
            "title": "Акции и их виды",
            "url": "docs/stocks",
            "sections": [
                {"heading": "Типы акций", "content": ["Обыкновенные акции дают право голоса на собрании акционеров.", "Привилегированные акции гарантируют фиксированный дивиденд."]},
            ]
        },
        {
            "id": "doc_8",
            "title": "ПИФ (Паевой Инвестиционный Фонд)",
            "url": "docs/pif",
            "sections": [
                {"heading": "Как работает ПИФ", "content": ["ПИФ — это форма коллективного инвестирования.", "Управляющая компания инвестирует средства пайщиков в различные активы."]},
            ]
        },
    ]

    test_queries = [
        {"query_id": "q1", "query": "Как открыть брокерский счет?", "relevant_doc_ids": ["doc_1"]},
        {"query_id": "q2", "query": "Что такое ИИС и какие налоговые льготы?", "relevant_doc_ids": ["doc_2"]},
        {"query_id": "q3", "query": "Как облагается налогом доход от инвестиций?", "relevant_doc_ids": ["doc_3", "doc_4"]},
        {"query_id": "q4", "query": "В чем разница между ETF и БПИФ?", "relevant_doc_ids": ["doc_5"]},
        {"query_id": "q5", "query": "Что такое ОФЗ и насколько они надежны?", "relevant_doc_ids": ["doc_6"]},
        {"query_id": "q6", "query": "Какие бывают виды акций?", "relevant_doc_ids": ["doc_7"]},
        {"query_id": "q7", "query": "Как работает паевой инвестиционный фонд?", "relevant_doc_ids": ["doc_8"]},
        {"query_id": "q8", "query": "Какие налоги платит инвестор?", "relevant_doc_ids": ["doc_3"]},
        {"query_id": "q9", "query": "Что выгоднее: ИИС или брокерский счет?", "relevant_doc_ids": ["doc_1", "doc_2"]},
        {"query_id": "q10", "query": "Как получить дивиденды по акциям?", "relevant_doc_ids": ["doc_4", "doc_7"]},
    ]

    if num_queries:
        test_queries = test_queries[:num_queries]

    print(f"[LIST] Generated {len(test_queries)} mock test queries with {len(docs)} documents")
    return docs, test_queries


def _load_real_data(num_queries: Optional[int] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Attempt to load real data from the project's data directory."""
    docs = load_documents()

    # For real data, we'll create a generic test query set
    # (In production, this would come from a labeled dataset)
    test_queries = [
        {"query_id": "q1", "query": "Что такое акция?", "relevant_doc_ids": []},
        {"query_id": "q2", "query": "Как продать акцию?", "relevant_doc_ids": []},
        {"query_id": "q3", "query": "Что такое ПИФ?", "relevant_doc_ids": []},
        {"query_id": "q4", "query": "Как работает брокерский счет?", "relevant_doc_ids": []},
        {"query_id": "q5", "query": "Что такое облигации?", "relevant_doc_ids": []},
    ]

    if num_queries:
        test_queries = test_queries[:num_queries]

    print(f"[LIST] Loaded {len(docs)} real documents, {len(test_queries)} test queries")
    return docs, test_queries