import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from utils.golden_set_loader import attach_ground_truth, load_qa_from_zip


def load_documents(
    data_dir: str = "data",
    max_docs: Optional[int] = None,
    file_pattern: str = "*.json",
) -> List[Dict[str, Any]]:
    project_root = Path(__file__).resolve().parent.parent.parent.parent
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

    for i, doc in enumerate(all_docs):
        if "id" not in doc:
            doc["id"] = f"doc_{i}"

    print(f"[FILE] Loaded {len(all_docs)} documents from {json_dir}")
    return all_docs


USE_MOCK = False


def load_golden_eval_set(
    num_queries: Optional[int] = None,
    zip_path: Optional[str] = None,
    use_mock: Optional[bool] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    mock = USE_MOCK if use_mock is None else use_mock
    if mock:
        docs, queries = _load_mock_data(num_queries)
        stats = {
            "total_qa": len(queries),
            "matched_queries": len(queries),
            "unmatched_queries": 0,
            "used_queries": len(queries),
            "unmatched_titles": [],
        }
        return docs, queries, stats

    docs = load_documents(file_pattern="tbank_articles_clean.json")
    qa_records = load_qa_from_zip(Path(zip_path) if zip_path else None)
    test_queries, stats = attach_ground_truth(qa_records, docs)

    if num_queries:
        test_queries = test_queries[:num_queries]
    stats["used_queries"] = len(test_queries)

    print(
        f"[GOLDEN] matched {stats['matched_queries']}/{stats['total_qa']} queries, "
        f"using {stats['used_queries']} for eval"
    )
    if stats["unmatched_queries"]:
        print(f"[GOLDEN] unmatched: {stats['unmatched_queries']}")

    return docs, test_queries, stats


def load_test_queries(
    use_mock: bool = False,
    num_queries: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    if use_mock:
        docs, queries = _load_mock_data(num_queries)
        return docs, queries

    docs, queries, _ = load_golden_eval_set(num_queries=num_queries)
    return docs, queries


def _load_mock_data(num_queries: Optional[int] = None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    docs = [
        {
            "id": "doc_1",
            "title": "Брокерский счет",
            "url": "docs/broker",
            "sections": [
                {"heading": "Что такое брокерский счет", "content": ["Брокерский счет позволяет торговать акциями и валютой самостоятельно.", "Для открытия счета нужен паспорт."]},
            ],
        },
        {
            "id": "doc_2",
            "title": "Индивидуальный Инвестиционный Счет (ИИС)",
            "url": "docs/iis",
            "sections": [
                {"heading": "Преимущества ИИС", "content": ["ИИС дает право на налоговый вычет 13% от взносов (тип А).", "Деньги нельзя снимать 3 года без потери льгот."]},
            ],
        },
        {
            "id": "doc_3",
            "title": "Налогообложение инвестиций",
            "url": "docs/taxes",
            "sections": [
                {"heading": "Налог на доход", "content": ["Налог на доход от инвестиций составляет 13% для резидентов.", "Брокер выступает налоговым агентом и удерживает налог автоматически."]},
            ],
        },
        {
            "id": "doc_4",
            "title": "Дивиденды по акциям",
            "url": "docs/dividends",
            "sections": [
                {"heading": "Дивидендная доходность", "content": ["Дивиденды — это часть прибыли компании, распределяемая между акционерами.", "Налог на дивиденды удерживается у источника выплаты."]},
            ],
        },
        {
            "id": "doc_5",
            "title": "ETF и БПИФ",
            "url": "docs/etf",
            "sections": [
                {"heading": "Что такое ETF", "content": ["ETF (Exchange Traded Fund) — биржевой инвестиционный фонд.", "БПИФ — российский аналог ETF."]},
            ],
        },
        {
            "id": "doc_6",
            "title": "ОФЗ (Облигации Федерального Займа)",
            "url": "docs/ofz",
            "sections": [
                {"heading": "Государственные облигации", "content": ["ОФЗ — долговые ценные бумаги, выпускаемые Министерством финансов РФ.", "Считаются одним из самых надежных инструментов."]},
            ],
        },
        {
            "id": "doc_7",
            "title": "Акции и их виды",
            "url": "docs/stocks",
            "sections": [
                {"heading": "Типы акций", "content": ["Обыкновенные акции дают право голоса на собрании акционеров.", "Привилегированные акции гарантируют фиксированный дивиденд."]},
            ],
        },
        {
            "id": "doc_8",
            "title": "ПИФ (Паевой Инвестиционный Фонд)",
            "url": "docs/pif",
            "sections": [
                {"heading": "Как работает ПИФ", "content": ["ПИФ — это форма коллективного инвестирования.", "Управляющая компания инвестирует средства пайщиков в различные активы."]},
            ],
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
