import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

from utils.golden_set_loader import attach_ground_truth, load_qa_from_zip, normalize_title

USE_MOCK = False
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
STRUCTURED_TBANK_PATH = PROJECT_ROOT / "research" / "data-collection" / "theoretical-texts" / "tbank_articles.json"


def _looks_like_chunk_corpus(docs: list[dict]) -> bool:
    if not docs:
        return False
    sample = docs[0]
    return "text" in sample and "meta" in sample and "sections" not in sample


def _load_structured_tbank_articles() -> list[dict]:
    if not STRUCTURED_TBANK_PATH.exists():
        return []
    with STRUCTURED_TBANK_PATH.open(encoding="utf-8") as f:
        return json.load(f)


def _parse_chunk_header(text: str) -> tuple[str, str, str]:
    raw_text = text or ""
    header, body = raw_text.split("\n\n", 1) if "\n\n" in raw_text else (raw_text, raw_text)
    parts = [part.strip() for part in header.split(">") if part.strip()]
    title = parts[0] if parts else header.strip()
    heading = parts[-1] if len(parts) > 1 else title
    return title, heading, body.strip()


def _reconstruct_articles_from_chunks(chunk_docs: list[dict]) -> list[dict]:
    structured_articles = _load_structured_tbank_articles()
    structured_index = {}
    for article in structured_articles:
        title = article.get("title")
        if not title:
            continue
        structured_index.setdefault(
            normalize_title(title),
            {
                "title": title,
                "url": article.get("url", ""),
            },
        )

    grouped_chunks = defaultdict(list)
    grouped_meta = {}
    for chunk in chunk_docs:
        title, heading, content = _parse_chunk_header(chunk.get("text", ""))
        if not title:
            continue
        title_key = normalize_title(title)
        canonical = structured_index.get(title_key, {"title": title, "url": ""})
        grouped_chunks[title_key].append(
            {
                "heading": heading,
                "content": content or chunk.get("text", ""),
            }
        )
        grouped_meta.setdefault(title_key, canonical)

    reconstructed = []
    for idx, (title_key, sections) in enumerate(grouped_chunks.items()):
        article_meta = grouped_meta[title_key]
        reconstructed.append(
            {
                "id": f"article_{idx}",
                "title": article_meta["title"],
                "url": article_meta["url"],
                "sections": [{"heading": item["heading"], "content": [item["content"]]} for item in sections],
            }
        )

    print(
        f"[RECONSTRUCT] built {len(reconstructed)} articles from {len(chunk_docs)} chunks"
    )
    return reconstructed if reconstructed else chunk_docs


def load_documents(data_dir="data", max_docs=None, file_pattern="*.json"):
    json_dir = Path(data_dir) if Path(data_dir).is_absolute() else PROJECT_ROOT / data_dir
    if not json_dir.exists():
        print(f"[WARN] data dir not found: {json_dir}")
        return []

    docs = []
    for path in sorted(json_dir.glob(file_pattern)):
        try:
            data = json.load(path.open(encoding="utf-8"))
        except Exception as e:
            print(f"[WARN] {path.name}: {e}")
            continue

        if isinstance(data, list):
            docs.extend(item for item in data if isinstance(item, dict))
        elif isinstance(data, dict):
            docs.append(data)

        if max_docs and len(docs) >= max_docs:
            docs = docs[:max_docs]
            break

    for i, doc in enumerate(docs):
        doc.setdefault("id", f"doc_{i}")

    print(f"[FILE] loaded {len(docs)} documents from {json_dir}")
    return docs


def load_golden_eval_set(num_queries=None, zip_path=None, use_mock=None):
    if use_mock if use_mock is not None else USE_MOCK:
        docs, queries = _mock_data(num_queries)
        stats = {
            "total_qa": len(queries),
            "matched_queries": len(queries),
            "unmatched_queries": 0,
            "used_queries": len(queries),
            "unmatched_titles": [],
        }
        return docs, queries, stats

    docs = load_documents(file_pattern="tbank_articles_clean.json")
    if not docs:
        docs = load_documents(file_pattern="tbank_articles.json")
    if _looks_like_chunk_corpus(docs):
        docs = _reconstruct_articles_from_chunks(docs)
    qa = load_qa_from_zip(Path(zip_path) if zip_path else None)
    queries, stats = attach_ground_truth(qa, docs)

    if num_queries:
        queries = queries[:num_queries]
    stats["used_queries"] = len(queries)

    print(
        f"[GOLDEN] matched {stats['matched_queries']}/{stats['total_qa']}, "
        f"eval on {stats['used_queries']}"
    )
    if stats["unmatched_queries"]:
        print(f"[GOLDEN] unmatched: {stats['unmatched_queries']}")

    return docs, queries, stats


def _mock_data(num_queries=None):
    docs = [
        {"id": "doc_1", "title": "Брокерский счет", "sections": [
            {"heading": "Что такое брокерский счет", "content": [
                "Брокерский счет позволяет торговать акциями и валютой самостоятельно.",
                "Для открытия счета нужен паспорт.",
            ]},
        ]},
        {"id": "doc_2", "title": "Индивидуальный Инвестиционный Счет (ИИС)", "sections": [
            {"heading": "Преимущества ИИС", "content": [
                "ИИС дает право на налоговый вычет 13% от взносов (тип А).",
                "Деньги нельзя снимать 3 года без потери льгот.",
            ]},
        ]},
        {"id": "doc_3", "title": "Налогообложение инвестиций", "sections": [
            {"heading": "Налог на доход", "content": [
                "Налог на доход от инвестиций составляет 13% для резидентов.",
                "Брокер выступает налоговым агентом и удерживает налог автоматически.",
            ]},
        ]},
        {"id": "doc_4", "title": "Дивиденды по акциям", "sections": [
            {"heading": "Дивидендная доходность", "content": [
                "Дивиденды - часть прибыли компании, распределяемая между акционерами.",
                "Налог на дивиденды удерживается у источника выплаты.",
            ]},
        ]},
        {"id": "doc_5", "title": "ETF и БПИФ", "sections": [
            {"heading": "Что такое ETF", "content": [
                "ETF - биржевой инвестиционный фонд.",
                "БПИФ - российский аналог ETF.",
            ]},
        ]},
        {"id": "doc_6", "title": "ОФЗ (Облигации Федерального Займа)", "sections": [
            {"heading": "Государственные облигации", "content": [
                "ОФЗ - долговые бумаги Минфина РФ.",
                "Считаются одним из самых надежных инструментов.",
            ]},
        ]},
        {"id": "doc_7", "title": "Акции и их виды", "sections": [
            {"heading": "Типы акций", "content": [
                "Обыкновенные акции дают право голоса на собрании акционеров.",
                "Привилегированные акции гарантируют фиксированный дивиденд.",
            ]},
        ]},
        {"id": "doc_8", "title": "ПИФ (Паевой Инвестиционный Фонд)", "sections": [
            {"heading": "Как работает ПИФ", "content": [
                "ПИФ - форма коллективного инвестирования.",
                "УК инвестирует средства пайщиков в различные активы.",
            ]},
        ]},
    ]

    queries = [
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
        queries = queries[:num_queries]

    print(f"[LIST] mock: {len(queries)} queries, {len(docs)} docs")
    return docs, queries
