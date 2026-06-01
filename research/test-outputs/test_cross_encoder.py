"""
Тестирование пайплайна с кросс-энкодером локально (без Docker)
Сравнивает результаты поиска: BM25+Bi-encoder vs BM25+Bi-encoder+CrossEncoder
"""

import sys
from pathlib import Path
import importlib.util
import numpy as np

# Load hybrid-retriever module directly (hyphen in name)
baseline_path = Path(__file__).parent.parent / "baseline"
spec = importlib.util.spec_from_file_location("hybrid_retriever", baseline_path / "hybrid-retriever.py")
mod = importlib.util.module_from_spec(spec)
sys.modules["hybrid_retriever"] = mod
spec.loader.exec_module(mod)

HybridRetriever = mod.HybridRetriever
CrossEncoderReranker = mod.CrossEncoderReranker
DocumentProcessor = mod.DocumentProcessor

SAMPLE_DOCS = [
    {"title": "Как продать акции", "sections": [{"heading": "Продажа акций", "content": ["Для продажи акций через брокера необходимо подать заявку в торговом терминале. Акции продаются по текущей рыночной цене или по заданной цене через лимитный ордер. Комиссия брокера составляет 0.05-0.3% от суммы сделки."]}]},
    {"title": "Что такое облигации", "sections": [{"heading": "Облигации", "content": ["Облигация — это долговая ценная бумага, по которой эмитент обязуется выплатить номинальную стоимость и купонный доход. Облигации бывают государственные (ОФЗ) и корпоративные."]}]},
    {"title": "Дивидендная стратегия", "sections": [{"heading": "Стратегия дивидендов", "content": ["Дивидендная стратегия предполагает покупку акций компаний, регулярно выплачивающих высокие дивиденды. Реинвестирование дивидендов позволяет увеличивать портфель за счёт сложного процента."]}]},
    {"title": "Как купить ETF фонд", "sections": [{"heading": "Покупка фондов", "content": ["ETF фонды покупаются на бирже через брокерский счёт. Фонды торгуются в течение всего торгового дня. Комиссия за управление в ETF обычно ниже, чем в ПИФах."]}]},
    {"title": "Налоги на инвестиции", "sections": [{"heading": "Налогообложение", "content": ["Доход от продажи акций облагается НДФЛ 13%. Брокер выступает налоговым агентом и удерживает налог автоматически. Льгота по ИИС позволяет получить налоговый вычет 13% от внесённой суммы."]}]},
    {"title": "Риск-менеджмент", "sections": [{"heading": "Управление рисками", "content": ["Диверсификация портфеля снижает риск. Рекомендуется распределять инвестиции между разными секторами и странами. Стоп-лосс помогает ограничить убытки."]}]},
    {"title": "Как открыть брокерский счёт", "sections": [{"heading": "Открытие счёта", "content": ["Брокерский счёт открывается в банке или у лицензированной брокерской компании. Необходимо предоставить паспорт и СНИЛС. Обслуживание может быть бесплатным при определённых условиях."]}]},
    {"title": "Вывод средств с брокерского счёта", "sections": [{"heading": "Вывод средств", "content": ["Для вывода средств необходимо подать заявку через терминал брокера. Вывод доступен в trading часы. Средства поступают на карту в течение 1-3 рабочих дней. При выводе автоматически удерживается НДФЛ 13%."]}]},
    {"title": "Типы ордеров", "sections": [{"heading": "Виды заявок", "content": ["Рыночный ордер исполняется по текущей цене. Лимитный ордер — по заданной цене. Стоп-ордер срабатывает при достижении определённого уровня цены."]}]},
    {"title": "Что такое IPO", "sections": [{"heading": "Первичное размещение", "content": ["IPO — первое размещение акций компании на бирже. Инвесторы могут подать заявку на участие в IPO через брокера. После начала торгов цена может значительно измениться."]}]},
]


def test_retriever_without_cross_encoder():
    print("\n" + "=" * 70)
    print("ТЕСТ 1: BM25 + Bi-encoder (БЕЗ кросс-энкодера)")
    print("=" * 70)

    retriever = HybridRetriever(
        docs=SAMPLE_DOCS,
        model_name="intfloat/multilingual-e5-small",
        cross_encoder_model="cross-encoder/mmarco-mMiniLMv2-L12-H384-v1",
        strategy="full",
        use_cross_encoder=False,
        show_debug=True,
    )
    retriever.encode_corpus(show_progress=False)

    query = "Как вывести деньги с брокерского счёта?"
    results = retriever.search(query, bm25_k=8, bi_encoder_k=5, top_k=3)

    print(f"\nЗапрос: '{query}'")
    print("Результаты:")
    for i, idx in enumerate(results, 1):
        print(f"  {i}. [{idx}] {SAMPLE_DOCS[idx]['title']}")

    return results


def test_retriever_with_cross_encoder():
    print("\n" + "=" * 70)
    print("ТЕСТ 2: BM25 + Bi-encoder + Cross-encoder")
    print("=" * 70)

    retriever = HybridRetriever(
        docs=SAMPLE_DOCS,
        model_name="intfloat/multilingual-e5-small",
        cross_encoder_model="cross-encoder/mmarco-mMiniLMv2-L12-H384-v1",
        strategy="full",
        use_cross_encoder=True,
        show_debug=True,
    )
    retriever.encode_corpus(show_progress=False)

    query = "Как вывести деньги с брокерского счёта?"
    results = retriever.search(query, bm25_k=8, bi_encoder_k=5, top_k=3)

    print(f"\nЗапрос: '{query}'")
    print("Результаты:")
    for i, idx in enumerate(results, 1):
        print(f"  {i}. [{idx}] {SAMPLE_DOCS[idx]['title']}")

    return results


def test_multiple_queries_with_comparison():
    print("\n" + "=" * 70)
    print("ТЕСТ 3: Сравнение по нескольким запросам")
    print("=" * 70)

    retriever_no_ce = HybridRetriever(
        docs=SAMPLE_DOCS,
        model_name="intfloat/multilingual-e5-small",
        cross_encoder_model="cross-encoder/mmarco-mMiniLMv2-L12-H384-v1",
        strategy="full",
        use_cross_encoder=False,
        show_debug=False,
    )
    retriever_no_ce.encode_corpus(show_progress=False)

    retriever_ce = HybridRetriever(
        docs=SAMPLE_DOCS,
        model_name="intfloat/multilingual-e5-small",
        cross_encoder_model="cross-encoder/mmarco-mMiniLMv2-L12-H384-v1",
        strategy="full",
        use_cross_encoder=True,
        show_debug=False,
    )
    retriever_ce.encode_corpus(show_progress=False)

    queries = [
        "Как продать акции?",
        "Налоги при инвестировании",
        "Как купить ETF фонд?",
    ]

    for query in queries:
        print(f"\n--- Запрос: '{query}' ---")

        no_ce = retriever_no_ce.search(query, bm25_k=8, bi_encoder_k=5, top_k=3)
        ce = retriever_ce.search(query, bm25_k=8, bi_encoder_k=5, top_k=3)

        print("  Без кросс-энкодера:")
        for i, idx in enumerate(no_ce, 1):
            print(f"    {i}. {SAMPLE_DOCS[idx]['title']}")

        print("  С кросс-энкодером:")
        for i, idx in enumerate(ce, 1):
            print(f"    {i}. {SAMPLE_DOCS[idx]['title']}")


if __name__ == "__main__":
    test_retriever_without_cross_encoder()
    test_retriever_with_cross_encoder()
    test_multiple_queries_with_comparison()
    print("\n" + "=" * 70)
    print("Все тесты завершены")
    print("=" * 70)