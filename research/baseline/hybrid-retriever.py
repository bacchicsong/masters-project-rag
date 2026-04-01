"""
Гибридный ретривер: BM25 → Bi-encoder
Система поиска с двухэтапным ранжированием
"""

import json
import time
import tracemalloc
from functools import wraps
from typing import List, Dict, Callable, Optional
import numpy as np
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer, CrossEncoder
import os


# ============================================================================
# ПАРАМЕТРЫ КОНФИГУРАЦИИ
# ============================================================================

CONFIG = {
    # Пути к данным
    'data_folder': '/content/drive/MyDrive/tbank_knowledge_2/',
    'file_indices_start': 1,
    'file_indices_end': 100,
    
    # Параметры модели
    'model_name': 'intfloat/multilingual-e5-small',
    'cross_encoder_model': 'cross-encoder/mmarco-mMiniLMv2-L12-H384-v1',
    'strategy': 'full',  # 'full', 'title_headings', 'title_only'
    
    # Параметры поиска
    'bm25_k': 50,  # Количество кандидатов после BM25
    'bi_encoder_k': 20,  # Кандидаты после bi-encoder для cross-encoder
    'top_k': 10,   # Финальное количество результатов
    
    # Параметры кросс-энкодера
    'use_cross_encoder': True,
    
    # Параметры оценки
    'eval_ks': [1, 3, 5, 8],  # K для метрик P@k, R@k
    
    # Показывать прогресс
    'show_progress': True,
    'show_debug': True,
}


# ============================================================================
# УТИЛИТЫ И ДЕКОРАТОРЫ
# ============================================================================

class Utils:
    """Вспомогательные функции и декораторы"""
    
    @staticmethod
    def measure_performance(func):
        """Кастомный декоратор: время + память"""
        @wraps(func)
        def wrapper(*args, **kwargs):
            tracemalloc.start()
            start = time.perf_counter()
            
            result = func(*args, **kwargs)
            
            elapsed = time.perf_counter() - start
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            print(f"⏱ [{func.__name__}] Time: {elapsed:.3f}s | "
                  f"Mem: {current/1024**2:.1f}MB (peak: {peak/1024**2:.1f}MB)")
            return result
        return wrapper


# ============================================================================
# ОБРАБОТКА ДОКУМЕНТОВ
# ============================================================================
class DocumentProcessor:
    """Класс для обработки и извлечения текста из документов"""
    
    # Стратегии преобразования документа → текст
    STRATEGIES = {
        'full': lambda self, d: self.extract_text(d, 'full'),
        'title_headings': lambda self, d: self.extract_text(d, 'title_headings'),
        'title_only': lambda self, d: self.extract_text(d, 'title_only'),
    }
    
    def extract_text(self, doc: dict, mode: str = 'full') -> str:
        """
        Извлечение текста из документа.
        
        Структура документа:
        {
            "url": "...",
            "title": "Заголовок страницы",
            "content": "...",  # ИГНОРИРУЕТСЯ (навигация сайта)
            "sections": [
                {
                    "heading": "Подзаголовок",
                    "content": ["параграф 1", "параграф 2", ...]
                }
            ]
        }
        
        Args:
            doc: документ
            mode: режим извлечения ('full', 'title_headings', 'title_only')
        """
        parts = []
        
        # Всегда добавляем title
        title = doc.get('title', '')
        if title:
            parts.append(title)
        
        # Обрабатываем sections в зависимости от режима
        if mode == 'title_only':
            # Только заголовок страницы
            pass
            
        elif mode == 'title_headings':
            # Заголовок страницы + заголовки секций
            for section in doc.get('sections', []):
                heading = section.get('heading', '')
                if heading:
                    parts.append(heading)
                    
        elif mode == 'full':
            # Всё: заголовок + секции с содержимым
            for section in doc.get('sections', []):
                heading = section.get('heading', '')
                if heading:
                    parts.append(heading)
                
                content = section.get('content', [])
                if isinstance(content, list):
                    parts.extend(content)
                else:
                    parts.append(str(content))
        
        # Фильтруем пустые строки и объединяем
        return ' '.join(filter(None, parts))
    
    @staticmethod
    def get_doc_title(doc: dict) -> str:
        """Возвращает заголовок документа"""
        return doc.get('title', 'Без заголовка')
    
    @staticmethod
    def get_doc_url(doc: dict) -> str:
        """Возвращает URL документа"""
        return doc.get('url', '')
    
    def get_text_by_strategy(self, doc: dict, strategy: str) -> str:
        """Получить текст документа согласно выбранной стратегии"""
        if strategy not in self.STRATEGIES:
            raise ValueError(f"Unknown strategy: {strategy}. "
                           f"Available: {list(self.STRATEGIES.keys())}")
        return self.STRATEGIES[strategy](self, doc)


# ============================================================================
# ЗАГРУЗЧИК ДАННЫХ
# ============================================================================

class DataLoader:
    """Класс для загрузки документов из файлов"""
    
    @staticmethod
    def load_documents(folder_path: str, 
                      start_idx: int = 1, 
                      end_idx: int = 100,
                      show_debug: bool = True) -> List[dict]:
        """
        Загружает документы из JSON файлов
        
        Args:
            folder_path: путь к папке с файлами
            start_idx: начальный индекс файлов
            end_idx: конечный индекс файлов
            show_debug: показывать отладочную информацию
        """
        needed_indices = np.arange(start_idx, end_idx)
        
        # Получаем все файлы из папки
        all_files = sorted([f for f in os.listdir(folder_path) 
                          if f.endswith('.json')])
        
        # Выбираем файлы по индексам
        selected_files = [all_files[i] for i in needed_indices 
                         if i < len(all_files)]
        
        # Список для всех данных
        documents = []
        
        # Читаем выбранные файлы и объединяем
        for filename in selected_files:
            file_path = os.path.join(folder_path, filename)
            
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Если данные - список, добавляем все элементы
                if isinstance(data, list):
                    documents.extend(data)
                    if show_debug:
                        print(f"Прочитан {filename}: добавлено {len(data)} записей")
                else:
                    # Если это один объект, добавляем его
                    documents.append(data)
                    if show_debug:
                        print(f"Прочитан {filename}: добавлена 1 запись")
        
        if show_debug:
            print(f"\n📄 Всего загружено документов: {len(documents)}")
        
        return documents


# ============================================================================
# КРОСС-ЭНКОДЕР РЕРАНКЕР
# ============================================================================

class CrossEncoderReranker:
    """Кросс-энкодер для переранжирования кандидатов"""
    
    def __init__(self, model_name: str = 'cross-encoder/mmarco-mMiniLMv2-L12-H384-v1'):
        self.model = CrossEncoder(model_name)
    
    @Utils.measure_performance
    def rerank(self, query: str, candidate_texts: List[str], top_k: int) -> List[int]:
        """
        Переранжирование кандидатов через кросс-энкодер
        
        Args:
            query: запрос
            candidate_texts: тексты кандидатов
            top_k: количество лучших результатов
            
        Returns:
            индексы лучших кандидатов в порядке убывания релевантности
        """
        if not candidate_texts:
            return []
        
        pairs = [[query, text] for text in candidate_texts]
        scores = self.model.predict(pairs)
        
        ranked_indices = np.argsort(scores)[::-1][:top_k]
        return ranked_indices.tolist()


# ============================================================================
# ГИБРИДНЫЙ РЕТРИВЕР
# ============================================================================

class HybridRetriever:
    """Гибридный поисковый движок: BM25 → Bi-encoder → Cross-encoder"""
    
    def __init__(self,
                 docs: List[dict],
                 model_name: str = 'intfloat/multilingual-e5-small',
                 cross_encoder_model: str = 'cross-encoder/mmarco-mMiniLMv2-L12-H384-v1',
                 strategy: str = 'full',
                 use_cross_encoder: bool = False,
                 show_debug: bool = True):
        """
        Args:
            docs: список документов
            model_name: название модели для эмбеддингов
            cross_encoder_model: модель кросс-энкодера
            strategy: стратегия извлечения текста
            use_cross_encoder: использовать ли cross-encoder
            show_debug: показывать отладочную информацию
        """
        self.docs = docs
        self.doc_processor = DocumentProcessor()
        self.strategy = strategy
        self.use_cross_encoder = use_cross_encoder
        self.show_debug = show_debug
        
        # Извлекаем тексты из документов
        self.texts = [self.doc_processor.get_text_by_strategy(d, strategy) 
                     for d in docs]
        
        if self.show_debug:
            self._print_debug_info()
        
        # Инициализация BM25
        tokenized = [t.lower().split() for t in self.texts]
        self.bm25 = BM25Okapi(tokenized)
        
        # Инициализация Bi-encoder
        self.model = SentenceTransformer(model_name)
        self.embeddings = None
        
        # Инициализация Cross-encoder
        self.cross_encoder = CrossEncoderReranker(cross_encoder_model) if use_cross_encoder else None
    
    def _print_debug_info(self):
        """Печать отладочной информации"""
        print(f"📄 Загружено документов: {len(self.docs)}")
        print(f"📝 Стратегия: {self.strategy}")
        print(f"🔄 Cross-encoder: {'включен' if self.use_cross_encoder else 'выключен'}")
    
        # Показываем примеры документов
        for i, (doc, text) in enumerate(zip(self.docs[:3], self.texts[:3])):
            title = self.doc_processor.get_doc_title(doc)
            url = self.doc_processor.get_doc_url(doc)
            preview = text[:100] + "..." if len(text) > 100 else text
            
            print(f"\n   [{i}] {title}")
            if url:
                print(f"       URL: {url}")
            print(f"       Text: {preview}")
    
    @Utils.measure_performance
    def encode_corpus(self, show_progress: bool = True):
        """Создание эмбеддингов для всего корпуса"""
        self.embeddings = self.model.encode(
            self.texts,
            show_progress_bar=show_progress,
            convert_to_numpy=True,
            normalize_embeddings=True
        )
    
    @Utils.measure_performance
    def search(self, 
               query: str, 
               bm25_k: int = 50,
               bi_encoder_k: int = 20,
               top_k: int = 10) -> List[int]:
        """
        Трёхэтапный поиск: BM25 → Bi-encoder → Cross-encoder
        
        Args:
            query: поисковый запрос
            bm25_k: количество кандидатов после BM25
            bi_encoder_k: количество кандидатов после bi-encoder (для cross-encoder)
            top_k: финальное количество результатов
            
        Returns:
            список индексов найденных документов
        """
        bm25_k = min(bm25_k, len(self.docs))
        
        # Stage 1: BM25
        scores_bm25 = self.bm25.get_scores(query.lower().split())
        candidates = np.argsort(scores_bm25)[::-1][:bm25_k]
        
        # Stage 2: Bi-encoder rerank
        q_emb = self.model.encode([query], normalize_embeddings=True)
        scores = self.embeddings[candidates] @ q_emb.T
        reranked = candidates[np.argsort(scores.flatten())[::-1]]
        
        if self.use_cross_encoder and self.cross_encoder:
            # Stage 3: Cross-encoder rerank
            bi_encoder_k = min(bi_encoder_k, len(reranked))
            top_candidates = reranked[:bi_encoder_k]
            candidate_texts = [self.texts[idx] for idx in top_candidates]
            
            cross_top_k = min(top_k, len(candidate_texts))
            ce_ranked = self.cross_encoder.rerank(query, candidate_texts, cross_top_k)
            
            final_results = [top_candidates[i] for i in ce_ranked]
        else:
            final_results = reranked[:top_k].tolist()
        
        return final_results
    
    def get_doc_title(self, idx: int) -> str:
        """Получить заголовок документа по индексу"""
        return DocumentProcessor.get_doc_title(self.docs[idx])


# ============================================================================
# МЕТРИКИ И ОЦЕНКА
# ============================================================================

class MetricsEvaluator:
    """Класс для вычисления метрик качества поиска"""
    
    @staticmethod
    def compute_metrics(retrieved: List[int], 
                       relevant: set, 
                       ks: List[int]) -> dict:
        """
        Вычисляет Precision@k и Recall@k
        
        Args:
            retrieved: список индексов найденных документов
            relevant: множество индексов релевантных документов
            ks: список значений k для вычисления метрик
        """
        results = {}
        for k in ks:
            hits = len(set(retrieved[:k]) & relevant)
            results[f'P@{k}'] = hits / k if k > 0 else 0
            results[f'R@{k}'] = hits / len(relevant) if relevant else 0
        return results
    
    @staticmethod
    def evaluate(retriever: HybridRetriever, 
                test_data: List[dict], 
                ks: List[int] = [1, 3, 5, 8]) -> dict:
        """
        Оценка качества на тестовых данных
        
        Args:
            retriever: экземпляр ретривера
            test_data: список тестовых запросов с релевантными документами
                      [{"query": "...", "relevant_ids": [...]}, ...]
            ks: список значений k для метрик
        """
        all_metrics = {f'{m}@{k}': [] for m in ['P', 'R'] for k in ks}
        
        for item in test_data:
            retrieved = retriever.search(item['query'])
            relevant = set(item['relevant_ids'])
            
            for k in ks:
                hits = len(set(retrieved[:k]) & relevant)
                all_metrics[f'P@{k}'].append(hits / k if k > 0 else 0)
                all_metrics[f'R@{k}'].append(
                    hits / len(relevant) if relevant else 0
                )
        
        return {m: np.mean(v) for m, v in all_metrics.items()}


# ============================================================================
# ЗАПУСК ЭКСПЕРИМЕНТОВ
# ============================================================================

class ExperimentRunner:
    """Класс для запуска экспериментов и тестирования"""
    
    def __init__(self, config: dict):
        self.config = config
        self.retriever = None
        self.documents = None
    
    def load_data(self):
        """Загрузка данных"""
        print("=" * 70)
        print("ЗАГРУЗКА ДАННЫХ")
        print("=" * 70)
        
        self.documents = DataLoader.load_documents(
            folder_path=self.config['data_folder'],
            start_idx=self.config['file_indices_start'],
            end_idx=self.config['file_indices_end'],
            show_debug=self.config['show_debug']
        )
    
    def initialize_retriever(self):
        """Инициализация ретривера"""
        print("\n" + "=" * 70)
        print("ИНИЦИАЛИЗАЦИЯ РЕТРИВЕРА")
        print("=" * 70)
        print(f"Модель: {self.config['model_name']}")
        print(f"Стратегия: {self.config['strategy']}")
        print(f"BM25_k: {self.config['bm25_k']}")
        print(f"Top_k: {self.config['top_k']}\n")
        
        self.retriever = HybridRetriever(
            docs=self.documents,
            model_name=self.config['model_name'],
            strategy=self.config['strategy'],
            show_debug=self.config['show_debug']
        )
        
        self.retriever.encode_corpus(
            show_progress=self.config['show_progress']
        )
    
    def test_single_query(self, query: str, top_n: int = 5):
        """Тестирование одного запроса"""
        print("\n" + "=" * 70)
        print(f"🔍 Query: '{query}'")
        print("=" * 70)
        
        results = self.retriever.search(
            query,
            bm25_k=self.config['bm25_k'],
            top_k=self.config['top_k']
        )
        
        for i, idx in enumerate(results[:top_n], 1):
            title = self.retriever.get_doc_title(idx)
            print(f"{i}. [{idx}] {title}")
    
    def test_multiple_queries(self, queries: List[str], top_n: int = 3):
        """Тестирование нескольких запросов"""
        print("\n" + "=" * 70)
        print("🧪 ТЕСТОВЫЕ ЗАПРОСЫ")
        print("=" * 70)
        
        for q in queries:
            results = self.retriever.search(
                q,
                bm25_k=self.config['bm25_k'],
                top_k=top_n
            )
            print(f"\n❓ {q}")
            for i, idx in enumerate(results, 1):
                print(f"   {i}. {self.retriever.get_doc_title(idx)}")
    
    def evaluate_test_data(self, test_data: List[dict]):
        """Оценка на тестовых данных с метриками"""
        print("\n" + "=" * 70)
        print("📊 ОЦЕНКА КАЧЕСТВА")
        print("=" * 70)
        
        metrics = MetricsEvaluator.evaluate(
            self.retriever,
            test_data,
            ks=self.config['eval_ks']
        )
        
        print("-" * 30)
        for k in self.config['eval_ks']:
            print(f"k={k}: Precision={metrics[f'P@{k}']:.3f} | "
                  f"Recall={metrics[f'R@{k}']:.3f}")


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Главная функция"""
    # Инициализация эксперимента
    runner = ExperimentRunner(CONFIG)

    # Загрузка данных
    runner.load_data()

    # Инициализация ретривера
    runner.initialize_retriever()

    # Тестовый запрос
    test_query = "Как перекинуть деньги со счёта у брокера на мой накопительный счёт?"
    runner.test_single_query(test_query, top_n=5)

    # Дополнительные тестовые запросы
    test_queries = [
        "Как оценить финансовое состояние компании?",
        "Что такое дивидендная стратегия инвестирования?",
        "Как работает реинвестирование дивидендов?",
        "Что такое суверенные и корпоративные облигации?",
        "Как выбрать надежного брокера?",
        "Что такое ликвидность ценных бумаг?",
        "Как рассчитать доходность инвестиций?",
        "Что такое комиссия за обслуживание брокерского счета?",
        "Как работают голубые фишки и акции роста?",
        "Что такое дробление акций (сплит)?",
    ]

    runner.test_multiple_queries(test_queries, top_n=3)


if __name__ == "__main__":
    main()
