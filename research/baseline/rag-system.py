"""
RAG система: Hybrid Retriever + Qwen Generator
Система вопрос-ответ с использованием поиска и генерации
"""

import torch
from transformers import AutoModelForCausalLM, AutoTokenizer
from typing import List, Dict, Optional, Tuple
import json
from dataclasses import dataclass
import time

# Импорт из предыдущего файла
from hybrid_retriever import (
    HybridRetriever, 
    DataLoader, 
    DocumentProcessor,
    CONFIG as RETRIEVER_CONFIG
)


# ============================================================================
# ПАРАМЕТРЫ КОНФИГУРАЦИИ
# ============================================================================

CONFIG = {
    # Параметры генератора
    'generator_model': 'Qwen/Qwen2.5-1.5B',
    'device': 'cuda' if torch.cuda.is_available() else 'cpu',
    'torch_dtype': torch.float16 if torch.cuda.is_available() else torch.float32,
    
    # Параметры генерации
    'max_new_tokens': 512,
    'temperature': 0.7,
    'top_p': 0.9,
    'top_k': 50,
    'do_sample': True,
    'repetition_penalty': 1.1,
    
    # Параметры контекста
    'max_context_docs': 1,  # Максимальное количество документов в контексте
    'max_context_length': 512,  # Максимальная длина контекста в токенах
    
    # Параметры ретривера (наследуются из RETRIEVER_CONFIG)
    'retriever': {
        'model_name': RETRIEVER_CONFIG['model_name'],
        'cross_encoder_model': RETRIEVER_CONFIG['cross_encoder_model'],
        'strategy': RETRIEVER_CONFIG['strategy'],
        'use_cross_encoder': RETRIEVER_CONFIG['use_cross_encoder'],
        'bm25_k': RETRIEVER_CONFIG['bm25_k'],
        'bi_encoder_k': RETRIEVER_CONFIG['bi_encoder_k'],
        'top_k': RETRIEVER_CONFIG['top_k'],
    },
    
    # Параметры данных
    'data_folder': RETRIEVER_CONFIG['data_folder'],
    'file_indices_start': RETRIEVER_CONFIG['file_indices_start'],
    'file_indices_end': RETRIEVER_CONFIG['file_indices_end'],
    
    # Параметры промпта
    'prompt_template': 'default',  # 'default', 'concise', 'detailed'
    'language': 'ru',
    
    # Отладка
    'show_debug': True,
    'show_retrieved_docs': True,
    'show_generation_time': True,
}


# ============================================================================
# ШАБЛОНЫ ПРОМПТОВ
# ============================================================================

@dataclass
class PromptTemplate:
    """Шаблон промпта для генерации"""
    system: str
    user: str
    
    def format(self, context: str, question: str) -> str:
        """Форматирование промпта"""
        raise NotImplementedError


class DefaultPromptTemplate(PromptTemplate):
    """Стандартный шаблон промпта"""
    
    def __init__(self):
        self.system = """Ты - полезный AI-ассистент, который отвечает на вопросы на основе предоставленного контекста.

Правила:
1. Используй только информацию из контекста для ответа
2. Если в контексте нет информации для ответа, честно скажи об этом
3. Отвечай кратко и по существу
4. Используй русский язык"""

        self.user = """Контекст:
{context}

Вопрос: {question}

Ответ:"""
    
    def format(self, context: str, question: str) -> str:
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.user.format(
                context=context, 
                question=question
            )}
        ]
        return messages


class ConcisePromptTemplate(PromptTemplate):
    """Краткий шаблон промпта"""
    
    def __init__(self):
        self.system = "Ты - AI-ассистент. Отвечай кратко на основе контекста."
        self.user = "Контекст:\n{context}\n\nВопрос: {question}\n\nКраткий ответ:"
    
    def format(self, context: str, question: str) -> str:
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.user.format(
                context=context,
                question=question
            )}
        ]
        return messages


class DetailedPromptTemplate(PromptTemplate):
    """Детальный шаблон промпта"""
    
    def __init__(self):
        self.system = """Ты - экспертный AI-ассистент в области финансов и инвестиций.

Твоя задача:
1. Тщательно проанализировать предоставленный контекст
2. Дать подробный, структурированный ответ на вопрос
3. Привести примеры, если они есть в контексте
4. Если информации недостаточно, указать это и предложить, что ещё может быть полезно узнать

Стиль ответа: профессиональный, но понятный для начинающего инвестора."""

        self.user = """📚 КОНТЕКСТ:
{context}

❓ ВОПРОС: {question}

💡 ПОДРОБНЫЙ ОТВЕТ:"""
    
    def format(self, context: str, question: str) -> str:
        messages = [
            {"role": "system", "content": self.system},
            {"role": "user", "content": self.user.format(
                context=context,
                question=question
            )}
        ]
        return messages


# Регистр шаблонов
PROMPT_TEMPLATES = {
    'default': DefaultPromptTemplate,
    'concise': ConcisePromptTemplate,
    'detailed': DetailedPromptTemplate,
}


# ============================================================================
# ПОСТРОИТЕЛЬ ПРОМПТОВ
# ============================================================================

class PromptBuilder:
    """Класс для построения промптов из контекста и вопроса"""
    
    def __init__(self, template_name: str = 'default'):
        if template_name not in PROMPT_TEMPLATES:
            raise ValueError(f"Unknown template: {template_name}. "
                           f"Available: {list(PROMPT_TEMPLATES.keys())}")
        self.template = PROMPT_TEMPLATES[template_name]()
    
    def build_context(self, 
                 documents: List[dict], 
                 indices: List[int],
                 max_docs: int = 5,
                 include_url: bool = True) -> str:
        """
        Построение контекста из найденных документов
        
        Args:
            documents: список всех документов
            indices: индексы найденных документов
            max_docs: максимальное количество документов
            include_url: включать URL источника
        """
        context_parts = []
        doc_processor = DocumentProcessor()
        
        for i, idx in enumerate(indices[:max_docs], 1):
            doc = documents[idx]
            title = doc_processor.get_doc_title(doc)
            text = doc_processor.extract_text(doc, mode='full')
            
            # Формируем контекст для документа
            doc_context = f"[Документ {i}] {title}\n{text}"
            
            # Опционально добавляем URL
            if include_url:
                url = doc_processor.get_doc_url(doc)
                if url:
                    doc_context = f"[Документ {i}] {title}\nИсточник: {url}\n{text}"
            
            context_parts.append(doc_context)
        
        return "\n\n".join(context_parts)
    
    def build_prompt(self, context: str, question: str) -> List[dict]:
        """Построение финального промпта"""
        return self.template.format(context, question)


# ============================================================================
# ГЕНЕРАТОР
# ============================================================================

class Generator:
    """Класс для работы с языковой моделью"""
    
    def __init__(self, 
                 model_name: str,
                 device: str = 'cuda',
                 torch_dtype = torch.float16,
                 show_debug: bool = True):
        """
        Args:
            model_name: название модели
            device: устройство для вычислений
            torch_dtype: тип данных для модели
            show_debug: показывать отладочную информацию
        """
        self.model_name = model_name
        self.device = device
        self.torch_dtype = torch_dtype
        self.show_debug = show_debug
        
        if self.show_debug:
            print(f"🤖 Загрузка модели: {model_name}")
            print(f"   Device: {device}")
            print(f"   Dtype: {torch_dtype}")
        
        # Загрузка токенизатора и модели
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_name,
            trust_remote_code=True
        )
        
        self.model = AutoModelForCausalLM.from_pretrained(
            model_name,
            torch_dtype=torch_dtype,
            device_map=device if device == 'cuda' else None,
            trust_remote_code=True
        )
        
        if device == 'cpu':
            self.model = self.model.to(device)
        
        self.model.eval()
        
        if self.show_debug:
            print(f"✅ Модель загружена успешно\n")
    
    def generate(self,
                messages: List[dict],
                max_new_tokens: int = 512,
                temperature: float = 0.3,
                top_p: float = 0.9,
                top_k: int = 3,
                do_sample: bool = True,
                repetition_penalty: float = 1.1,
                **kwargs) -> str:
        """
        Генерация ответа
        
        Args:
            messages: список сообщений в формате chat
            max_new_tokens: максимальное количество новых токенов
            temperature: температура генерации
            top_p: nucleus sampling параметр
            top_k: top-k sampling параметр
            do_sample: использовать sampling
            repetition_penalty: штраф за повторения
        """
        # Применяем chat template
        text = self.tokenizer.apply_chat_template(
            messages,
            tokenize=False,
            add_generation_prompt=True
        )
        
        # Токенизация
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            max_length=2048
        ).to(self.device)
        
        # Генерация
        with torch.no_grad():
            outputs = self.model.generate(
                **inputs,
                max_new_tokens=max_new_tokens,
                temperature=temperature,
                top_p=top_p,
                top_k=top_k,
                do_sample=do_sample,
                repetition_penalty=repetition_penalty,
                pad_token_id=self.tokenizer.pad_token_id,
                eos_token_id=self.tokenizer.eos_token_id,
                **kwargs
            )
        
        # Декодирование
        generated_ids = outputs[0][inputs['input_ids'].shape[1]:]
        response = self.tokenizer.decode(
            generated_ids,
            skip_special_tokens=True
        )
        
        return response.strip()


# ============================================================================
# RAG СИСТЕМА
# ============================================================================

class RAGSystem:
    """Полная RAG система: Retriever + Generator"""
    
    def __init__(self, config: dict):
        """
        Args:
            config: конфигурация системы
        """
        self.config = config
        self.documents = None
        self.retriever = None
        self.generator = None
        self.prompt_builder = None
        
    def initialize(self):
        """Инициализация всех компонентов"""
        print("=" * 70)
        print("ИНИЦИАЛИЗАЦИЯ RAG СИСТЕМЫ")
        print("=" * 70)
        
        # 1. Загрузка данных
        print("\n[1/3] Загрузка документов...")
        self.documents = DataLoader.load_documents(
            folder_path=self.config['data_folder'],
            start_idx=self.config['file_indices_start'],
            end_idx=self.config['file_indices_end'],
            show_debug=self.config['show_debug']
        )
        
        # 2. Инициализация ретривера
        print("\n[2/3] Инициализация ретривера...")
        self.retriever = HybridRetriever(
            docs=self.documents,
            model_name=self.config['retriever']['model_name'],
            cross_encoder_model=self.config['retriever']['cross_encoder_model'],
            strategy=self.config['retriever']['strategy'],
            use_cross_encoder=self.config['retriever']['use_cross_encoder'],
            show_debug=self.config['show_debug']
        )
        self.retriever.encode_corpus(show_progress=True)
        
        # 3. Инициализация генератора
        print("\n[3/3] Инициализация генератора...")
        self.generator = Generator(
            model_name=self.config['generator_model'],
            device=self.config['device'],
            torch_dtype=self.config['torch_dtype'],
            show_debug=self.config['show_debug']
        )
        
        # 4. Инициализация prompt builder
        self.prompt_builder = PromptBuilder(
            template_name=self.config['prompt_template']
        )
        
        print("=" * 70)
        print("✅ RAG СИСТЕМА ГОТОВА К РАБОТЕ")
        print("=" * 70)
    
    def answer(self, 
          question: str,
          return_context: bool = False,
          return_retrieved_ids: bool = False) -> Dict:
        """
        Ответ на вопрос
        
        Args:
            question: вопрос пользователя
            return_context: вернуть контекст
            return_retrieved_ids: вернуть ID найденных документов
            
        Returns:
            словарь с ответом и дополнительной информацией
        """
        start_time = time.time()
        
        # 1. Поиск релевантных документов
        if self.config['show_debug']:
            print(f"\n🔍 Поиск документов для вопроса: '{question}'")
        
        retrieval_start = time.time()
        retrieved_ids = self.retriever.search(
            question,
            bm25_k=self.config['retriever']['bm25_k'],
            top_k=self.config['retriever']['top_k']
        )
        retrieval_time = time.time() - retrieval_start
        
        if self.config['show_retrieved_docs']:
            print(f"\n📚 Найдено документов: {len(retrieved_ids)}")
            doc_processor = DocumentProcessor()
            for i, idx in enumerate(retrieved_ids[:self.config['max_context_docs']], 1):
                doc = self.documents[idx]
                title = doc_processor.get_doc_title(doc)
                url = doc_processor.get_doc_url(doc)
                print(f"   {i}. [{idx}] {title}")
                if url:
                    print(f"       → {url}")
        
        # 2. Построение контекста
        context = self.prompt_builder.build_context(
            documents=self.documents,
            indices=retrieved_ids,
            max_docs=self.config['max_context_docs']
        )
        
        # 3. Построение промпта
        messages = self.prompt_builder.build_prompt(context, question)
        print(messages)
        # 4. Генерация ответа
        if self.config['show_debug']:
            print(f"\n🤖 Генерация ответа...")
        
        generation_start = time.time()
        answer = self.generator.generate(
            messages=messages,
            max_new_tokens=self.config['max_new_tokens'],
            temperature=self.config['temperature'],
            top_p=self.config['top_p'],
            top_k=self.config['top_k'],
            do_sample=self.config['do_sample'],
            repetition_penalty=self.config['repetition_penalty']
        )
        generation_time = time.time() - generation_start
        
        total_time = time.time() - start_time
        
        if self.config['show_generation_time']:
            print(f"\n⏱️  Время выполнения:")
            print(f"   Поиск: {retrieval_time:.3f}s")
            print(f"   Генерация: {generation_time:.3f}s")
            print(f"   Всего: {total_time:.3f}s")
        
        # Формирование результата
        result = {
            'question': question,
            'answer': answer,
            'retrieval_time': retrieval_time,
            'generation_time': generation_time,
            'total_time': total_time,
        }
        
        if return_context:
            result['context'] = context
        
        if return_retrieved_ids:
            result['retrieved_ids'] = retrieved_ids
            doc_processor = DocumentProcessor()
            result['retrieved_docs'] = [
                {
                    'id': idx,
                    'title': doc_processor.get_doc_title(self.documents[idx]),
                    'url': doc_processor.get_doc_url(self.documents[idx])
                }
                for idx in retrieved_ids[:self.config['max_context_docs']]
            ]
        
        return result


# ============================================================================
# ЗАПУСК ЭКСПЕРИМЕНТОВ
# ============================================================================

class ExperimentRunner:
    """Класс для запуска экспериментов с RAG системой"""
    
    def __init__(self, config: dict):
        self.config = config
        self.rag_system = RAGSystem(config)
    
    def run(self):
        """Запуск полного эксперимента"""
        # Инициализация
        self.rag_system.initialize()
        
        # Тестовые вопросы
        test_questions = [
            "Что такое акция?",
        ]
        
        # Запуск тестов
        self.test_single_question(test_questions[0])
        #self.test_multiple_questions(test_questions[1:])
        
    def test_single_question(self, question: str):
        """Тест одного вопроса с подробным выводом"""
        print("\n" + "=" * 70)
        print("ТЕСТ: ОДИН ВОПРОС (ПОДРОБНЫЙ ВЫВОД)")
        print("=" * 70)
        
        result = self.rag_system.answer(
            question,
            return_context=True,
            return_retrieved_ids=True
        )
        
        print(f"\n❓ ВОПРОС:\n{result['question']}")
        print(f"\n💡 ОТВЕТ:\n{result['answer']}")
        
        if 'retrieved_docs' in result:
            print(f"\n📚 ИСПОЛЬЗОВАННЫЕ ДОКУМЕНТЫ:")
            for doc in result['retrieved_docs']:
                print(f"   • [{doc['id']}] {doc['title']}")
        
        print(f"\n⏱️  ВРЕМЯ: {result['total_time']:.3f}s")
    
    def test_multiple_questions(self, questions: List[str]):
        """Тест нескольких вопросов"""
        print("\n" + "=" * 70)
        print("ТЕСТ: НЕСКОЛЬКО ВОПРОСОВ")
        print("=" * 70)
        
        results = self.rag_system.batch_answer(questions)
        
        # Сводка результатов
        print("\n" + "=" * 70)
        print("СВОДКА РЕЗУЛЬТАТОВ")
        print("=" * 70)
        
        avg_retrieval_time = sum(r['retrieval_time'] for r in results) / len(results)
        avg_generation_time = sum(r['generation_time'] for r in results) / len(results)
        avg_total_time = sum(r['total_time'] for r in results) / len(results)
        
        print(f"\nОбработано вопросов: {len(results)}")
        print(f"Среднее время поиска: {avg_retrieval_time:.3f}s")
        print(f"Среднее время генерации: {avg_generation_time:.3f}s")
        print(f"Среднее общее время: {avg_total_time:.3f}s")
        
        # Вывод кратких результатов
        print("\n" + "-" * 70)
        for i, result in enumerate(results, 1):
            print(f"\n{i}. {result['question']}")
            answer_preview = result['answer'][:150] + "..." if len(result['answer']) > 150 else result['answer']
            print(f"   → {answer_preview}")
    
    def interactive_mode(self):
        """Интерактивный режим"""
        print("\n" + "=" * 70)
        print("ИНТЕРАКТИВНЫЙ РЕЖИМ")
        print("=" * 70)
        print("Введите 'exit' или 'quit' для выхода\n")
        
        while True:
            question = input("❓ Ваш вопрос: ").strip()
            
            if question.lower() in ['exit', 'quit', 'выход']:
                print("👋 До свидания!")
                break
            
            if not question:
                continue
            
            result = self.rag_system.answer(question)
            print(f"\n💡 ОТВЕТ:\n{result['answer']}\n")
            print("-" * 70)


# ============================================================================
# УТИЛИТЫ
# ============================================================================

def save_results(results: List[Dict], filename: str = 'rag_results.json'):
    """Сохранение результатов в JSON"""
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"💾 Результаты сохранены в {filename}")


def print_config(config: dict):
    """Красивый вывод конфигурации"""
    print("\n" + "=" * 70)
    print("КОНФИГУРАЦИЯ СИСТЕМЫ")
    print("=" * 70)
    
    print(f"\n🤖 Генератор:")
    print(f"   Модель: {config['generator_model']}")
    print(f"   Device: {config['device']}")
    print(f"   Max tokens: {config['max_new_tokens']}")
    print(f"   Temperature: {config['temperature']}")
    
    print(f"\n🔍 Ретривер:")
    print(f"   Модель: {config['retriever']['model_name']}")
    print(f"   Стратегия: {config['retriever']['strategy']}")
    print(f"   BM25 k: {config['retriever']['bm25_k']}")
    print(f"   Top k: {config['retriever']['top_k']}")
    
    print(f"\n📝 Промпт:")
    print(f"   Шаблон: {config['prompt_template']}")
    print(f"   Макс. документов: {config['max_context_docs']}")
    
    print(f"\n📚 Данные:")
    print(f"   Папка: {config['data_folder']}")
    print(f"   Файлы: {config['file_indices_start']}-{config['file_indices_end']}")
    print("=" * 70)


# ============================================================================
# MAIN
# ============================================================================

def main():
    """Главная функция"""
    
    # Вывод конфигурации
    print_config(CONFIG)
    
    # Создание и запуск эксперимента
    runner = ExperimentRunner(CONFIG)
    runner.run()
    
    # Раскомментируйте для интерактивного режима
    # runner.interactive_mode()


if __name__ == "__main__":
    main()