import asyncio
import sys
import os
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

# --- НАСТРОЙКА ПУТЕЙ ---
# Добавляем папку src, чтобы Python видел ваш проект
sys.path.append(os.path.join(os.getcwd(), 'src'))

# Импортируем ваши классы
from src.domain.query.usecase.query_usecase import QueryUsecase
from src.domain.query.query import Query
from src.infrastructure.db.qdrand import get_embedded_model

# --- ЗАГЛУШКИ (Mocks) ---
class MockConfig:
    QDRANT_COLLECTIONS_NAME = "test_collection_local"

class MockLogger:
    def info(self, message): print(f"[INFO] {message}")
    def error(self, message): print(f"[ERROR] {message}")

# --- ПОДГОТОВКА ДАННЫХ ---
async def setup_qdrant_data(client: QdrantClient, collection_name: str):
    print("--- 1. Создание встроенной базы данных (в памяти) ---")
    
    model = get_embedded_model()
    
    # В локальном режиме (in-memory) коллекция каждый раз создается с нуля
    if not client.collection_exists(collection_name):
        # Определяем размер вектора (обычно 384 или 768)
        sample_vec = model.encode("test")
        dim = len(sample_vec)
        
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

        # Тестовые данные (имитируем ваши документы)
        docs = [
            {"text": "Брокерский счет нужен для покупки акций и валюты.", "title": "Брокерский счет", "url": "docs/broker"},
            {"text": "ИИС позволяет вернуть 13% от взносов (вычет типа А).", "title": "ИИС", "url": "docs/iis"},
            {"text": "Налог на дивиденды составляет 13% для резидентов.", "title": "Налоги", "url": "docs/taxes"},
            {"text": "Облигации федерального займа (ОФЗ) считаются надежными.", "title": "Облигации", "url": "docs/bonds"},
        ]

        points = []
        for idx, doc in enumerate(docs):
            vector = model.encode(doc["text"]).tolist()
            points.append(PointStruct(
                id=idx + 1,
                vector=vector,
                payload=doc # Сохраняем поля title и url, которые нужны вашему коду
            ))
        
        client.upsert(collection_name=collection_name, points=points)
        print(f"--- 2. Загружено {len(docs)} документа в память ---")

# --- ЗАПУСК ТЕСТА ---
async def main():
    # === ГЛАВНОЕ ИЗМЕНЕНИЕ ЗДЕСЬ ===
    # location=":memory:" создает базу прямо в оперативной памяти.
    # Она исчезнет, когда скрипт завершится. Docker не нужен.
    client = QdrantClient(location=":memory:") 
    
    config = MockConfig()
    logger = MockLogger()

    # Заполняем базу данными
    await setup_qdrant_data(client, config.QDRANT_COLLECTIONS_NAME)

    # Инициализируем ваш UseCase
    use_case = QueryUsecase(qdrant=client, logger=logger, config=config)

    # Задаем вопрос
    query_text = "как вернуть налог с инвестиций?" # Должен найти про ИИС
    query_dto = Query(query_topic=query_text)

    print(f"\n--- 3. Запрос пользователя: '{query_text}' ---")
    
    try:
        # Запускаем ваш метод
        result = await use_case.processes_query(query_dto)
        
        print("\n=== РЕЗУЛЬТАТ ОТ LLM/ПОИСКА ===")
        print(result.text)
            
    except Exception as e:
        print(f"\n[ОШИБКА] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
