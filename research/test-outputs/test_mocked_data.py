import asyncio
import sys
import urllib3
from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

src_path = Path(__file__).parent.parent.parent / "src"
sys.path.append(str(src_path.resolve()))

from domain.query.usecase.query_usecase import QueryUsecase
from domain.query.query import Query
from infrastructure.db.qdrand import get_embedded_model

class MockConfig:
    QDRANT_COLLECTIONS_NAME = "test_collection_local"
    
    GIGACHAT_AUTH_KEY = '...'

class MockLogger:
    def info(self, message): print(f"[INFO] {message}")
    def error(self, message): print(f"[ERROR] {message}")
    def warning(self, message): print(f"[WARN] {message}")

async def setup_qdrant_data(client: QdrantClient, collection_name: str):
    print("1. Инициализация базы данных (в памяти)")
    model = get_embedded_model()
    
    if not client.collection_exists(collection_name):
        sample_vec = model.encode("test")
        dim = len(sample_vec)
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

        docs = [
            {"text": "Брокерский счет позволяет торговать акциями и валютой самостоятельно.", "title": "Брокерский счет", "url": "docs/broker"},
            {"text": "ИИС (Индивидуальный Инвестиционный Счет) дает право на вычет 13% от взносов (тип А). Деньги нельзя снимать 3 года.", "title": "ИИС", "url": "docs/iis"},
            {"text": "Налог на доход от инвестиций - 13%. Брокер удерживает его сам.", "title": "Налоги", "url": "docs/taxes"},
        ]

        points = []
        for idx, doc in enumerate(docs):
            vector = model.encode(doc["text"]).tolist()
            points.append(PointStruct(id=idx + 1, vector=vector, payload=doc))
        
        client.upsert(collection_name=collection_name, points=points)
        print(f"2. Загружено {len(docs)} документа в Qdrant")

async def main():
    client = QdrantClient(location=":memory:") 
    
    config = MockConfig()
    logger = MockLogger()

    await setup_qdrant_data(client, config.QDRANT_COLLECTIONS_NAME)

    use_case = QueryUsecase(qdrant=client, logger=logger, config=config)

    query_text = "Что такое брокеркий счёт?"
    query_dto = Query(query_topic=query_text)


    print(f"3. Запрос: '{query_text}'")
    
    try:
        result = await use_case.processes_query(query_dto)
        
        print("4. Ответ GigaChat:")
        print(result.text)

    except Exception as e:
        print(f"\n[ОШИБКА] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())