import asyncio
import sys
import json
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
    QDRANT_COLLECTIONS_NAME = "real_data_test"
    GIGACHAT_AUTH_KEY = '...'

class MockLogger:
    def info(self, message): print(f"[INFO] {message}")
    def error(self, message): print(f"[ERROR] {message}")
    def warning(self, message): print(f"[WARN] {message}")


def load_json_files(directory: str) -> list[dict[str, str]]:
    json_dir = Path(directory)
    all_docs = []
    
    for file in json_dir.glob("*.json"):
        with file.open("r", encoding="utf-8") as f:
            data = json.load(f)
            
            if isinstance(data, dict):
                if "sections" in data:
                    all_docs.extend(data["sections"])
            
            elif isinstance(data, list):
                for item in data[:100]:
                    if isinstance(item, dict) and "sections" in item:
                        all_docs.extend(item["sections"])
            else:
                raise ValueError(f"Unsupported file format in {file.name}")
                
    return all_docs

async def setup_qdrant_from_local_files(client: QdrantClient, collection_name: str):
    print("1. Загрузка файлов")
    
    raw_docs = load_json_files("data")
    
    if not raw_docs:
        print("Не найдено документов в папке data.")
        return False

    print(f"Загружено JSON: {len(raw_docs)}")
    print("2. Векторизация и индексация")

    model = get_embedded_model()

    if not client.collection_exists(collection_name):
        sample_vec = model.encode("test")
        client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=len(sample_vec), distance=Distance.COSINE),
        )

    points = []
    for idx, doc in enumerate(raw_docs):
        text_representation = json.dumps(doc, ensure_ascii=False)
        
        vector = model.encode(text_representation[:8000]).tolist()
        
        payload = doc.copy()
        
        payload['text'] = text_representation 

        points.append(PointStruct(
            id=idx + 1,
            vector=vector,
            payload=payload
        ))
        
        if (idx + 1) % 10 == 0:
            print(f"Обработано {idx + 1}")

    client.upsert(collection_name=collection_name, points=points)
    print(f"Загружено {len(points)} документов в Qdrant")
    return True


async def main():
    client = QdrantClient(location=":memory:") 
    
    config = MockConfig()
    logger = MockLogger()

    success = await setup_qdrant_from_local_files(client, config.QDRANT_COLLECTIONS_NAME)
    if not success:
        return

    use_case = QueryUsecase(qdrant=client, logger=logger, config=config)

    query_text = "Как продать акцию?" 
    
    query_dto = Query(query_topic=query_text)

    print(f"3. Запрос пользователя: '{query_text}'")
    
    try:
        result = await use_case.processes_query(query_dto)
        
        print("ОТВЕТ GIGACHAT:")
        print(result.text)

    except Exception as e:
        print(f"КРИТИЧЕСКАЯ ОШИБКА {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())