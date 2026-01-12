import json
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer

from config.config import RAG_CONFIG

QDRANT_TIMEOUT = 180
EMBEDDING_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


def get_embedded_model() -> SentenceTransformer:
    return SentenceTransformer(EMBEDDING_MODEL_NAME)


def init_qdrant(logger) -> QdrantClient:
    global collection_exists
    qdrant = QdrantClient(
        url=f"http://{RAG_CONFIG.QDRANT_HOST}:{RAG_CONFIG.QDRANT_PORT}",
        timeout=QDRANT_TIMEOUT,
    )
    collection_name = RAG_CONFIG.QDRANT_COLLECTION_NAME
    collection_exists = qdrant.collection_exists(collection_name=collection_name)
    if not collection_exists:
        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )
        collection_exists = True
        logger.info(f"Новая коллекция '{collection_name}' была создана")
    return qdrant


def insert_document(qdrant: QdrantClient, collection_name: str, doc: dict, idx: int):
    model = get_embedded_model()
    text_representation = json.dumps(doc, ensure_ascii=False)
    payload = doc.copy()
    payload['text'] = text_representation 
    vector = model.encode(text_representation).tolist()

    point = PointStruct(id=idx, vector=vector, payload=payload)
    qdrant.upsert(collection_name=collection_name, points=[point])
