import logging

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer

from config.config import config

QDRANT_TIMEOUT = 180
EMBEDDING_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"

def get_embedded_model() -> SentenceTransformer:
    return SentenceTransformer(EMBEDDING_MODEL_NAME)

def init_qdrant(logger) -> QdrantClient:
    global collection_exists
    qdrant = QdrantClient(
        url=f"http://{config.QDRANT_HOST}:{config.QDRANT_PORT}",
        timeout=QDRANT_TIMEOUT,
    )
    collection_name = config.QDRANT_COLLECTION_NAME
    if not collection_exists:
        if not qdrant.collection_exists(collection_name=collection_name):
            qdrant.create_collection(
                collection_name=collection_name,
                vectors_config=VectorParams(size=384, distance=Distance.COSINE)
            )
            collection_exists = True
            logger.info('Новая коллекция была создана')
    return qdrant

def insert_document(qdrant: QdrantClient, collection_name: str, doc: dict, idx: int):
    model = get_embedded_model()
    content = " ".join(doc["content"])
    sections = "\n".join(f"{sec['heading']}: {' '.join(sec['content'])}" for sec in doc.get("section", []))
    text = f"{doc['title']} {content} {sections}"

    embedding = model.encode(text)
    point = PointStruct(id=idx, vector=embedding.tolist(), payload=doc)
    qdrant.upsert(collection_name=collection_name, points=[point])
