import logging

from qdrant_client import QdrantClient

from config.config import config

QDRANT_TIMEOUT = 180

collection_exists = False


def init_qdrant():
    global collection_exists
    qdrant = QdrantClient(
        url=f"http://{config.QDRANT_HOST}:{config.QDRANT_PORT}",
        timeout=QDRANT_TIMEOUT,
    )
    collection_name = config.QDRANT_COLLECTION_NAME
    if not collection_exists:
        if not qdrant.collection_exists(collection_name=collection_name):
            qdrant.create_collection(...)
            collection_exists = True
    return qdrant
