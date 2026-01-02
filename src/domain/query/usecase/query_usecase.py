import time
from qdrant_client import QdrantClient

from domain.query.query import Query, QueryResults
from domain.query.delivery.dto.dto import HistoryResponseDTO
from domain.query.delivery.dto.dto import HistoryItemDTO
from domain.query.usecase.i_query_usecase import IQueryUsecase
from infrastructure.db.qdrand import get_embedded_model
from config.config import config

MAX_TOKENS = 4096 

class QueryUsecase(IQueryUsecase):
    def __init__(self, qdrant: QdrantClient, logger, config):
        self.qdrant = qdrant
        self.logger = logger
        self.model = get_embedded_model()
        self.collections_name = config.QDRANT_COLLECTIONS_NAME
        self.history: list[HistoryResponseDTO] | list = []

    async def _private_method_1_used_in_processes_query_method(self, query_topic):
        if not query_topic:
            raise ValueError("Query topic cannot be empty.")
        return self.model.encode(query_topic)

    def _private_method_2_used_in_processes_query_method(self, embedding):
        if not embedding:
            raise TypeError("Embedding cannot be None.")
        results = self.qdrant.search(
            collection_name=self.collections_name,
            query_vector=embedding,
            limit=3,
        )
        return [res.payload for res in results]

    async def _private_method_3_used_in_processes_query_method(self, nearests_texts, query_topic):
        context = "\n".join([f"{doc['title']}: {doc['url']}" for doc in nearests_texts])
        return {"answer": f"{query_topic}: {context}"}

    async def processes_query(self, query: Query) -> QueryResults:
        """Processes the query topic and returns the final result."""
        start_time = time.time()

        embedding = await self._private_method_1_used_in_processes_query_method(query.query_topic)

        nearests_texts = self._private_method_2_used_in_processes_query_method(embedding)

        model_answer = await self._private_method_3_used_in_processes_query_method(nearests_texts, query.query_topic)

        self.history.append(HistoryItemDTO(
            query=query.query_topic,
            response=model_answer['answer'],
            timestamp=time.time(),
            duration=time.time() - start_time,
        ))
        return QueryResults(text=model_answer['answer'])