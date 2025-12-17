from domain.query.query import Query, QueryResults
from domain.query.usecase.i_query_usecase import IQueryUsecase

MAX_TOKENS = 4096 

class QueryUsecase(IQueryUsecase):
    def __init__(self, ...):
        pass

    def _private_method_2_used_in_processes_query_method(self, embedding):
        """Doing some logic 2."""
        if not embedding:
            raise TypeError("Embedding cannot be None.")
        pass

    async def _private_method_1_used_in_processes_query_method(self, query_topic):
        """Doing some logic 1."""
        # take user text and translate to embedding with model
        if not query_topic:
            raise ValueError("Query topic cannot be empty.")
        pass

    async def _private_method_3_used_in_processes_query_method(self, nearests_texts, query_topic):
        """Doing some logic 3."""
        # send context and user query to LLM. 
        # return LLM answer



    async def processes_query(self, query: Query) -> QueryResults:
        """Processes the query topic and returns the final result."""
        embedding = await self._private_method_1_used_in_processes_query_method(query.query_topic)

        nearests_texts = self._private_method_2_used_in_processes_query_method(embedding)

        model_answer = await self._private_method_3_used_in_processes_query_method(nearests_texts, query.query_topic)
        
        return QueryResults(text=model_answer['answer'])