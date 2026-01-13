from abc import ABC, abstractmethod

from domain.query.query import Query, QueryResults


class IQueryUsecase(ABC):
    @abstractmethod
    async def processes_query(self, query: Query) -> QueryResults:
        """Processes the query topic and returns the final result."""
        raise NotImplementedError
