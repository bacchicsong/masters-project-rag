from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional, Dict


class QueryResponseDTO(BaseModel):
    text: str


class QueryRequestDTO(BaseModel):
    query_topic: str
    system_prompt: Optional[str] = None


class HistoryItemDTO(BaseModel):
    query: str
    response: str
    timestamp: datetime
    duration: float


class HistoryResponseDTO(BaseModel):
    history: List[HistoryItemDTO]


class StatsResponseDTO(BaseModel):
    total_queries: int
    mean_time: float
    quantiles: Dict[str, float]
    query_stats: Dict[str, float]
