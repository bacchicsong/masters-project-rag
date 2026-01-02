import os
from typing import Optional
from dotenv import load_dotenv

from fastapi import APIRouter, Depends, Form, HTTPException

from domain.query.delivery.dto.dto import QueryResponseDTO, HistoryResponseDTO, StatsResponseDTO
from domain.query.query import Query
from domain.query.usecase.i_query_usecase import IQueryUsecase
from infrastructure.di.dependencies import get_query_usecase, verify_token

load_dotenv()

router = APIRouter(prefix="/api/v1")

@router.post("/forward")
async def forward(
    query_topic: str,
    token: str = Depends(verify_token),
    system_promt: Optional[str] = Form(None),
    service: IQueryUsecase = Depends(get_query_usecase),
) -> QueryResponseDTO:

    try:
        research_results = await service.processes_query(
            Query(
                query_topic=query_topic,
                system_promt=system_promt,
            )
        )
        return QueryResponseDTO(text=research_results.text)
    
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except TypeError as te:
        raise HTTPException(status_code=422, detail=str(te))
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing query topic: {e}"
        )

@router.get("/history")
async def get_history(
    token: str = Depends(verify_token),
    service: IQueryUsecase = Depends(get_query_usecase)
) -> HistoryResponseDTO:
    return service.history

@router.get("/stats")
async def get_stats(
    token: str = Depends(verify_token),
    service: IQueryUsecase = Depends(get_query_usecase)
) -> StatsResponseDTO | dict[str, str]:
    history = service.history
    if not all(history.values()):
        return {"details": "There is no available history yet"}
    
    durations = [obj["duration"] for obj in history]
    lenghts = [len(obj["query"]) for obj in history]
    mean_time = sum(durations) / len(durations)
    mean_len = sum(lenghts) / len(lenghts)
    mean_time.sort()
    q50 = durations[len(durations) // 2]
    q95 = durations[int(len(durations) * 0.95)]
    q99 = durations[int(len(durations) * 0.99)]

    return StatsResponseDTO(
        total_queries=len(history),
        mean_time=mean_time,
        quantiles={
            "50%": q50, "95%": q95, "99%": q99,
        },
        query_stats={
            "avg_query_len": mean_len,
            "max_query_len": max(lenghts),
            "min_query_len": min(lenghts)
        },
    )

@router.post("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}
