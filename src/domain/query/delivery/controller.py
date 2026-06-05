from typing import Optional
from dotenv import load_dotenv

from fastapi import APIRouter, Depends, Form, HTTPException, Query

from domain.query.delivery.dto.dto import (
    QueryResponseDTO,
    HistoryResponseDTO,
    StatsResponseDTO,
    FeedbackRequestDTO,
    FeedbackResponseDTO,
)
from domain.query.query import Query as DomainQuery
from domain.query.usecase.i_query_usecase import IQueryUsecase
from infrastructure.di.dependencies import get_query_usecase, verify_token

load_dotenv()

router = APIRouter(prefix="/api/v1")


@router.post("/forward")
async def forward(
    query_topic: str = Form(...),
    system_prompt: Optional[str] = Form(None),
    token: str = Depends(verify_token),
    service: IQueryUsecase = Depends(get_query_usecase),
) -> QueryResponseDTO:
    _ = token
    try:
        research_results = await service.processes_query(
            DomainQuery(
                query_topic=query_topic,
                system_prompt=system_prompt,
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
    history_depth: int = Query(10),
    service: IQueryUsecase = Depends(get_query_usecase),
) -> HistoryResponseDTO | dict[str, str]:
    if not service.history:
        return {"details": "There is no available history yet"}
    history_items = service.history if len(service.history) <= history_depth else service.history[:history_depth]
    return HistoryResponseDTO(history=history_items)


@router.get("/stats")
async def get_stats(
    history_depth: int = Query(10),
    service: IQueryUsecase = Depends(get_query_usecase),
) -> StatsResponseDTO | dict[str, str]:
    if not service.history:
        return {"details": "There is no available history yet"}
    if len(service.history) <= history_depth:
        history = service.history
    else:
        history = service.history[:history_depth]

    durations = [obj.duration for obj in history]
    lengths = [len(obj.query) for obj in history]
    if not durations:
        return {"details": "No history data available for stats"}

    mean_time = sum(durations) / len(durations)
    sorted_durations = sorted(durations)
    mean_len = sum(lengths) / len(lengths)
    q50 = sorted_durations[len(sorted_durations) // 2]
    q95 = sorted_durations[int(len(sorted_durations) * 0.95)]
    q99 = sorted_durations[int(len(sorted_durations) * 0.99)]

    return StatsResponseDTO(
        total_queries=len(history),
        mean_time=mean_time,
        quantiles={
            "50%": q50,
            "95%": q95,
            "99%": q99,
        },
        query_stats={
            "avg_query_len": mean_len,
            "max_query_len": max(lengths),
            "min_query_len": min(lengths),
        },
    )


@router.post("/health")
async def health_check() -> dict[str, str]:
    return {"status": "ok"}


@router.post("/feedback")
async def submit_feedback(
    feedback: FeedbackRequestDTO,
    service: IQueryUsecase = Depends(get_query_usecase),
) -> FeedbackResponseDTO:
    """Accept user feedback on a query response and store triplet for fine-tuning."""
    try:
        count = service.save_feedback(feedback)
        return FeedbackResponseDTO(
            status="ok",
            triplet_count=count,
        )
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing feedback: {e}"
        )
