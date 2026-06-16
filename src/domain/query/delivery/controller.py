from typing import Optional
from dotenv import load_dotenv

from fastapi import APIRouter, Depends, Form, Header, HTTPException

from domain.query.delivery.dto.dto import (
    QueryResponseDTO,
    HistoryResponseDTO,
    StatsResponseDTO,
    FeedbackRequestDTO,
    FeedbackResponseDTO,
)
from domain.query.query import Query
from domain.query.usecase.i_query_usecase import IQueryUsecase
from infrastructure.di.dependencies import get_query_usecase, verify_token
from config.config import RAG_CONFIG
from tools.fine_tune_bi_encoder import fine_tune
from tools.minio_ingest import ingest_minio_to_qdrant
from tools.prometheus_metrics import RAG_FINE_TUNE_RUNS_TOTAL

load_dotenv()

router = APIRouter(prefix="/api/v1")


@router.post("/forward")
async def forward(
    query_topic: str,
    system_promt: Optional[str] = Form(None),
    token: str = Depends(verify_token),
    service: IQueryUsecase = Depends(get_query_usecase),
) -> QueryResponseDTO:
    _ = token
    try:
        research_results = await service.processes_query(
            Query(
                query_topic=query_topic,
                system_promt=system_promt,
            )
        )
        return QueryResponseDTO(
            text=research_results.text,
            query_id=research_results.query_id,
            retrieved_doc_ids=research_results.retrieved_doc_ids,
        )

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
    history_depth: int = Form(...),
    service: IQueryUsecase = Depends(get_query_usecase),
) -> HistoryResponseDTO | dict[str, str]:
    if not service.history:
        return {"details": "There is no available history yet"}
    if len(service.history) <= history_depth:
        return service.history
    return service.history[:history_depth]


@router.get("/stats")
async def get_stats(
    history_depth: int = Form(...),
    service: IQueryUsecase = Depends(get_query_usecase),
) -> StatsResponseDTO | dict[str, str]:
    if not service.history:
        return {"details": "There is no available history yet"}
    if len(service.history) <= history_depth:
        history = service.history
    else:
        history = service.history[:history_depth]

    durations = [obj["duration"] for obj in history]
    lenghts = [len(obj["query"]) for obj in history]
    mean_time = sum(durations) / len(durations)
    mean_len = sum(lenghts) / len(lenghts)
    durations.sort()
    q50 = durations[len(durations) // 2]
    q95 = durations[int(len(durations) * 0.95)]
    q99 = durations[int(len(durations) * 0.99)]

    return StatsResponseDTO(
        total_queries=len(history),
        mean_time=float(mean_time),
        quantiles={
            "50%": float(q50),
            "95%": float(q95),
            "99%": float(q99),
        },
        query_stats={
            "avg_query_len": float(mean_len),
            "max_query_len": float(max(lenghts)),
            "min_query_len": float(min(lenghts)),
        },
    )


@router.get("/health")
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


def verify_internal_token(x_internal_token: str = Header("")):
    if x_internal_token != RAG_CONFIG.INTERNAL_API_TOKEN:
        raise HTTPException(status_code=401, detail="Unauthorized")


@router.post("/admin/ingest/minio")
async def ingest_from_minio(_: str = Depends(verify_internal_token)) -> dict:
    """Internal endpoint used by Airflow to load parsed JSON objects into Qdrant."""
    return ingest_minio_to_qdrant()


@router.post("/admin/fine-tune-feedback")
async def fine_tune_feedback(_: str = Depends(verify_internal_token)) -> dict:
    """Internal endpoint used by Airflow to fine-tune the bi-encoder from feedback triplets."""
    try:
        result = fine_tune()
        RAG_FINE_TUNE_RUNS_TOTAL.labels(status=result.get("status", "unknown")).inc()
        return result
    except Exception as e:
        RAG_FINE_TUNE_RUNS_TOTAL.labels(status="failed").inc()
        raise HTTPException(status_code=500, detail=f"Fine-tuning failed: {e}")
