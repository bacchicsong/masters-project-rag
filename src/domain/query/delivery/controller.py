from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException

from domain.query.delivery.dto.dto import QueryResponseDTO
from domain.query.query import Query
from domain.query.usecase.i_query_usecase import IQueryUsecase
from infrastructure.di.dependencies import get_query_usecase

router = APIRouter(prefix="/api/v1")


@router.post("/forward")
async def forward(
    query_topic: str,
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


@router.post("/health")
async def health_check() -> dict:
    return {"status": "ok"}
