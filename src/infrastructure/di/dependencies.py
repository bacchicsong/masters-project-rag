import os
import logging

from fastapi import Depends, HTTPException
from qdrant_client import QdrantClient

from config.config import config
from domain.query.usecase.i_query_usecase import IQueryUsecase
from domain.query.usecase.query_usecase import QueryUsecase
from infrastructure.db.qdrand import init_qdrant

def get_logger() -> logging.Logger:
    logger = logging.getLogger("app_logger")
    return logger

def verify_token(x_token: str) -> str | HTTPException:
    expected_token = os.getenv("API_ACESS_TOKEN")
    if x_token != expected_token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return x_token


def get_qdrant(logger: logging.Logger = Depends(get_logger)) -> QdrantClient:
    qd = init_qdrant(logger)
    try:
        yield qd
    finally:
        qd.close()

def get_query_usecase(
    qdrant: QdrantClient = Depends(get_qdrant),
    logger: logging.Logger = Depends(get_logger),
) -> IQueryUsecase:
    return QueryUsecase(qdrant, logger)
