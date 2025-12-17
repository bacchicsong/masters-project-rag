import logging

from fastapi import Depends

from qdrant_client import QdrantClient

from config.config import config
from domain.query.usecase.i_query_usecase import IQueryUsecase
from domain.query.usecase.query_usecase import QueryUsecase
from infrastructure.db.qdrant import init_qdrant

def get_logger() -> logging.Logger:
    logger = logging.getLogger("app_logger")
    return logger


def get_qdrant(logger: logging.Logger = Depends(get_logger)) -> QdrantClient:
    qd = init_qdrant(logger)
    try:
        yield qd
    finally:
        qd.close()

def get_gigachat_embbedder() -> GigaChatEmbedder:
    return GigaChatEmbedder()

def get_query_usecase(
    qdrant: QdrantClient = Depends(get_qdrant),
    logger: logging.Logger = Depends(get_logger),
    ...
) -> IQueryUsecase:
    return QueryUsecase(qdrant, logger, ...)


def get_answer_usecase(
        logger: logging.Logger = Depends(get_logger)
        embedder: GigaChatEmbedder = Depends(get_gigachat_embbedder)
) -> IAnswerUsecase:
    return AnswerUsecase(logger, embedder)