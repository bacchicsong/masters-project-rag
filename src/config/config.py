from pathlib import Path
import os

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).resolve().parent.parent
ENV_PATH = BASE_DIR / ".env"

if ENV_PATH.exists():
    load_dotenv(ENV_PATH)


class Settings(BaseSettings):
    GIGACHAT_AUTH_KEY: str
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8088
    QDRANT_HOST: str = "qdrant"
    QDRANT_PORT: int = 6333
    QDRANT_COLLECTION_NAME: str = "definitions"
    TELEGRAM_BOT_TOKEN: str = ""
    ENABLE_TELEGRAM_BOT: bool = False
    USE_CROSS_ENCODER: bool = False
    PRELOAD_EMBEDDING_MODEL: bool = True
    INTERNAL_API_TOKEN: str = "local-dev-token"
    MINIO_ENDPOINT: str = "minio:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin"
    MINIO_SECURE: bool = False
    MINIO_DATA_BUCKET: str = "rag-data"
    MINIO_DATA_PREFIX: str = "parsed/"
    EMBEDDING_MODEL_NAME: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    MODEL_NAME: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    LLM_MODEL: str | None = "cointegrated/rut5-base-absum"

    class Config:
        env_file = ".env"


RAG_CONFIG = Settings()
