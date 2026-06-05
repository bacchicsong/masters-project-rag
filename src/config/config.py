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
    MODEL_NAME: str = "sberbank-ai/sbert_large_nlu_rus"
    LLM_MODEL: str | None = "cointegrated/rut5-base-absum"

    class Config:
        env_file = ".env"
        extra = "ignore"


RAG_CONFIG = Settings()
