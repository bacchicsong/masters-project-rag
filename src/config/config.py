from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8088
    QDRANT_HOST: str = "qdrant"
    QDRANT_PORT: int = 6333
    QDRANT_COLLECTION_NAME: str = "definitions"
    MODEL_NAME: str = "sberbank-ai/sbert_large_nlu_rus"
    LLM_MODEL: str = "cointegrated/rut5-base-absum"
    GIGACHAT_AUTH_KEY: str

    class Config:
        env_file = ".env"


RAG_CONFIG = Settings()
