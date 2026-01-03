from fastapi import FastAPI

from config.config import RAG_CONFIG
from domain.query.delivery.controller import router

app = FastAPI(docs_url="/swagger")

app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=RAG_CONFIG.APP_HOST, port=RAG_CONFIG.APP_PORT)
