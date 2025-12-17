from fastapi import FastAPI

from config.config import config
from domain.query.delivery.controller import router

app = FastAPI(docs_url="/swagger")

app.include_router(router)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=config.APP_HOST, port=config.APP_PORT)
