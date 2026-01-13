import asyncio
import sys
from pathlib import Path

src_path = Path(__file__).parent.parent.parent / "src"
sys.path.append(str(src_path.resolve()))

from config.config import RAG_CONFIG
from tools.client import FastAPIClient

client = FastAPIClient(
    base_url="http://localhost:8088",
    api_token=RAG_CONFIG.GIGACHAT_AUTH_KEY,
)

response = asyncio.run(
    client.forward(
        query_topic="Что такое ПИФ?",
        system_prompt="Ты - финансовый консультант.",
    )
)

print(response.text)