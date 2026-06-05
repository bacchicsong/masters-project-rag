from typing import Optional

import httpx
from domain.query.delivery.dto.dto import QueryResponseDTO


class FastAPIClient:
    def __init__(
        self,
        base_url: str,
        api_token: str,
        timeout: float = 30.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_token = api_token
        self.timeout = timeout

    async def forward(
        self,
        query_topic: str,
        system_prompt: Optional[str] = None,
    ) -> QueryResponseDTO:
        headers = {
            "X-Token": self.api_token,
        }

        data = {
            "query_topic": query_topic,
        }

        if system_prompt is not None:
            data["system_prompt"] = system_prompt

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}/api/v1/forward",
                headers=headers,
                data=data,
            )

        response.raise_for_status()
        return QueryResponseDTO(**response.json())
    