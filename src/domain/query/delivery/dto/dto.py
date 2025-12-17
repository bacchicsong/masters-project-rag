from pydantic import BaseModel


class QueryResponseDTO(BaseModel):
    text: str
