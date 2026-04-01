import uuid
from typing import Optional, List


class Query:
    def __init__(
        self,
        query_topic: str,
        system_promt: Optional[str] = None,
    ):
        self.query_topic = query_topic
        self.system_promt = (
            system_promt
            or """\
        your prompt
        """
        )


class QueryResults:
    def __init__(self, text, query_id: str = None, retrieved_doc_ids: List[int] = None):
        self.text = text
        self.query_id = query_id or str(uuid.uuid4())
        self.retrieved_doc_ids = retrieved_doc_ids or []
