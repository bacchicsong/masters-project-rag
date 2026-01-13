from typing import Optional


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
    def __init__(self, text):
        self.text = text
