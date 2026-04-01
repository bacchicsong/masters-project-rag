import json
import os
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

FEEDBACK_DIR = Path(__file__).parent.parent.parent.parent / "data" / "feedback"
FEEDBACK_FILE = FEEDBACK_DIR / "feedback.jsonl"


class TripletRecord:
    def __init__(
        self,
        query: str,
        query_id: str,
        positive_doc: str,
        negative_doc: str,
        timestamp: str,
    ):
        self.query = query
        self.query_id = query_id
        self.positive_doc = positive_doc
        self.negative_doc = negative_doc
        self.timestamp = timestamp

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "query_id": self.query_id,
            "positive_doc": self.positive_doc,
            "negative_doc": self.negative_doc,
            "timestamp": self.timestamp,
        }

    @staticmethod
    def from_dict(d: dict) -> "TripletRecord":
        return TripletRecord(
            query=d["query"],
            query_id=d["query_id"],
            positive_doc=d["positive_doc"],
            negative_doc=d["negative_doc"],
            timestamp=d["timestamp"],
        )


class FeedbackStorage:
    def __init__(self):
        os.makedirs(FEEDBACK_DIR, exist_ok=True)

    def save_triplet(self, triplet: TripletRecord):
        with open(FEEDBACK_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(triplet.to_dict(), ensure_ascii=False) + "\n")

    def load_all(self, since: Optional[str] = None) -> List[TripletRecord]:
        if not FEEDBACK_FILE.exists():
            return []
        records = []
        with open(FEEDBACK_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = TripletRecord.from_dict(json.loads(line))
                if since and record.timestamp < since:
                    continue
                records.append(record)
        return records

    def count(self) -> int:
        if not FEEDBACK_FILE.exists():
            return 0
        with open(FEEDBACK_FILE, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())