"""
Document processing utilities for RAG experiments.
Extracts text from documents using different strategies.
"""
from typing import List, Dict, Any, Optional

# Available text extraction strategies
STRATEGIES = ["full", "title_headings", "title_only"]


class DocumentProcessor:
    """Processes documents to extract text using various strategies."""

    @staticmethod
    def extract_text(doc: Dict[str, Any], mode: str = "full") -> str:
        """
        Extract text from a document according to the specified strategy.

        Args:
            doc: Document dict with 'title' and 'sections' keys
            mode: Extraction strategy:
                  - 'full': title + all section headings and content
                  - 'title_headings': title + section headings only
                  - 'title_only': title only

        Returns:
            Extracted text string
        """
        if mode not in STRATEGIES:
            raise ValueError(f"Unknown strategy '{mode}'. Available: {STRATEGIES}")

        parts = []

        # Always include title
        title = doc.get("title", doc.get("id", ""))
        if title:
            parts.append(title)

        sections = doc.get("sections", [])

        if mode == "full":
            for section in sections:
                heading = section.get("heading", "")
                if heading:
                    parts.append(heading)

                content = section.get("content", [])
                if isinstance(content, list):
                    parts.extend(str(c) for c in content if c)
                elif isinstance(content, str) and content:
                    parts.append(content)

        elif mode == "title_headings":
            for section in sections:
                heading = section.get("heading", "")
                if heading:
                    parts.append(heading)

        # title_only: nothing extra to add

        # Filter empty strings and join
        return " ".join(filter(None, parts))

    @staticmethod
    def get_doc_metadata(doc: Dict[str, Any]) -> Dict[str, Any]:
        """Extract metadata from a document."""
        return {
            "id": doc.get("id", doc.get("url", "unknown")),
            "title": doc.get("title", "No title"),
            "url": doc.get("url", ""),
            "num_sections": len(doc.get("sections", [])),
        }

    @staticmethod
    def chunk_text(
        text: str,
        chunk_size: int = 512,
        overlap: int = 50,
        min_chunk_size: int = 50
    ) -> List[str]:
        """
        Split text into overlapping chunks.

        Args:
            text: Input text to chunk
            chunk_size: Target chunk size in characters
            overlap: Overlap between chunks in characters
            min_chunk_size: Minimum chunk size to include

        Returns:
            List of text chunks
        """
        if not text or len(text) < min_chunk_size:
            return [text] if text else []

        chunks = []
        start = 0

        while start < len(text):
            end = min(start + chunk_size, len(text))

            # Try to break at a sentence boundary
            if end < len(text):
                # Look for sentence endings within the last 20% of the chunk
                search_start = max(start, end - chunk_size // 5)
                last_period = text.rfind(".", search_start, end)
                last_newline = text.rfind("\n", search_start, end)
                last_boundary = max(last_period, last_newline)

                if last_boundary > search_start:
                    end = last_boundary + 1

            chunk = text[start:end].strip()
            if len(chunk) >= min_chunk_size:
                chunks.append(chunk)

            if end >= len(text):
                break
            new_start = end - overlap
            if new_start <= start:
                new_start = end
            start = new_start

        return chunks


def get_text_by_strategy(doc: Dict[str, Any], strategy: str) -> str:
    """Convenience function to extract text by strategy."""
    return DocumentProcessor.extract_text(doc, strategy)