def extract_text(doc, mode="full"):
    title = doc.get("title") or doc.get("id") or ""
    parts = [title] if title else []

    if mode == "title_only":
        return title

    for section in doc.get("sections", []):
        heading = section.get("heading", "")
        if heading:
            parts.append(heading)
        if mode != "full":
            continue
        content = section.get("content", [])
        if isinstance(content, list):
            parts.extend(str(c) for c in content if c)
        elif content:
            parts.append(str(content))

    return " ".join(parts)


def get_text_by_strategy(doc, strategy):
    if strategy not in ("full", "title_headings", "title_only"):
        raise ValueError(f"unknown strategy: {strategy}")
    return extract_text(doc, strategy)


def chunk_text(text, chunk_size=512, overlap=50, min_chunk_size=50):
    if not text:
        return []
    if len(text) < min_chunk_size:
        return [text]

    chunks = []
    start = 0
    while start < len(text):
        end = min(start + chunk_size, len(text))
        if end < len(text):
            search_from = max(start, end - chunk_size // 5)
            cut = max(text.rfind(".", search_from, end), text.rfind("\n", search_from, end))
            if cut > search_from:
                end = cut + 1

        piece = text[start:end].strip()
        if len(piece) >= min_chunk_size:
            chunks.append(piece)

        if end >= len(text):
            break
        start = max(end - overlap, start + 1)

    return chunks
