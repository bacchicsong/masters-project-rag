import json
import re
import unicodedata
import zipfile
from difflib import get_close_matches
from pathlib import Path

GOLDEN_ZIP = Path(__file__).resolve().parent.parent.parent / "data-collection" / "Q_A_articles.zip"


def normalize_title(title):
    t = unicodedata.normalize("NFKC", title or "")
    t = t.replace("\u00a0", " ").lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("–", "-").replace("—", "-")
    t = re.sub(r": частным лицам.*", "", t)
    return t


def fix_mojibake(text):
    for enc in ("latin-1", "cp1252", "cp437"):
        try:
            fixed = text.encode(enc).decode("utf-8")
            if re.search(r"[а-яА-ЯёЁ]", fixed):
                return fixed
        except UnicodeError:
            pass
    return text


def _article_title(content, filename):
    m = re.search(r'по статье [«"](.+?)[»"]', content)
    if m:
        return fix_mojibake(m.group(1).strip())
    name = Path(filename).name
    if name.startswith("._"):
        return ""
    return fix_mojibake(Path(name).stem.replace(".json", ""))


def load_qa_from_zip(zip_path=None):
    path = Path(zip_path) if zip_path else GOLDEN_ZIP
    records = []

    with zipfile.ZipFile(path) as zf:
        for name in zf.namelist():
            if not name.endswith(".json") or "__MACOSX" in name or "/._" in name:
                continue
            data = json.loads(zf.read(name))
            content = data.get("content", "")
            title = _article_title(content, name)
            if not title:
                continue
            for i, q in enumerate(re.findall(r"##\s*Вопрос\s*\d+:\s*(.+?)\n", content), 1):
                slug = re.sub(r"\W+", "_", normalize_title(title))[:40]
                records.append({
                    "query_id": f"{slug}_{i}",
                    "query": q.strip(),
                    "article_title": title,
                })

    return records


def _match_doc_id(article_title, title_index, docs):
    norm = normalize_title(article_title)
    if norm in title_index:
        return title_index[norm]

    fixed = normalize_title(fix_mojibake(article_title))
    if fixed in title_index:
        return title_index[fixed]

    for doc in docs:
        doc_norm = normalize_title(doc.get("title", ""))
        if norm in doc_norm or doc_norm in norm or fixed in doc_norm or doc_norm in fixed:
            return doc["id"]

    close = get_close_matches(norm, list(title_index.keys()), n=1, cutoff=0.82)
    return title_index[close[0]] if close else None


def attach_ground_truth(qa_records, docs):
    title_index = {normalize_title(d.get("title", "")): d["id"] for d in docs if d.get("title")}
    matched, unmatched = [], []

    for rec in qa_records:
        doc_id = _match_doc_id(rec["article_title"], title_index, docs)
        item = {**rec, "relevant_doc_ids": [doc_id] if doc_id else []}
        (matched if doc_id else unmatched).append(item)

    stats = {
        "total_qa": len(qa_records),
        "matched_queries": len(matched),
        "unmatched_queries": len(unmatched),
        "unmatched_titles": sorted({r["article_title"] for r in unmatched}),
    }
    return matched, stats
