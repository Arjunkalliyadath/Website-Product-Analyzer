import re
from typing import List, Tuple

def clean_comment(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def normalize_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "")
    text = re.sub(r"\u200b", "", text)
    return text.strip()

def remove_links(text: str) -> str:
    return re.sub(r"https?://\S+|www\.\S+", "", text or "")

def unique_comments(items: List[Tuple[str, str]]) -> List[Tuple[str, str]]:
    seen = set()
    result = []
    for platform, comment in items:
        key = (platform, comment.lower())
        if key in seen:
            continue
        seen.add(key)
        result.append((platform, comment))
    return result