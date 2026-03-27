TAG_INFO = {
    "caution": {"symbol": "⚠", "meaning": "He might be a cheater"},
    "zero": {"symbol": "○", "meaning": "Member of Server Zero"},
    "admin": {"symbol": "♛", "meaning": "What's there to explain?"},
    "zako": {"symbol": "☠", "meaning": "He can only do def"},
}

TAG_SYMBOLS = {tag: info["symbol"] for tag, info in TAG_INFO.items()}

ALLOWED_TAGS = tuple(TAG_SYMBOLS.keys())


def get_tag_symbol(tag: str | None) -> str:
    if not tag:
        return ""
    return TAG_SYMBOLS.get(str(tag).strip().lower(), "")


def get_tag_meaning(tag: str | None) -> str:
    if not tag:
        return ""
    info = TAG_INFO.get(str(tag).strip().lower())
    return info["meaning"] if info else ""


def is_valid_tag(tag: str | None) -> bool:
    if not tag:
        return False
    return str(tag).strip().lower() in ALLOWED_TAGS
