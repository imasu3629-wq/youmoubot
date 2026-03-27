TAG_SYMBOLS = {
    "caution": "⚠",
    "zero": "○",
    "admin": "♛",
    "zako": "☠",
}

TAG_INFO = {
    "caution": {"symbol": "⚠", "meaning": "もしかしたらチーターかも..."},
    "zero": {"symbol": "○", "meaning": "ゼロ鯖所属"},
    "admin": {"symbol": "♛", "meaning": "アドミン"},
    "zako": {"symbol": "☠", "meaning": "def以外の仕事を与えてはいけないだろう"},
}

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
