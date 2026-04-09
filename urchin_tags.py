import os
from datetime import datetime
from typing import Any

import requests

URCHIN_API_KEY = "fead37cf-b662-4ec5-a85f-ec29d20569c7"
URCHIN_URL_TEMPLATE = "https://urchin.ws/player/{name}"
URCHIN_TIMEOUT = 8

PRIORITY = [
    "Confirmed Cheater",
    "Blatant Cheater",
    "Closet Cheater",
    "Sniper",
    "Possible Sniper",
    "Legit Sniper",
    "Caution",
    "Account",
    "Info",
]

ICON_MAP = {
    "Confirmed Cheater": os.path.join("assets", "tag_icons", "Confirmed Cheater.PNG"),
    "Blatant Cheater": os.path.join("assets", "tag_icons", "Blatant Cheater.PNG"),
    "Closet Cheater": os.path.join("assets", "tag_icons", "Closet Cheater.PNG"),
    "Sniper": os.path.join("assets", "tag_icons", "Sniper.PNG"),
    "Possible Sniper": os.path.join("assets", "tag_icons", "Possible Sniper.PNG"),
    "Legit Sniper": os.path.join("assets", "tag_icons", "Legit Sniper.PNG"),
    "Caution": os.path.join("assets", "tag_icons", "Caution.PNG"),
    "Account": os.path.join("assets", "tag_icons", "Account.PNG"),
    "Info": os.path.join("assets", "tag_icons", "Info.PNG"),
}




def _normalize_tag_entry(raw: Any) -> dict[str, str] | None:
    if not isinstance(raw, dict):
        return None

    tag_name = str(raw.get("tag") or raw.get("name") or raw.get("type") or "").strip()
    if not tag_name:
        return None

    reason = str(raw.get("reason") or raw.get("description") or "").strip()
    added_on = str(raw.get("added_on") or raw.get("addedOn") or raw.get("created_at") or "").strip()

    return {
        "tag": tag_name,
        "reason": reason,
        "added_on": added_on,
    }



def _extract_tag_entries(payload: Any) -> list[dict[str, str]]:
    if not isinstance(payload, dict):
        return []

    candidates = [
        payload.get("tags"),
        payload.get("data"),
        (payload.get("player") or {}).get("tags") if isinstance(payload.get("player"), dict) else None,
        (payload.get("result") or {}).get("tags") if isinstance(payload.get("result"), dict) else None,
    ]

    extracted: list[dict[str, str]] = []
    for candidate in candidates:
        if isinstance(candidate, list):
            for item in candidate:
                normalized = _normalize_tag_entry(item)
                if normalized:
                    extracted.append(normalized)
            if extracted:
                return extracted
    return []



def fetch_urchin_tags(player_name: str) -> list[dict[str, str]]:
    normalized_name = str(player_name or "").strip()
    if not normalized_name:
        return []

    try:
        response = requests.get(
            URCHIN_URL_TEMPLATE.format(name=normalized_name),
            params={
                "key": URCHIN_API_KEY,
                "sources": "GAME,CHAT,MANUAL",
            },
            timeout=URCHIN_TIMEOUT,
        )
        if response.status_code != 200:
            return []
        payload = response.json()
    except (requests.RequestException, ValueError):
        return []

    return _extract_tag_entries(payload)



def get_highest_priority_urchin_tag(tags: list[dict[str, str]]) -> dict[str, str] | None:
    if not tags:
        return None

    normalized_to_entry: dict[str, dict[str, str]] = {}
    for entry in tags:
        tag_name = str(entry.get("tag") or "").strip()
        if not tag_name:
            continue
        normalized_to_entry[tag_name.lower()] = {
            "tag": tag_name,
            "reason": str(entry.get("reason") or "").strip(),
            "added_on": str(entry.get("added_on") or "").strip(),
        }

    for tag_name in PRIORITY:
        match = normalized_to_entry.get(tag_name.lower())
        if match:
            return match

    return next(iter(normalized_to_entry.values()), None)



def get_urchin_icon_path(tag_name: str) -> str | None:
    normalized_name = str(tag_name or "").strip().lower()
    if not normalized_name:
        return None
    for name, path in ICON_MAP.items():
        if name.lower() == normalized_name:
            return path
    return None



def format_urchin_added_on_date(added_on: str) -> str:
    value = str(added_on or "").strip()
    if not value:
        return "Unknown"

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
        return parsed.strftime("%Y-%m-%d")
    except ValueError:
        pass

    if len(value) >= 10:
        return value[:10]
    return "Unknown"
