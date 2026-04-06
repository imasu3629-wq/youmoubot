import io
import os
import json
import base64
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from PIL import Image, ImageDraw, ImageFilter, ImageFont

REQUEST_TIMEOUT = 10
HEAD_CACHE_DIR = os.path.join("cache", "skins")
SESSION_PROFILE_URL = "https://sessionserver.mojang.com/session/minecraft/profile/{uuid}"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR_PATH = Path(__file__).resolve().parent
TAG_ICON_DIR = BASE_DIR_PATH / "assets" / "tag_icons"
STATS_BACKGROUND_PATH = BASE_DIR_PATH / "assets" / "Background.PNG"
STATS_BACKGROUND_BLUR_RADIUS = 8
TAG_ICON_FILENAME_MAP = {
    "account": "Account.PNG",
    "blatant_cheater": "Blatant_Cheater.PNG",
    "caution": "Caution.PNG",
    "closet_cheater": "Closet_Cheater.PNG",
    "confirmed_cheater": "Confirmed_Cheater.PNG",
    "info": "Info.PNG",
    "legit_sniper": "Legit_Sniper.PNG",
    "possible_sniper": "Possible_Sniper.PNG",
    "sniper": "Sniper.PNG",
    "zero": "Zero.PNG",
}
FONT_PATH = os.path.join(BASE_DIR, "Minecraftia.ttf")
SYMBOL_FONT_PATH = os.path.join(BASE_DIR, "fonts", "symbola.ttf")
SYMBOL_Y_OFFSET = -5
logger = logging.getLogger(__name__)

MC_COLORS = {
    "black": "#000000",
    "dark_blue": "#0000AA",
    "dark_green": "#00AA00",
    "dark_aqua": "#00AAAA",
    "dark_red": "#AA0000",
    "dark_purple": "#AA00AA",
    "gold": "#FFAA00",
    "gray": "#AAAAAA",
    "dark_gray": "#555555",
    "blue": "#5555FF",
    "green": "#55FF55",
    "aqua": "#55FFFF",
    "red": "#FF5555",
    "light_purple": "#FF55FF",
    "yellow": "#FFFF55",
    "white": "#FFFFFF",
}

PRESTIGE_STYLES = {
    0: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#AAAAAA"], "symbol_color": "#AAAAAA", "right_bracket_color": "#AAAAAA"},
    100: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#FFFFFF", "#FFFFFF", "#FFFFFF"], "symbol_color": "#AAAAAA", "right_bracket_color": "#AAAAAA"},
    200: {"left_bracket_color": "#FFAA00", "digit_colors": ["#FFAA00", "#FFAA00", "#FFAA00"], "symbol_color": "#FFAA00", "right_bracket_color": "#FFAA00"},
    300: {"left_bracket_color": "#55FFFF", "digit_colors": ["#55FFFF", "#55FFFF", "#55FFFF"], "symbol_color": "#55FFFF", "right_bracket_color": "#55FFFF"},
    400: {"left_bracket_color": "#00AA00", "digit_colors": ["#00AA00", "#00AA00", "#00AA00"], "symbol_color": "#00AA00", "right_bracket_color": "#00AA00"},
    500: {"left_bracket_color": "#00AAAA", "digit_colors": ["#00AAAA", "#00AAAA", "#00AAAA"], "symbol_color": "#00AAAA", "right_bracket_color": "#00AAAA"},
    600: {"left_bracket_color": "#AA0000", "digit_colors": ["#AA0000", "#AA0000", "#AA0000"], "symbol_color": "#AA0000", "right_bracket_color": "#AA0000"},
    700: {"left_bracket_color": "#FF55FF", "digit_colors": ["#FF55FF", "#FF55FF", "#FF55FF"], "symbol_color": "#FF55FF", "right_bracket_color": "#FF55FF"},
    800: {"left_bracket_color": "#5555FF", "digit_colors": ["#5555FF", "#5555FF", "#5555FF"], "symbol_color": "#5555FF", "right_bracket_color": "#5555FF"},
    900: {"left_bracket_color": "#AA00AA", "digit_colors": ["#AA00AA", "#AA00AA", "#AA00AA"], "symbol_color": "#AA00AA", "right_bracket_color": "#AA00AA"},
    1000: {"left_bracket_color": "#FF5555", "digit_colors": ["#FFAA00", "#FFFF55", "#55FFFF", "#FF55FF"], "symbol_color": "#FF55FF", "right_bracket_color": "#AA00AA"},
    1100: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#FFFFFF", "#FFFFFF", "#FFFFFF", "#FFFFFF"], "symbol_color": "#AAAAAA", "right_bracket_color": "#AAAAAA"},
    1200: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#FFFF55", "#FFFF55", "#FFAA00", "#FFAA00"], "symbol_color": "#FFAA00", "right_bracket_color": "#AAAAAA"},
    1300: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#55FFFF", "#55FFFF", "#00AAAA", "#00AAAA"], "symbol_color": "#00AAAA", "right_bracket_color": "#AAAAAA"},
    1400: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#55FF55", "#55FF55", "#00AA00", "#00AA00"], "symbol_color": "#00AA00", "right_bracket_color": "#AAAAAA"},
    1500: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#55FFFF", "#55FFFF", "#5555FF", "#5555FF"], "symbol_color": "#5555FF", "right_bracket_color": "#AAAAAA"},
    1600: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#FF5555", "#FF5555", "#AA0000", "#AA0000"], "symbol_color": "#AA0000", "right_bracket_color": "#AAAAAA"},
    1700: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#FF55FF", "#FF55FF", "#AA00AA", "#AA00AA"], "symbol_color": "#AA00AA", "right_bracket_color": "#AAAAAA"},
    1800: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#5555FF", "#5555FF", "#0000AA", "#0000AA"], "symbol_color": "#0000AA", "right_bracket_color": "#AAAAAA"},
    1900: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#FF55FF", "#FF55FF", "#AA00AA", "#555555"], "symbol_color": "#555555", "right_bracket_color": "#AAAAAA"},
    2000: {"left_bracket_color": "#AAAAAA", "digit_colors": ["#FFFFFF", "#FFFFFF", "#FFFFFF", "#FFFFFF"], "symbol_color": "#AAAAAA", "right_bracket_color": "#AAAAAA"},
    2100: {"left_bracket_color": "#FFAA00", "digit_colors": ["#555555", "#FFFF55", "#FFFF55", "#FFAA00"], "symbol_color": "#FFAA00", "right_bracket_color": "#FFAA00"},
    2200: {"left_bracket_color": "#FFAA00", "digit_colors": ["#FFFFFF", "#FFFFFF", "#55FFFF", "#55FFFF"], "symbol_color": "#00AAAA", "right_bracket_color": "#00AAAA"},
    2300: {"left_bracket_color": "#AA00AA", "digit_colors": ["#AA00AA", "#AA00AA", "#FFFF55", "#FFFF55"], "symbol_color": "#FFFF55", "right_bracket_color": "#FFFF55"},
    2400: {"left_bracket_color": "#55FFFF", "digit_colors": ["#55FFFF", "#555555", "#FFFFFF", "#FFFFFF"], "symbol_color": "#AAAAAA", "right_bracket_color": "#555555"},
    2500: {"left_bracket_color": "#FFFFFF", "digit_colors": ["#55FF55", "#55FF55", "#55FF55", "#55FF55"], "symbol_color": "#00AA00", "right_bracket_color": "#00AA00"},
    2600: {"left_bracket_color": "#AA0000", "digit_colors": ["#AA0000", "#AA0000", "#FF55FF", "#FF55FF"], "symbol_color": "#FF55FF", "right_bracket_color": "#FF55FF"},
    2700: {"left_bracket_color": "#FFFF55", "digit_colors": ["#FFFF55", "#555555", "#555555", "#AAAAAA"], "symbol_color": "#AAAAAA", "right_bracket_color": "#555555"},
    2800: {"left_bracket_color": "#55FF55", "digit_colors": ["#55FF55", "#55FF55", "#FFAA00", "#FFAA00"], "symbol_color": "#FFAA00", "right_bracket_color": "#FFFF55"},
    2900: {"left_bracket_color": "#55FFFF", "digit_colors": ["#55FFFF", "#55FFFF", "#5555FF", "#5555FF"], "symbol_color": "#5555FF", "right_bracket_color": "#5555FF"},
    3000: {"left_bracket_color": "#FFFF55", "digit_colors": ["#FFAA00", "#FFAA00", "#FF5555", "#FF5555"], "symbol_color": "#FF5555", "right_bracket_color": "#AA0000"},
    3100: {"left_bracket_color": "#5555FF", "digit_colors": ["#5555FF", "#55FFFF", "#55FFFF", "#FFAA00"], "symbol_color": "#FFAA00", "right_bracket_color": "#FFFF55"},
    3200: {"left_bracket_color": "#AA0000", "digit_colors": ["#FFFFFF", "#FFFFFF", "#AA0000", "#AA0000"], "symbol_color": "#FF5555", "right_bracket_color": "#FF5555"},
    3300: {"left_bracket_color": "#5555FF", "digit_colors": ["#FF55FF", "#FF55FF", "#FF5555", "#FF5555"], "symbol_color": "#FF5555", "right_bracket_color": "#AA0000"},
    3400: {"left_bracket_color": "#55FF55", "digit_colors": ["#FF55FF", "#FF55FF", "#FF55FF", "#FF55FF"], "symbol_color": "#AA00AA", "right_bracket_color": "#00AA00"},
    3500: {"left_bracket_color": "#FF5555", "digit_colors": ["#FF5555", "#FF5555", "#55FF55", "#55FF55"], "symbol_color": "#55FF55", "right_bracket_color": "#55FF55"},
    3600: {"left_bracket_color": "#55FF55", "digit_colors": ["#FF55FF", "#FF55FF", "#5555FF", "#5555FF"], "symbol_color": "#5555FF", "right_bracket_color": "#0000AA"},
    3700: {"left_bracket_color": "#AA0000", "digit_colors": ["#55FFFF", "#55FFFF", "#00AAAA", "#00AAAA"], "symbol_color": "#00AAAA", "right_bracket_color": "#00AAAA"},
    3800: {"left_bracket_color": "#0000AA", "digit_colors": ["#000000", "#FF55FF", "#FF55FF", "#FF55FF"], "symbol_color": "#FF55FF", "right_bracket_color": "#0000AA"},
    3900: {"left_bracket_color": "#FF5555", "digit_colors": ["#FF5555", "#55FFFF", "#55FFFF", "#5555FF"], "symbol_color": "#5555FF", "right_bracket_color": "#5555FF"},
    4000: {"left_bracket_color": "#AA00AA", "digit_colors": ["#FF5555", "#FF5555", "#FFAA00", "#FFAA00"], "symbol_color": "#FFAA00", "right_bracket_color": "#FFFF55"},
    4100: {"left_bracket_color": "#FFFF55", "digit_colors": ["#FFFF55", "#FFFF55", "#FF55FF", "#FF55FF"], "symbol_color": "#FF55FF", "right_bracket_color": "#FF55FF"},
    4200: {"left_bracket_color": "#0000AA", "digit_colors": ["#55FFFF", "#55FFFF", "#AAAAAA", "#AAAAAA"], "symbol_color": "#AAAAAA", "right_bracket_color": "#AAAAAA"},
    4300: {"left_bracket_color": "#55FF55", "digit_colors": ["#55FF55", "#55FF55", "#FFAA00", "#FFAA00"], "symbol_color": "#AA00AA", "right_bracket_color": "#FF55FF"},
    4400: {"left_bracket_color": "#55FF55", "digit_colors": ["#55FF55", "#FFAA00", "#FFAA00", "#FF55FF"], "symbol_color": "#FF55FF", "right_bracket_color": "#FF55FF"},
    4500: {"left_bracket_color": "#FFFFFF", "digit_colors": ["#FFFFFF", "#00AAAA", "#00AAAA", "#55FFFF"], "symbol_color": "#00AAAA", "right_bracket_color": "#55FFFF"},
    4600: {"left_bracket_color": "#00AAAA", "digit_colors": ["#00AAAA", "#FFFF55", "#FFAA00", "#FFAA00"], "symbol_color": "#FF55FF", "right_bracket_color": "#FF55FF"},
    4700: {"left_bracket_color": "#FFFFFF", "digit_colors": ["#FF5555", "#FF5555", "#0000AA", "#0000AA"], "symbol_color": "#0000AA", "right_bracket_color": "#5555FF"},
    4800: {"left_bracket_color": "#AA00AA", "digit_colors": ["#FF5555", "#FFAA00", "#55FFFF", "#55FFFF"], "symbol_color": "#55FFFF", "right_bracket_color": "#00AAAA"},
    4900: {"left_bracket_color": "#55FF55", "digit_colors": ["#FFFFFF", "#FFFFFF", "#FFFFFF", "#55FF55"], "symbol_color": "#55FF55", "right_bracket_color": "#55FF55"},
    5000: {"left_bracket_color": "#AA0000", "digit_colors": ["#FF5555", "#AA00AA", "#5555FF", "#5555FF"], "symbol_color": "#0000AA", "right_bracket_color": "#000000"},
}


def get_star_symbol(star: int) -> str:
    if star < 1100:
        return "✫"
    elif star < 2100:
        return "✪"
    elif star < 3100:
        return "⚝"
    else:
        return "✥"


def get_prestige_key(star: int) -> int:
    if star >= 5000:
        return 5000
    return (star // 100) * 100


def get_prestige_style(star: int) -> dict[str, Any]:
    prestige_key = get_prestige_key(star)
    if prestige_key in PRESTIGE_STYLES:
        return PRESTIGE_STYLES[prestige_key]

    fallback_key = max((key for key in PRESTIGE_STYLES if key <= prestige_key), default=0)
    return PRESTIGE_STYLES.get(fallback_key, PRESTIGE_STYLES[0])


def draw_star_text(
    draw: ImageDraw.ImageDraw,
    x: int,
    y: int,
    star: int,
    digit_font: ImageFont.ImageFont,
    symbol_font: ImageFont.ImageFont,
    style: dict[str, Any],
) -> int:
    safe_star = max(star, 0)
    symbol = get_star_symbol(safe_star)
    current_x = x

    draw.text((current_x, y), "[", font=digit_font, fill=style["left_bracket_color"])
    bbox = draw.textbbox((current_x, y), "[", font=digit_font)
    current_x = bbox[2]

    digits = list(str(safe_star))
    digit_colors = style["digit_colors"]
    for i, ch in enumerate(digits):
        color = digit_colors[min(i, len(digit_colors) - 1)]
        draw.text((current_x, y), ch, font=digit_font, fill=color)
        bbox = draw.textbbox((current_x, y), ch, font=digit_font)
        current_x = bbox[2]

    symbol_bbox = draw.textbbox((current_x, y), symbol, font=symbol_font)
    digit_bbox = draw.textbbox((0, 0), "0", font=digit_font)
    digit_h = digit_bbox[3] - digit_bbox[1]
    sym_h = symbol_bbox[3] - symbol_bbox[1]
    symbol_y = y + int((digit_h - sym_h) / 2) + SYMBOL_Y_OFFSET

    draw.text((current_x, symbol_y), symbol, font=symbol_font, fill=style["symbol_color"])
    current_x = symbol_bbox[2]

    draw.text((current_x, y), "]", font=digit_font, fill=style["right_bracket_color"])
    bbox = draw.textbbox((current_x, y), "]", font=digit_font)
    current_x = bbox[2]

    return current_x


RANKING_META = {
    "fkdr": {"title": "Bedwars FKDR Ranking", "label": "FKDR", "decimals": 2},
    "wins": {"title": "Bedwars Wins Ranking", "label": "Wins", "decimals": 0},
    "star": {"title": "Bedwars Star Ranking", "label": "Star", "decimals": 0},
    "bblr": {"title": "Bedwars BBLR Ranking", "label": "BBLR", "decimals": 2},
    "wlr": {"title": "Bedwars WLR Ranking", "label": "WLR", "decimals": 2},
    "kdr": {"title": "Bedwars KDR Ranking", "label": "KDR", "decimals": 2},
    "final_kills": {"title": "Bedwars Final Kills Ranking", "label": "Final Kills", "decimals": 0},
    "beds_broken": {"title": "Bedwars Beds Broken Ranking", "label": "Beds Broken", "decimals": 0},
    "winstreak": {"title": "Bedwars Winstreak Ranking", "label": "Winstreak", "decimals": 0},
}


class RankingRenderError(Exception):
    pass


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _bedwars_exp_to_next_star_progress(total_experience: Any) -> float | None:
    try:
        exp = max(int(total_experience), 0)
    except (TypeError, ValueError):
        return None

    exp_in_prestige = exp % 482_000
    level_thresholds = (500, 1_500, 3_500, 7_000)

    if exp_in_prestige < level_thresholds[0]:
        return exp_in_prestige / 500.0
    if exp_in_prestige < level_thresholds[1]:
        return (exp_in_prestige - level_thresholds[0]) / 1_000.0
    if exp_in_prestige < level_thresholds[2]:
        return (exp_in_prestige - level_thresholds[1]) / 2_000.0
    if exp_in_prestige < level_thresholds[3]:
        return (exp_in_prestige - level_thresholds[2]) / 3_500.0

    return ((exp_in_prestige - level_thresholds[3]) % 5_000) / 5_000.0


def _dig_value(payload: Any, paths: list[tuple[str, ...]]) -> Any:
    if not isinstance(payload, dict):
        return None
    for path in paths:
        current = payload
        found = True
        for key in path:
            if not isinstance(current, dict) or key not in current:
                found = False
                break
            current = current[key]
        if found and current is not None:
            return current
    return None


def format_number(value: Any) -> str:
    if value is None:
        return "N/A"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return "N/A"


def calculate_bblr(beds_broken: int | None, beds_lost: int | None) -> float:
    broken = _safe_int(beds_broken)
    lost = _safe_int(beds_lost)
    if lost > 0:
        return broken / lost
    if broken > 0:
        return float(broken)
    return 0.0


def _load_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    if os.path.exists(FONT_PATH):
        try:
            return ImageFont.truetype(FONT_PATH, size=size)
        except OSError:
            pass
    return ImageFont.load_default()


def _load_symbol_font(size: int) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    print(f"[ranking_renderer] symbol font path = {SYMBOL_FONT_PATH}")
    if os.path.exists(SYMBOL_FONT_PATH):
        try:
            return ImageFont.truetype(SYMBOL_FONT_PATH, size=size)
        except OSError as error:
            print(f"[ranking_renderer] failed to load symbol font: {error}")
    else:
        print("[ranking_renderer] symbol font file does not exist")
    return ImageFont.load_default()


def _extract_head_from_skin_bytes(skin_bytes: bytes) -> str | None:
    try:
        skin = Image.open(io.BytesIO(skin_bytes)).convert("RGBA")
    except OSError:
        return None

    if skin.width < 64 or skin.height < 32:
        return None

    base_layer = skin.crop((8, 8, 16, 16)).convert("RGBA")
    hat_layer = skin.crop((40, 8, 48, 16)).convert("RGBA")
    base_layer.alpha_composite(hat_layer)

    upscaled = base_layer.resize((64, 64), Image.Resampling.NEAREST)

    output = io.BytesIO()
    upscaled.save(output, format="PNG")
    return base64.b64encode(output.getvalue()).decode("utf-8")


def fetch_head_base64_from_uuid(uuid: str) -> str | None:
    if not uuid:
        return None
    try:
        session_response = requests.get(SESSION_PROFILE_URL.format(uuid=uuid), timeout=REQUEST_TIMEOUT)
        if session_response.status_code != 200:
            return None
        profile = session_response.json()
        textures_prop = next(
            (prop for prop in profile.get("properties", []) if prop.get("name") == "textures"),
            None,
        )
        if not textures_prop or not textures_prop.get("value"):
            return None
        decoded = base64.b64decode(textures_prop["value"]).decode("utf-8")
        textures_payload = json.loads(decoded)
        skin_url = (((textures_payload.get("textures") or {}).get("SKIN")) or {}).get("url")
        if not skin_url:
            return None
        skin_response = requests.get(skin_url, timeout=REQUEST_TIMEOUT)
        if skin_response.status_code != 200:
            return None
        return _extract_head_from_skin_bytes(skin_response.content)
    except requests.RequestException:
        return None
    except (ValueError, json.JSONDecodeError, base64.binascii.Error):
        return None


def _load_head_image(head_image_base64: str | None) -> Image.Image:
    if head_image_base64:
        try:
            raw = base64.b64decode(head_image_base64)
            head = Image.open(io.BytesIO(raw)).convert("RGBA")
            if head.width <= 16 and head.height <= 16:
                return head.resize((64, 64), Image.Resampling.NEAREST)
            return head
        except (ValueError, OSError, base64.binascii.Error):
            pass
    fallback = Image.new("RGBA", (64, 64), (60, 60, 60, 255))
    fallback_draw = ImageDraw.Draw(fallback)
    fallback_draw.rectangle((12, 12, 52, 52), fill=(110, 110, 110, 255))
    return fallback


def _normalize_raw_tag_to_key(raw_tag: str | None) -> str | None:
    value = str(raw_tag or "").strip().lower()
    if not value:
        return None
    return value.replace(" ", "_").replace("-", "_")


def resolve_tag_icon_path(raw_tag: str | None) -> Path | None:
    logger.info("Raw tag from API: %s", raw_tag)
    normalized_tag = _normalize_raw_tag_to_key(raw_tag)
    if not normalized_tag:
        logger.info("Resolved tag icon filename: %s", None)
        return None

    filename = TAG_ICON_FILENAME_MAP.get(normalized_tag)
    logger.info("Resolved tag icon filename: %s", filename)
    if not filename:
        return None

    icon_path = TAG_ICON_DIR / filename
    logger.info("Resolved tag icon path: %s", icon_path)
    logger.info("Tag icon exists: %s", icon_path.exists())
    if not icon_path.exists():
        return None
    return icon_path


def _load_tag_icon(raw_tag: str | None, icon_size: int) -> Image.Image | None:
    icon_path = resolve_tag_icon_path(raw_tag)
    if not icon_path:
        return None

    try:
        icon = Image.open(icon_path).convert("RGBA")
        icon = icon.resize((icon_size, icon_size), Image.Resampling.LANCZOS)
        logger.info("Loaded tag icon successfully: %s", icon_path)
        return icon
    except OSError as exc:
        filename = icon_path.name
        logger.warning(
            "Failed to load tag icon: raw_tag=%s filename=%s path=%s exists=%s error=%s",
            raw_tag,
            filename,
            icon_path,
            icon_path.exists(),
            exc,
        )
        return None


def _get_display_tag_payload(row: Any) -> dict[str, str] | None:
    payload = row.get("display_tag")
    if isinstance(payload, dict):
        tag_name = str(payload.get("tag") or "").strip()
        source = str(payload.get("source") or "").strip()
        if tag_name:
            return {"tag": tag_name, "source": source}
    selected_tag = row.get("urchin_tag")
    if isinstance(selected_tag, dict):
        raw_tag = str(selected_tag.get("tag") or "").strip()
        if raw_tag:
            return {"tag": raw_tag, "source": "urchin"}
    return None


def _extract_metric_value(row: Any, metric: str) -> float:
    if metric == "star":
        return float(_safe_int(row["bedwars_star"]))
    if metric == "bblr":
        return calculate_bblr(row.get("beds_broken"), row.get("beds_lost"))
    if metric in ("fkdr", "wlr", "kdr"):
        return _safe_float(row[metric])
    return float(_safe_int(row[metric]))


def _format_metric_value(value: float, decimals: int) -> str:
    if decimals == 0:
        return str(int(value))
    return f"{value:.2f}"


def _measure_text_width(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.ImageFont) -> int:
    if not text:
        return 0
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _wrap_text_by_width(
    draw: ImageDraw.ImageDraw,
    text: str,
    font: ImageFont.ImageFont,
    max_width: int,
) -> list[str]:
    value = str(text or "").strip()
    if not value:
        return []

    words = value.split()
    wrapped_lines: list[str] = []
    current_line = ""

    for word in words:
        tentative = word if not current_line else f"{current_line} {word}"
        if _measure_text_width(draw, tentative, font) <= max_width:
            current_line = tentative
            continue

        if current_line:
            wrapped_lines.append(current_line)
            current_line = ""

        if _measure_text_width(draw, word, font) <= max_width:
            current_line = word
            continue

        chunk = ""
        for ch in word:
            tentative_chunk = f"{chunk}{ch}"
            if _measure_text_width(draw, tentative_chunk, font) <= max_width:
                chunk = tentative_chunk
            else:
                if chunk:
                    wrapped_lines.append(chunk)
                chunk = ch
        current_line = chunk

    if current_line:
        wrapped_lines.append(current_line)
    return wrapped_lines


def _latest_timestamp(rows: list[Any]) -> str:
    latest = None
    for row in rows:
        raw = row["last_updated"]
        if not raw:
            continue
        try:
            parsed = datetime.fromisoformat(str(raw).replace("Z", ""))
            if latest is None or parsed > latest:
                latest = parsed
        except ValueError:
            continue
    if not latest:
        return "Updated: N/A"
    return latest.strftime("Updated: %Y-%m-%d %H:%M")


def render_ranking_image(rows: list[Any], metric: str, *, show_title: bool = True, rank_start: int = 1) -> io.BytesIO:
    if metric not in RANKING_META:
        raise RankingRenderError("Unsupported ranking metric")

    width = 1000
    top_padding = 120 if show_title else 70
    height = top_padding + (len(rows) * 64) + 70
    image = Image.new("RGBA", (width, height), (10, 10, 10, 255))
    draw = ImageDraw.Draw(image)

    title_font = _load_font(28)
    header_font = _load_font(18)
    body_font = _load_font(20)
    badge_font = _load_font(20)
    symbol_font = _load_symbol_font(20)
    footer_font = _load_font(14)

    meta = RANKING_META[metric]
    if show_title:
        draw.text((40, 25), meta["title"], font=title_font, fill="#FFFFFF")
        header_y = 75
    else:
        header_y = 25
    draw.text((40, header_y), "Rank", font=header_font, fill="#AAAAAA")
    draw.text((140, header_y), "Player", font=header_font, fill="#AAAAAA")
    draw.text((560, header_y), meta["label"], font=header_font, fill="#AAAAAA")

    row_start_y = header_y + 30
    for idx, row in enumerate(rows, start=1):
        y = row_start_y + (idx - 1) * 64

        draw.rounded_rectangle((30, y - 4, 970, y + 50), radius=8, fill=(18, 18, 18, 255))
        draw.text((40, y + 8), f"#{rank_start + idx - 1}", font=body_font, fill="#FFFFFF")

        head = _load_head_image(row["head_image_base64"]).resize((32, 32), Image.Resampling.NEAREST)
        image.paste(head, (90, y + 5), head)

        star = _safe_int(row["bedwars_star"])
        draw_star_text(
            draw,
            380,
            y + 8,
            star,
            badge_font,
            symbol_font,
            get_prestige_style(max(star, 0)),
        )

        name = str(row["minecraft_name"] or "Unknown")
        name_x = 140
        name_y = y + 8
        draw.text((name_x, name_y), name, font=body_font, fill="#FFFFFF")
        display_tag = _get_display_tag_payload(row)
        icon = _load_tag_icon((display_tag or {}).get("tag"), 20)
        if icon:
            name_bbox = draw.textbbox((name_x, name_y), name, font=body_font)
            name_height = name_bbox[3] - name_bbox[1]
            icon_y = name_y + max(int((name_height - icon.height) / 2), 0)
            image.paste(icon, (name_bbox[2] + 8, icon_y), icon)

        metric_value = _extract_metric_value(row, metric)
        value_text = _format_metric_value(metric_value, meta["decimals"])
        draw.text((560, y + 8), value_text, font=body_font, fill="#55FFFF")

    draw.text((40, height - 35), _latest_timestamp(rows), font=footer_font, fill="#AAAAAA")

    output = io.BytesIO()
    image.save(output, format="PNG")
    output.seek(0)
    return output


def render_stats_image(row: Any) -> io.BytesIO:
    width, height = 1280, 720
    display_tag = _get_display_tag_payload(row)
    display_tag_name = (display_tag or {}).get("tag")
    raw_stats = row.get("raw_flashlight_json") if isinstance(row.get("raw_flashlight_json"), dict) else {}
    bedwars_blob = _dig_value(
        raw_stats,
        [("stats", "bedwars"), ("stats", "Bedwars"), ("player", "stats", "Bedwars"), ("playerData", "stats", "bedwars")],
    ) or {}

    font_y_offsets = {
        16: -1,
        22: -2,
        28: -2,
        30: -2,
    }

    def _font_y_offset(font: ImageFont.ImageFont) -> int:
        return font_y_offsets.get(getattr(font, "size", 0), 0)

    def _text_size(
        d: ImageDraw.ImageDraw,
        text: str,
        font: ImageFont.ImageFont,
    ) -> tuple[int, int, tuple[int, int, int, int]]:
        bbox = d.textbbox((0, 0), text, font=font)
        return bbox[2] - bbox[0], bbox[3] - bbox[1], bbox

    def draw_text_with_shadow(
        d: ImageDraw.ImageDraw,
        position: tuple[int, int],
        text: str,
        font: ImageFont.ImageFont,
        fill: tuple[int, int, int] = (235, 235, 235),
    ) -> None:
        x, y = position
        y += _font_y_offset(font)
        d.text((x + 2, y + 2), text, font=font, fill=(0, 0, 0, 140))
        d.text((x, y), text, font=font, fill=fill)

    def draw_centered_text(
        d: ImageDraw.ImageDraw,
        rect: tuple[int, int, int, int],
        text: str,
        font: ImageFont.ImageFont,
        fill: tuple[int, int, int] = (235, 235, 235),
        y_offset: int = 0,
    ) -> None:
        rx1, ry1, rx2, ry2 = rect
        text_w, text_h, _ = _text_size(d, text, font)
        x = rx1 + (rx2 - rx1 - text_w) // 2
        y = ry1 + (ry2 - ry1 - text_h) // 2 + y_offset
        draw_text_with_shadow(d, (x, y), text, font, fill)

    def draw_left_aligned_text(
        d: ImageDraw.ImageDraw,
        rect: tuple[int, int, int, int],
        text: str,
        font: ImageFont.ImageFont,
        fill: tuple[int, int, int] = (235, 235, 235),
        left_padding: int = 0,
        y_offset: int = 0,
    ) -> None:
        rx1, ry1, _, ry2 = rect
        _, text_h, _ = _text_size(d, text, font)
        x = rx1 + left_padding
        y = ry1 + (ry2 - ry1 - text_h) // 2 + y_offset
        draw_text_with_shadow(d, (x, y), text, font, fill)

    panel_radius = 18
    panel_fill = (8, 12, 18, 190)
    panel_outline = (135, 155, 180, 100)

    def draw_rounded_panel(d: ImageDraw.ImageDraw, rect: tuple[int, int, int, int]) -> None:
        d.rounded_rectangle(rect, radius=panel_radius, fill=panel_fill, outline=panel_outline, width=2)

    def draw_stat_box(d: ImageDraw.ImageDraw, rect: tuple[int, int, int, int]) -> None:
        d.rounded_rectangle(rect, radius=14, fill=(18, 22, 30, 185), outline=(100, 120, 150, 60), width=1)

    def draw_centered_at(
        d: ImageDraw.ImageDraw,
        center_x: int,
        y: int,
        text: str,
        font: ImageFont.ImageFont,
        fill: tuple[int, int, int] = (235, 235, 235),
    ) -> None:
        text_w, _, _ = _text_size(d, text, font)
        draw_text_with_shadow(d, (center_x - (text_w // 2), y), text, font, fill)

    def paste_centered_image(base: Image.Image, image_to_paste: Image.Image, rect: tuple[int, int, int, int]) -> None:
        rx1, ry1, rx2, ry2 = rect
        rw = rx2 - rx1
        rh = ry2 - ry1
        scale = min(rw / image_to_paste.width, rh / image_to_paste.height)
        new_size = (max(1, int(image_to_paste.width * scale)), max(1, int(image_to_paste.height * scale)))
        resized = image_to_paste.resize(new_size, Image.Resampling.LANCZOS)
        px = rx1 + (rw - resized.width) // 2
        py = ry1 + (rh - resized.height) // 2
        base.paste(resized, (px, py), resized if resized.mode == "RGBA" else None)

    def draw_progress_bar(d: ImageDraw.ImageDraw, x: int, y: int, w: int, h: int, progress: float) -> None:
        d.rounded_rectangle((x, y, x + w, y + h), radius=9, fill=(28, 34, 44, 205), outline=(120, 140, 170, 70), width=1)
        inner_w = max(0, min(w - 4, int((w - 4) * max(0.0, min(progress, 1.0)))))
        if inner_w > 0:
            d.rounded_rectangle((x + 2, y + 2, x + 2 + inner_w, y + h - 2), radius=7, fill=(70, 220, 255, 220))

    def safe_ratio_text(value: Any) -> str:
        if value is None:
            return "N/A"
        try:
            return f"{float(value):.2f}"
        except (TypeError, ValueError):
            return "N/A"

    def get_stat(paths: list[tuple[str, ...]], fallback_key: str | None = None) -> Any:
        value = _dig_value(bedwars_blob, paths)
        if value is None and fallback_key:
            value = row.get(fallback_key)
        return value

    def _percent_progress(value: Any) -> float:
        try:
            numeric = float(value or 0)
        except (TypeError, ValueError):
            numeric = 0.0
        if numeric > 1:
            numeric = numeric / 100.0
        return max(0.0, min(numeric, 1.0))

    resolved_background_path = STATS_BACKGROUND_PATH.resolve()
    logger.info("Stats background resolved path: %s", resolved_background_path)
    logger.info("Stats background exists: %s", resolved_background_path.exists())
    try:
        background = Image.open(resolved_background_path).convert("RGBA")
        logger.info("Stats background loaded size: %sx%s", background.width, background.height)
        scale = max(width / background.width, height / background.height)
        bg_size = (int(background.width * scale), int(background.height * scale))
        background = background.resize(bg_size, Image.Resampling.LANCZOS)
        crop_x = (background.width - width) // 2
        crop_y = (background.height - height) // 2
        background = background.crop((crop_x, crop_y, crop_x + width, crop_y + height))
        background = background.filter(ImageFilter.GaussianBlur(radius=STATS_BACKGROUND_BLUR_RADIUS))
        canvas = background
    except Exception as exc:
        logger.exception(
            "Failed loading stats background from %s: %s",
            resolved_background_path,
            exc,
        )
        canvas = Image.new("RGBA", (width, height), (20, 24, 32, 255))
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay, "RGBA")
    # Keep panel readability while preserving the background details.
    draw.rectangle((0, 0, width, height), fill=(0, 0, 0, 80))

    title_font = _load_font(42)
    value_font = _load_font(32)
    body_font = _load_font(22)
    small_font = _load_font(18)
    badge_font = _load_font(30)
    symbol_font = _load_symbol_font(30)

    # Header panel
    draw_rounded_panel(draw, (20, 20, 1260, 120))
    head = _load_head_image(row.get("head_image_base64")).resize((60, 60), Image.Resampling.NEAREST)
    overlay.paste(head, (35, 35), head)
    draw_text_with_shadow(draw, (110, 32), "Star", body_font, (190, 195, 205))
    player_name = str(row.get("minecraft_name") or "Unknown")
    draw_text_with_shadow(draw, (245, 31), "mcid", _load_font(14), (190, 195, 205))
    username_font = title_font
    max_name_width = 900 - 325
    while getattr(username_font, "size", 0) > 20 and _measure_text_width(draw, player_name, username_font) > max_name_width:
        username_font = _load_font(getattr(username_font, "size", 42) - 2)
    if getattr(username_font, "size", 0) > 26:
        username_font = _load_font(26)
    draw_text_with_shadow(draw, (325, 30), player_name, username_font, (235, 235, 235))
    star = _safe_int(row.get("bedwars_star"))
    draw_star_text(draw, 115, 68, star, badge_font, symbol_font, get_prestige_style(max(star, 0)))

    total_experience = _dig_value(bedwars_blob, [("Experience",), ("experience",), ("Exp",), ("exp",)])
    xp_progress = _bedwars_exp_to_next_star_progress(total_experience)
    if xp_progress is None:
        xp_progress = _dig_value(bedwars_blob, [("level_progress",), ("xp_progress",), ("progress",)])
    draw_progress_bar(draw, 360, 82, 740, 18, _percent_progress(xp_progress))
    draw_text_with_shadow(draw, (350, 54), "0%", small_font, (70, 220, 255))
    draw_text_with_shadow(draw, (1110, 54), "100%", small_font, (70, 220, 255))
    tag_icon = _load_tag_icon(display_tag_name, 48)
    if tag_icon:
        overlay.paste(tag_icon, (1200, 46), tag_icon)

    # Left skin panel
    draw_rounded_panel(draw, (20, 140, 320, 490))
    skin_rect = (45, 200, 295, 450)
    skin_img = _load_head_image(row.get("head_image_base64")).resize((250, 250), Image.Resampling.NEAREST)
    paste_centered_image(overlay, skin_img, skin_rect)

    # Ratios panel
    draw_rounded_panel(draw, (340, 140, 1260, 270))
    draw_text_with_shadow(draw, (360, 152), "Ratios", body_font)
    ratio_specs = [
        ("FKDR", 360, safe_ratio_text(row.get("fkdr")), (190, 110, 255)),
        ("WLR", 580, safe_ratio_text(row.get("wlr")), (190, 110, 255)),
        ("KDR", 800, safe_ratio_text(row.get("kdr")), (255, 195, 40)),
        ("BBLR", 1020, f"{calculate_bblr(get_stat([('beds_broken',), ('beds_broken_bedwars',)], 'beds_broken'), get_stat([('beds_lost',), ('beds_lost_bedwars',)], 'beds_lost')):.2f}", (190, 110, 255)),
    ]
    ratio_centers = [462, 682, 902, 1122]
    for idx, (label, box_x, value, color) in enumerate(ratio_specs):
        draw_stat_box(draw, (box_x, 185, box_x + 205, 247))
        draw_centered_at(draw, ratio_centers[idx], 197, label, small_font, (190, 195, 205))
        draw_centered_at(draw, ratio_centers[idx], 222, value, body_font, color)

    # Career stats panel
    draw_rounded_panel(draw, (340, 285, 1260, 455))
    draw_text_with_shadow(draw, (360, 297), "Career Stats", body_font)
    combat_specs = [
        ("Wins", "wins", (92, 255, 110), 360, 320, 332, 354),
        ("Losses", "losses", (255, 95, 95), 580, 320, 332, 354),
        ("Finals", "final_kills", (92, 255, 110), 800, 320, 332, 354),
        ("Final Deaths", "final_deaths", (255, 95, 95), 1020, 320, 332, 354),
        ("Beds Broken", "beds_broken", (92, 255, 110), 360, 395, 407, 429),
        ("Beds Lost", "beds_lost", (255, 95, 95), 580, 395, 407, 429),
        ("Kills", "kills", (92, 255, 110), 800, 395, 407, 429),
        ("Deaths", "deaths", (255, 95, 95), 1020, 395, 407, 429),
    ]
    center_by_x = {360: 462, 580: 682, 800: 902, 1020: 1122}
    for label, key, color, x, y, label_y, value_y in combat_specs:
        value = format_number(get_stat([(key,), (f"{key}_bedwars",)], key))
        draw_stat_box(draw, (x, y, x + 205, y + 58))
        center_x = center_by_x[x]
        draw_centered_at(draw, center_x, label_y, label, small_font, (190, 195, 205))
        draw_centered_at(draw, center_x, value_y, value, small_font if len(value) > 9 else body_font, color)

    # Bottom panels
    draw_rounded_panel(draw, (20, 500, 320, 690))
    draw_text_with_shadow(draw, (35, 515), "Winstreak", body_font, (190, 195, 205))
    draw_centered_at(draw, 170, 620, format_number(row.get("winstreak")), value_font, (255, 195, 40))

    draw_rounded_panel(draw, (340, 500, 700, 690))
    draw_text_with_shadow(draw, (360, 515), "Tag", body_font, (190, 195, 205))
    urchin_title = display_tag_name or "N/A"
    tag_icon_large = _load_tag_icon(display_tag_name, 88)
    if tag_icon_large:
        overlay.paste(tag_icon_large, (520 - (tag_icon_large.width // 2), 610 - (tag_icon_large.height // 2)), tag_icon_large)
    else:
        draw_centered_at(draw, 520, 610, urchin_title, value_font, (190, 110, 255))

    draw_rounded_panel(draw, (720, 500, 1260, 690))
    draw_text_with_shadow(draw, (740, 515), "Information", body_font, (190, 195, 205))
    updated = row.get("last_updated")
    try:
        updated_dt = datetime.fromisoformat(str(updated).replace("Z", ""))
        updated_text = updated_dt.strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        updated_text = "N/A"
    draw_text_with_shadow(draw, (740, 595), f"Updated: {updated_text}", small_font, (235, 235, 235))
    source_value = str((display_tag or {}).get("source") or "")
    source_text = "Source: Urchin" if source_value.lower() == "urchin" else "Source: Manual"
    draw_text_with_shadow(draw, (740, 630), source_text, small_font, (190, 195, 205))

    final_canvas = Image.alpha_composite(canvas, overlay)
    output = io.BytesIO()
    final_canvas.save(output, format="PNG")
    output.seek(0)
    return output
