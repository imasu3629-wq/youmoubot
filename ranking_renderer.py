import io
import os
import json
import base64
from datetime import datetime
from typing import Any

import requests
from PIL import Image, ImageDraw, ImageFont

from tags import TAG_INFO, get_tag_symbol

REQUEST_TIMEOUT = 10
HEAD_CACHE_DIR = os.path.join("cache", "skins")
SESSION_PROFILE_URL = "https://sessionserver.mojang.com/session/minecraft/profile/{uuid}"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FONT_PATH = os.path.join(BASE_DIR, "Minecraftia.ttf")
SYMBOL_FONT_PATH = os.path.join(BASE_DIR, "fonts", "symbola.ttf")
SYMBOL_Y_OFFSET = -5

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


def _extract_tags(raw_tag: Any) -> list[str]:
    if raw_tag is None:
        return []
    if isinstance(raw_tag, str):
        return [raw.strip().lower() for raw in raw_tag.split(",") if raw.strip()]
    if isinstance(raw_tag, (list, tuple, set)):
        normalized = []
        for raw in raw_tag:
            value = str(raw).strip().lower()
            if value:
                normalized.append(value)
        return normalized
    value = str(raw_tag).strip().lower()
    return [value] if value else []


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

    output = io.BytesIO()
    base_layer.save(output, format="PNG")
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
            return Image.open(io.BytesIO(raw)).convert("RGBA")
        except (ValueError, OSError, base64.binascii.Error):
            pass
    fallback = Image.new("RGBA", (64, 64), (60, 60, 60, 255))
    fallback_draw = ImageDraw.Draw(fallback)
    fallback_draw.rectangle((12, 12, 52, 52), fill=(110, 110, 110, 255))
    return fallback


def _extract_metric_value(row: Any, metric: str) -> float:
    if metric == "star":
        return float(_safe_int(row["bedwars_star"]))
    if metric in ("fkdr", "wlr", "kdr"):
        return _safe_float(row[metric])
    return float(_safe_int(row[metric]))


def _format_metric_value(value: float, decimals: int) -> str:
    if decimals == 0:
        return str(int(value))
    return f"{value:.2f}"


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


def render_ranking_image(rows: list[Any], metric: str) -> io.BytesIO:
    if metric not in RANKING_META:
        raise RankingRenderError("Unsupported ranking metric")

    width = 1000
    height = 120 + (len(rows) * 64) + 70
    image = Image.new("RGBA", (width, height), (10, 10, 10, 255))
    draw = ImageDraw.Draw(image)

    title_font = _load_font(28)
    header_font = _load_font(18)
    body_font = _load_font(20)
    badge_font = _load_font(20)
    symbol_font = _load_symbol_font(20)
    footer_font = _load_font(14)

    meta = RANKING_META[metric]
    draw.text((40, 25), meta["title"], font=title_font, fill="#FFFFFF")
    draw.text((40, 75), "Rank", font=header_font, fill="#AAAAAA")
    draw.text((140, 75), "Player", font=header_font, fill="#AAAAAA")
    draw.text((560, 75), meta["label"], font=header_font, fill="#AAAAAA")

    row_start_y = 105
    for idx, row in enumerate(rows, start=1):
        y = row_start_y + (idx - 1) * 64

        draw.rounded_rectangle((30, y - 4, 970, y + 50), radius=8, fill=(18, 18, 18, 255))
        draw.text((40, y + 8), f"#{idx}", font=body_font, fill="#FFFFFF")

        head = _load_head_image(row["head_image_base64"]).resize((32, 32))
        image.paste(head, (90, y + 5), head)

        name = str(row["minecraft_name"] or "Unknown")
        name_x = 140
        name_y = y + 8
        draw.text((name_x, name_y), name, font=body_font, fill="#FFFFFF")

        tag_symbol = get_tag_symbol(row.get("tag"))
        if tag_symbol:
            name_bbox = draw.textbbox((name_x, name_y), name, font=body_font)
            draw.text((name_bbox[2] + 8, name_y), tag_symbol, font=symbol_font, fill="#FFFFFF")

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

        metric_value = _extract_metric_value(row, metric)
        value_text = _format_metric_value(metric_value, meta["decimals"])
        draw.text((560, y + 8), value_text, font=body_font, fill="#55FFFF")

    draw.text((40, height - 35), _latest_timestamp(rows), font=footer_font, fill="#AAAAAA")

    output = io.BytesIO()
    image.save(output, format="PNG")
    output.seek(0)
    return output


def render_stats_image(row: Any) -> io.BytesIO:
    width = 720
    tags = _extract_tags(row.get("tag"))
    tag_meanings = [TAG_INFO[tag]["meaning"] for tag in tags if tag in TAG_INFO]
    has_tag_meanings = bool(tag_meanings)
    base_height = 280
    tag_line_height = 28
    extra_height = tag_line_height if has_tag_meanings else 0
    height = base_height + extra_height
    image = Image.new("RGBA", (width, height), (10, 10, 10, 255))
    draw = ImageDraw.Draw(image)

    title_font = _load_font(28)
    body_font = _load_font(24)
    badge_font = _load_font(32)
    symbol_font = _load_symbol_font(32)
    small_font = _load_font(16)
    tag_font = _load_font(20)

    draw.rounded_rectangle((20, 20, width - 20, height - 20), radius=12, fill=(18, 18, 18, 255))
    draw.text((40, 35), "Bedwars Stats", font=title_font, fill="#FFFFFF")

    head = _load_head_image(row["head_image_base64"]).resize((96, 96), Image.Resampling.NEAREST)
    image.paste(head, (45, 95), head)

    name = str(row["minecraft_name"] or "Unknown")
    player_label = f"Player: {name}"
    player_x = 170
    player_y = 100
    draw.text((player_x, player_y), player_label, font=body_font, fill="#FFFFFF")

    tag_symbols = []
    for tag in tags:
        symbol = get_tag_symbol(tag)
        if symbol:
            tag_symbols.append(symbol)
    if tag_symbols:
        label_bbox = draw.textbbox((player_x, player_y), player_label, font=body_font)
        draw.text(
            (label_bbox[2] + 8, player_y),
            " ".join(tag_symbols),
            font=symbol_font,
            fill="#FFFFFF",
        )
    star = _safe_int(row["bedwars_star"])
    draw_star_text(
        draw,
        170,
        145,
        star,
        badge_font,
        symbol_font,
        get_prestige_style(max(star, 0)),
    )
    draw.text((170, 195), f"FKDR: {_safe_float(row['fkdr']):.2f}", font=body_font, fill="#55FFFF")
    if has_tag_meanings:
        tag_line = " / ".join(tag_meanings)
        draw.text(
            (170, 235),
            f"Tag: {tag_line}",
            font=tag_font,
            fill="#FFFFFF",
        )

    draw.text((40, height - 35), f"Last Updated: {row['last_updated'] or 'N/A'}", font=small_font, fill="#AAAAAA")

    output = io.BytesIO()
    image.save(output, format="PNG")
    output.seek(0)
    return output
