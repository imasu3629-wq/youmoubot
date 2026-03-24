import io
import os
from datetime import datetime
from typing import Any

import requests
from PIL import Image, ImageDraw, ImageFont

REQUEST_TIMEOUT = 10
HEAD_CACHE_DIR = os.path.join("cache", "skins")
DEFAULT_FONT_PATH = os.environ.get(
    "RANKING_FONT_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "Minecraftia.ttf"),
)

PRESTIGE_STYLES = {
    100: {"symbol": "✫", "leftBracket": "#FFFFFF", "digits": ["#FFFFFF", "#FFFFFF", "#FFFFFF"], "symbolColor": "#AAAAAA", "rightBracket": "#FFFFFF"},
    200: {"symbol": "✫", "leftBracket": "#FFAA00", "digits": ["#FFAA00", "#FFAA00", "#FFAA00"], "symbolColor": "#FFAA00", "rightBracket": "#FFAA00"},
    300: {"symbol": "✫", "leftBracket": "#55FFFF", "digits": ["#55FFFF", "#55FFFF", "#55FFFF"], "symbolColor": "#55FFFF", "rightBracket": "#55FFFF"},
    400: {"symbol": "✫", "leftBracket": "#00AA00", "digits": ["#00AA00", "#00AA00", "#00AA00"], "symbolColor": "#00AA00", "rightBracket": "#00AA00"},
    500: {"symbol": "✫", "leftBracket": "#00AAAA", "digits": ["#00AAAA", "#00AAAA", "#00AAAA"], "symbolColor": "#00AAAA", "rightBracket": "#00AAAA"},
    600: {"symbol": "✫", "leftBracket": "#AA0000", "digits": ["#AA0000", "#AA0000", "#AA0000"], "symbolColor": "#AA0000", "rightBracket": "#AA0000"},
    700: {"symbol": "✫", "leftBracket": "#FF55FF", "digits": ["#FF55FF", "#FF55FF", "#FF55FF"], "symbolColor": "#FF55FF", "rightBracket": "#FF55FF"},
    800: {"symbol": "✫", "leftBracket": "#5555FF", "digits": ["#5555FF", "#5555FF", "#5555FF"], "symbolColor": "#5555FF", "rightBracket": "#5555FF"},
    900: {"symbol": "✫", "leftBracket": "#AA00AA", "digits": ["#AA00AA", "#AA00AA", "#AA00AA"], "symbolColor": "#AA00AA", "rightBracket": "#AA00AA"},
    1000: {"symbol": "✫", "leftBracket": "#FF5555", "digits": ["#FFAA00", "#55FF55", "#55FFFF"], "symbolColor": "#FF55FF", "rightBracket": "#AA00AA"},
    1100: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#FFFFFF", "#FFFFFF", "#FFFFFF", "#FFFFFF"], "symbolColor": "#AAAAAA", "rightBracket": "#AAAAAA"},
    1200: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#FFFF55", "#FFFF55", "#FFFF55", "#FFFF55"], "symbolColor": "#FFAA00", "rightBracket": "#AAAAAA"},
    1300: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#55FFFF", "#55FFFF", "#55FFFF", "#55FFFF"], "symbolColor": "#00AAAA", "rightBracket": "#AAAAAA"},
    1400: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#55FF55", "#55FF55", "#55FF55", "#55FF55"], "symbolColor": "#00AA00", "rightBracket": "#AAAAAA"},
    1500: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#00AAAA", "#00AAAA", "#00AAAA", "#00AAAA"], "symbolColor": "#5555FF", "rightBracket": "#AAAAAA"},
    1600: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#FF5555", "#FF5555", "#FF5555", "#FF5555"], "symbolColor": "#AA0000", "rightBracket": "#AAAAAA"},
    1700: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#FF55FF", "#FF55FF", "#FF55FF", "#FF55FF"], "symbolColor": "#AA00AA", "rightBracket": "#AAAAAA"},
    1800: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#5555FF", "#5555FF", "#5555FF", "#5555FF"], "symbolColor": "#0000AA", "rightBracket": "#AAAAAA"},
    1900: {"symbol": "✪", "leftBracket": "#AAAAAA", "digits": ["#AA00AA", "#AA00AA", "#AA00AA", "#AA00AA"], "symbolColor": "#555555", "rightBracket": "#AAAAAA"},
    2000: {"symbol": "✪", "leftBracket": "#555555", "digits": ["#AAAAAA", "#FFFFFF", "#FFFFFF", "#AAAAAA"], "symbolColor": "#FFFFFF", "rightBracket": "#555555"},
    2100: {"symbol": "⚝", "leftBracket": "#FFFFFF", "digits": ["#FFFF55", "#FFFF55", "#FFAA00", "#FFAA00"], "symbolColor": "#FFAA00", "rightBracket": "#FFAA00"},
    2200: {"symbol": "⚝", "leftBracket": "#FFAA00", "digits": ["#FFFFFF", "#FFFFFF", "#55FFFF", "#00AAAA"], "symbolColor": "#00AAAA", "rightBracket": "#00AAAA"},
    2300: {"symbol": "⚝", "leftBracket": "#AA00AA", "digits": ["#FF55FF", "#FF55FF", "#FFAA00", "#FFFF55"], "symbolColor": "#FFFF55", "rightBracket": "#FFFF55"},
    2400: {"symbol": "⚝", "leftBracket": "#55FFFF", "digits": ["#FFFFFF", "#FFFFFF", "#AAAAAA", "#AAAAAA"], "symbolColor": "#FFFFFF", "rightBracket": "#555555"},
    2500: {"symbol": "⚝", "leftBracket": "#FFFFFF", "digits": ["#55FF55", "#55FF55", "#00AA00", "#00AA00"], "symbolColor": "#00AA00", "rightBracket": "#00AA00"},
    2600: {"symbol": "⚝", "leftBracket": "#AA0000", "digits": ["#FF5555", "#FF5555", "#FF55FF", "#FF55FF"], "symbolColor": "#FF55FF", "rightBracket": "#AA00AA"},
    2700: {"symbol": "⚝", "leftBracket": "#FFFF55", "digits": ["#FFFF55", "#555555", "#555555", "#AAAAAA"], "symbolColor": "#AAAAAA", "rightBracket": "#555555"},
    2800: {"symbol": "⚝", "leftBracket": "#55FF55", "digits": ["#55FF55", "#00AA00", "#FFAA00", "#FFAA00"], "symbolColor": "#FFAA00", "rightBracket": "#FFFF55"},
    2900: {"symbol": "⚝", "leftBracket": "#55FFFF", "digits": ["#00AAAA", "#00AAAA", "#5555FF", "#5555FF"], "symbolColor": "#5555FF", "rightBracket": "#0000AA"},
    3000: {"symbol": "⚝", "leftBracket": "#FFFF55", "digits": ["#FFAA00", "#FFAA00", "#FF5555", "#FF5555"], "symbolColor": "#FF5555", "rightBracket": "#AA0000"},
    3100: {"symbol": "✥", "leftBracket": "#5555FF", "digits": ["#5555FF", "#55FFFF", "#FFAA00", "#FFFF55"], "symbolColor": "#FFAA00", "rightBracket": "#FFFF55"},
    3200: {"symbol": "✥", "leftBracket": "#FFAA00", "digits": ["#FFAA00", "#FFFFFF", "#FFFFFF", "#AAAAAA"], "symbolColor": "#AAAAAA", "rightBracket": "#AAAAAA"},
    3300: {"symbol": "✥", "leftBracket": "#5555FF", "digits": ["#5555FF", "#FF55FF", "#FF5555", "#FF5555"], "symbolColor": "#FF5555", "rightBracket": "#FF5555"},
    3400: {"symbol": "✥", "leftBracket": "#00AA00", "digits": ["#55FF55", "#FF55FF", "#AA00AA", "#AA00AA"], "symbolColor": "#AA00AA", "rightBracket": "#00AA00"},
    3500: {"symbol": "✥", "leftBracket": "#FF5555", "digits": ["#FF5555", "#FFAA00", "#55FF55", "#55FF55"], "symbolColor": "#55FF55", "rightBracket": "#55FF55"},
    3600: {"symbol": "✥", "leftBracket": "#55FF55", "digits": ["#55FF55", "#55FFFF", "#5555FF", "#5555FF"], "symbolColor": "#5555FF", "rightBracket": "#0000AA"},
    3700: {"symbol": "✥", "leftBracket": "#AA0000", "digits": ["#AA0000", "#55FFFF", "#00AAAA", "#00AAAA"], "symbolColor": "#00AAAA", "rightBracket": "#00AAAA"},
    3800: {"symbol": "✥", "leftBracket": "#0000AA", "digits": ["#5555FF", "#FF55FF", "#FF55FF", "#AA00AA"], "symbolColor": "#FF55FF", "rightBracket": "#0000AA"},
    3900: {"symbol": "✥", "leftBracket": "#FF5555", "digits": ["#FF5555", "#55FFFF", "#55FFFF", "#5555FF"], "symbolColor": "#5555FF", "rightBracket": "#555555"},
    4000: {"symbol": "✥", "leftBracket": "#AA00AA", "digits": ["#FF55FF", "#FFAA00", "#FFAA00", "#FFFF55"], "symbolColor": "#FFAA00", "rightBracket": "#FFFF55"},
    4100: {"symbol": "✥", "leftBracket": "#FFFF55", "digits": ["#FF5555", "#FF5555", "#FF55FF", "#FF55FF"], "symbolColor": "#FF55FF", "rightBracket": "#AA00AA"},
    4200: {"symbol": "✥", "leftBracket": "#0000AA", "digits": ["#55FFFF", "#55FFFF", "#FFFFFF", "#FFFFFF"], "symbolColor": "#AAAAAA", "rightBracket": "#AAAAAA"},
    4300: {"symbol": "✥", "leftBracket": "#000000", "digits": ["#555555", "#555555", "#AA00AA", "#AA00AA"], "symbolColor": "#AA00AA", "rightBracket": "#000000"},
    4400: {"symbol": "✥", "leftBracket": "#00AA00", "digits": ["#AAAAAA", "#AAAAAA", "#FFAA00", "#FFAA00"], "symbolColor": "#AA00AA", "rightBracket": "#FF55FF"},
    4500: {"symbol": "✥", "leftBracket": "#FFFFFF", "digits": ["#FFFFFF", "#FFFFFF", "#55FFFF", "#00AAAA"], "symbolColor": "#55FFFF", "rightBracket": "#00AAAA"},
    4600: {"symbol": "✥", "leftBracket": "#00AAAA", "digits": ["#AAAAAA", "#FFFF55", "#FFAA00", "#FFAA00"], "symbolColor": "#AA00AA", "rightBracket": "#FF55FF"},
    4700: {"symbol": "✥", "leftBracket": "#AAAAAA", "digits": ["#FF5555", "#FF5555", "#5555FF", "#5555FF"], "symbolColor": "#0000AA", "rightBracket": "#AAAAAA"},
    4800: {"symbol": "✥", "leftBracket": "#AA00AA", "digits": ["#FF5555", "#FFAA00", "#FFFF55", "#55FFFF"], "symbolColor": "#55FFFF", "rightBracket": "#00AAAA"},
    4900: {"symbol": "✥", "leftBracket": "#55FF55", "digits": ["#AAAAAA", "#AAAAAA", "#AAAAAA", "#FFFFFF"], "symbolColor": "#55FF55", "rightBracket": "#55FF55"},
    5000: {"symbol": "✥", "leftBracket": "#AA0000", "digits": ["#FF5555", "#AA00AA", "#5555FF", "#5555FF"], "symbolColor": "#0000AA", "rightBracket": "#000000"},
}

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
    if os.path.exists(DEFAULT_FONT_PATH):
        try:
            return ImageFont.truetype(DEFAULT_FONT_PATH, size=size)
        except OSError:
            pass
    return ImageFont.load_default()


def _normalized_tier(star: int) -> int:
    if star < 100:
        return 100
    return min((star // 100) * 100, 5000)


def _get_prestige_style(star: int) -> dict[str, Any]:
    tier = _normalized_tier(star)
    return PRESTIGE_STYLES.get(tier, PRESTIGE_STYLES[100])


def _draw_badge(draw: ImageDraw.ImageDraw, x: int, y: int, star: int, font: ImageFont.ImageFont):
    style = _get_prestige_style(star)
    star_text = str(max(star, 0))

    cursor_x = x
    parts = [
        ("[", style["leftBracket"]),
    ]

    digit_colors = style["digits"]
    for i, digit in enumerate(star_text):
        color_index = min(i, len(digit_colors) - 1)
        parts.append((digit, digit_colors[color_index]))

    parts.extend(
        [
            (style["symbol"], style["symbolColor"]),
            ("]", style["rightBracket"]),
        ]
    )

    for text, color in parts:
        draw.text((cursor_x, y), text, font=font, fill=color)
        bbox = draw.textbbox((cursor_x, y), text, font=font)
        cursor_x += bbox[2] - bbox[0]


def _download_and_cache_head(uuid: str) -> str | None:
    if not uuid:
        return None

    os.makedirs(HEAD_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(HEAD_CACHE_DIR, f"{uuid}.png")
    if os.path.exists(cache_path):
        return cache_path

    try:
        response = requests.get(
            f"https://crafatar.com/avatars/{uuid}?size=64&overlay",
            timeout=REQUEST_TIMEOUT,
        )
        if response.status_code == 200:
            with open(cache_path, "wb") as fp:
                fp.write(response.content)
            return cache_path
    except requests.RequestException:
        return None

    return None


def _load_head_image(uuid: str) -> Image.Image:
    head_path = _download_and_cache_head(uuid)
    if head_path:
        try:
            return Image.open(head_path).convert("RGBA")
        except OSError:
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

        head = _load_head_image(str(row["minecraft_uuid"] or "")).resize((32, 32))
        image.paste(head, (90, y + 5), head)

        name = str(row["minecraft_name"] or "Unknown")
        draw.text((140, y + 8), name, font=body_font, fill="#FFFFFF")

        star = _safe_int(row["bedwars_star"])
        _draw_badge(draw, 380, y + 8, star, badge_font)

        metric_value = _extract_metric_value(row, metric)
        value_text = _format_metric_value(metric_value, meta["decimals"])
        draw.text((560, y + 8), value_text, font=body_font, fill="#55FFFF")

    draw.text((40, height - 35), _latest_timestamp(rows), font=footer_font, fill="#AAAAAA")

    output = io.BytesIO()
    image.save(output, format="PNG")
    output.seek(0)
    return output
