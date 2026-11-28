"""Minecraft風のピクセル文字描画（§カラーコード・影・プロポーショナル幅）。

座標・サイズはすべて「実ピクセル」。unit = フォントの1ドットの大きさ（px）。
1行の高さは 9 unit、大文字の高さは 7 unit、影は 1 unit 右下にずらして描きます。
"""
from __future__ import annotations

import math
import os
from functools import lru_cache

from PIL import Image, ImageDraw, ImageFont

FONT_DIR = os.path.join(os.path.dirname(__file__), "fonts")
PIXEL_FONT = os.path.join(FONT_DIR, "Monocraft.ttf")
SYMBOL_FONT = os.path.join(FONT_DIR, "DejaVuSans.ttf")

# Minecraft の16色（§0〜§f）
MC_COLORS: dict[str, tuple[int, int, int]] = {
    "0": (0, 0, 0), "1": (0, 0, 170), "2": (0, 170, 0), "3": (0, 170, 170),
    "4": (170, 0, 0), "5": (170, 0, 170), "6": (255, 170, 0), "7": (170, 170, 170),
    "8": (85, 85, 85), "9": (85, 85, 255), "a": (85, 255, 85), "b": (85, 255, 255),
    "c": (255, 85, 85), "d": (255, 85, 255), "e": (255, 255, 85), "f": (255, 255, 255),
}

CAP = 7        # 大文字の高さ（unit）
LINE = 9       # 行の高さ（unit）
SPACE = 4      # 半角スペースの幅（unit）


def to_rgb(color) -> tuple[int, int, int]:
    """'a' / '§a' / '#55ff55' / (r,g,b) のどれでも RGB にする。"""
    if isinstance(color, tuple):
        return color[:3]
    c = str(color).lstrip("§")
    if c.lower() in MC_COLORS:
        return MC_COLORS[c.lower()]
    c = c.lstrip("#")
    return tuple(int(c[i:i + 2], 16) for i in (0, 2, 4))


def shadow_of(rgb: tuple[int, int, int]) -> tuple[int, int, int]:
    """Minecraft と同じ影色（各チャンネル1/4）。"""
    return (rgb[0] // 4, rgb[1] // 4, rgb[2] // 4)


def strip_codes(text: str) -> str:
    out, i = [], 0
    while i < len(text):
        if text[i] == "§" and i + 1 < len(text):
            i += 8 if text[i + 1] == "#" else 2
            continue
        out.append(text[i])
        i += 1
    return "".join(out)


# ---------------------------------------------------------------------------
# グリフ（文字の形）を「1 unit = 1px」または「0.5 unit = 1px」の原寸で作ってキャッシュ
# ---------------------------------------------------------------------------
class _Glyph:
    __slots__ = ("mask", "top", "half")

    def __init__(self, mask: Image.Image, top: int, half: bool):
        self.mask = mask    # "L" 画像（0/255）
        self.top = top      # ベースラインからマスク上端までのずれ（原寸px、上が負）
        self.half = half    # True なら 1px = 0.5 unit（記号用の細かいドット）


_mono = ImageFont.truetype(PIXEL_FONT, 18, layout_engine=ImageFont.Layout.BASIC)  # 1 unit = 2px
_sym = ImageFont.truetype(SYMBOL_FONT, 15, layout_engine=ImageFont.Layout.BASIC)


def _raster(font, ch: str):
    img = Image.new("L", (64, 64), 0)
    d = ImageDraw.Draw(img)
    d.fontmode = "1"  # アンチエイリアスなし（ドットをくっきり）
    d.text((16, 44), ch, font=font, fill=255, anchor="ls")
    bbox = img.getbbox()
    if not bbox:
        return None
    return img.crop(bbox), bbox[1] - 44


_NOTDEF = _raster(_mono, "\U0010fffd")


def _shape(size: int, painter) -> Image.Image:
    """4倍で描いて縮小→二値化して、きれいなドット絵記号を作る。"""
    ss = 4
    big = Image.new("L", (size * ss, size * ss), 0)
    painter(ImageDraw.Draw(big), size * ss)
    small = big.resize((size, size), Image.BOX)
    return small.point(lambda v: 255 if v >= 110 else 0)


def _star_points(cx, cy, r_out, r_in, n=5, rot=-90):
    pts = []
    for i in range(n * 2):
        r = r_out if i % 2 == 0 else r_in
        a = math.radians(rot + i * 180 / n)
        pts.append((cx + r * math.cos(a), cy + r * math.sin(a)))
    return pts


def _custom_symbols() -> dict[str, Image.Image]:
    """プレステージの星などを自作のドット絵で用意（0.5 unit 単位、高さ約7 unit）。"""
    S = 15
    syms: dict[str, Image.Image] = {}

    def star_filled(d, s, hole=0.0):
        c = s / 2
        d.polygon(_star_points(c, c * 1.06, s * 0.5, s * 0.2), fill=255)
        if hole:
            d.polygon(_star_points(c, c * 1.06, s * hole, s * hole * 0.45), fill=0)

    syms["✫"] = _shape(S, lambda d, s: star_filled(d, s, hole=0.13))
    syms["⚝"] = _shape(S, lambda d, s: (star_filled(d, s),
                                         d.polygon(_star_points(s / 2, s / 2 * 1.06, s * 0.34, s * 0.135), fill=0)))

    def outlined(d, s):
        star_filled(d, s)
        d.polygon(_star_points(s / 2, s / 2 * 1.06, s * 0.38, s * 0.15), fill=0)
        d.polygon(_star_points(s / 2, s / 2 * 1.06, s * 0.27, s * 0.11), fill=255)
    syms["✭"] = _shape(S, outlined)

    def circled(d, s):
        d.ellipse((0, 0, s - 1, s - 1), fill=255)
        d.polygon(_star_points(s / 2, s / 2 * 1.05, s * 0.36, s * 0.15), fill=0)
    syms["✪"] = _shape(S, circled)

    def clubs(d, s):
        c, w, r = s / 2, s * 0.09, s * 0.17
        d.rectangle((c - w, s * 0.12, c + w, s * 0.88), fill=255)
        d.rectangle((s * 0.12, c - w, s * 0.88, c + w), fill=255)
        for x, y in ((c, s * 0.18), (c, s * 0.82), (s * 0.18, c), (s * 0.82, c)):
            d.ellipse((x - r, y - r, x + r, y + r), fill=255)
    syms["✥"] = _shape(S, clubs)

    # Hypixel スタッフの記号（簡略化した自作ドット）
    zo = ["##...##", "##...##", "##...##", "#######", "##...##", "##...##", "##.....", "##....."]
    img = Image.new("L", (7, 8), 0)
    for y, row in enumerate(zo):
        for x, v in enumerate(row):
            if v == "#":
                img.putpixel((x, y), 255)
    syms["ዞ"] = img.resize((14, 16), Image.NEAREST)
    return syms


_CUSTOM = _custom_symbols()


@lru_cache(maxsize=4096)
def _glyph(ch: str) -> _Glyph | None:
    if ch in _CUSTOM:
        m = _CUSTOM[ch]
        bbox = m.getbbox()
        m = m.crop(bbox)
        return _Glyph(m, round(-CAP - m.height / 2), True)  # 大文字の高さの中央にそろえる

    r = _raster(_mono, ch)
    if r is not None and not (_NOTDEF and r[0].tobytes() == _NOTDEF[0].tobytes()):
        crop, top = r
        w, h = math.ceil(crop.width / 2), math.ceil(crop.height / 2)
        if top % 2:  # 縦が半ドットずれている字は1px詰めてそろえる
            top -= 1
            crop = crop.crop((0, -1, crop.width, crop.height))
        down = crop.resize((w, h), Image.NEAREST)
        top //= 2
        if ch == ",":  # 数字の区切りが「.」に見えないよう、カンマは1ドット下げる
            top += 1
        return _Glyph(down, top, False)

    r = _raster(_sym, ch)  # Monocraft に無い字は DejaVu Sans で代用
    if r is None:
        return None
    crop, _ = r
    return _Glyph(crop, round(-CAP - crop.height / 2), True)


@lru_cache(maxsize=8192)
def _scaled(ch: str, unit: int):
    """unit(px) に拡大したマスク・上端オフセット・幅を返す。"""
    g = _glyph(ch)
    if g is None:
        return None
    f = unit / 2 if g.half else unit
    w = max(1, round(g.mask.width * f))
    h = max(1, round(g.mask.height * f))
    mask = g.mask.resize((w, h), Image.NEAREST)
    top = round(g.top * f)
    return mask, top, w


def parse(text: str, default="f"):
    """§コードを解釈して (文字, RGB, 太字) を順に返す。§#rrggbb で任意色も可。"""
    base = to_rgb(default)
    color, bold = base, False
    i = 0
    while i < len(text):
        c = text[i]
        if c == "§" and i + 1 < len(text):
            code = text[i + 1].lower()
            if code == "#" and i + 8 <= len(text):
                color, bold = to_rgb(text[i + 2:i + 8]), False
                i += 8
                continue
            if code in MC_COLORS:
                color, bold = MC_COLORS[code], False
            elif code == "l":
                bold = True
            elif code == "r":
                color, bold = base, False
            i += 2
            continue
        yield c, color, bold
        i += 1


def layout(text: str, unit: int, default="f"):
    items, x = [], 0
    for ch, color, bold in parse(text, default):
        if ch == " ":
            x += SPACE * unit
            continue
        g = _scaled(ch, unit)
        if g is None:
            x += SPACE * unit
            continue
        mask, top, w = g
        items.append((mask, x, top, color, bold))
        x += w + unit + (unit if bold else 0)
    width = x - unit if items and text and not text.endswith(" ") else x
    return items, max(0, width)


def measure(text: str, unit: int) -> int:
    return layout(text, unit)[1]


def draw(img: Image.Image, x: float, y: float, text: str, unit: int, default="f",
         anchor: str = "l", shadow: bool = True, shadow_color=None) -> int:
    """(x, y) = 文字の上端（大文字の頭）。anchor は 'l' / 'm' / 'r'。描いた幅を返す。"""
    items, width = layout(text, unit, default)
    if anchor == "m":
        x -= width / 2
    elif anchor == "r":
        x -= width
    x, base = round(x), round(y) + CAP * unit
    passes = ([True] if shadow else []) + [False]
    for is_shadow in passes:
        off = unit if is_shadow else 0
        for mask, gx, top, color, bold in items:
            col = (shadow_color or shadow_of(color)) if is_shadow else color
            fill = col + (255,) if len(col) == 3 else col
            for bx in ((0, unit) if bold else (0,)):
                px, py = x + gx + bx + off, base + top + off
                img.paste(fill, (px, py, px + mask.width, py + mask.height), mask)
    return width


def fit_unit(text: str, max_width: float, units) -> int:
    """max_width に収まる一番大きい unit を選ぶ。"""
    for u in units:
        if measure(text, u) <= max_width:
            return u
    return units[-1]
