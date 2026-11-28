"""インベントリ風デザイン用の、16x16 のオリジナルのドット絵アイコン。

文字で描いた絵（1文字 = 1ドット、'.' は透明）と、図形から作るアイコンがあります。
"""
from __future__ import annotations

import math
import random
from functools import lru_cache

from PIL import Image


def _hex(c: str) -> tuple[int, int, int, int]:
    c = c.lstrip("#")
    return (int(c[0:2], 16), int(c[2:4], 16), int(c[4:6], 16), 255)


def from_art(rows: list[str], palette: dict[str, str]) -> Image.Image:
    img = Image.new("RGBA", (16, 16), (0, 0, 0, 0))
    px = img.load()
    for y, row in enumerate(rows):
        for x, ch in enumerate(row):
            if ch != ".":
                px[x, y] = _hex(palette[ch])
    return img


def outlined(img: Image.Image, color: str) -> Image.Image:
    """透明な隣接ドットに輪郭色を置く（上下左右）。"""
    src = img.load()
    out = img.copy()
    px = out.load()
    oc = _hex(color)
    for y in range(16):
        for x in range(16):
            if src[x, y][3]:
                continue
            for dx, dy in ((1, 0), (-1, 0), (0, 1), (0, -1)):
                nx, ny = x + dx, y + dy
                if 0 <= nx < 16 and 0 <= ny < 16 and src[nx, ny][3]:
                    px[x, y] = oc
                    break
    return out


def _shape(fn) -> Image.Image:
    """fn(x, y) -> 色 or None（ドットの中心で判定）"""
    img = Image.new("RGBA", (16, 16), (0, 0, 0, 0))
    px = img.load()
    for y in range(16):
        for x in range(16):
            c = fn(x + 0.5, y + 0.5)
            if c:
                px[x, y] = _hex(c)
    return img


# ---------------------------------------------------------------- 文字で描いたアイコン
TROPHY = from_art([
    "................",
    "..kkkkkkkkkkkk..",
    ".kYYyyyyyyyyodk.",
    "kkYyyyyyyyyyodkk",
    "k.kyyyyyyyyodk.k",
    "k.kYyyyyyyyodk.k",
    ".kkoyyyyyyyodkk.",
    "...koyyyyyodk...",
    "....kooyyodk....",
    ".....kkoodk.....",
    "......kodk......",
    "......kodk......",
    "....kkkodkkk....",
    "...kBBBBBBBBk...",
    "...kbbbbbbbbk...",
    "...kkkkkkkkkk...",
], {"k": "4a2c08", "Y": "fff6b0", "y": "ffd23f", "o": "e39b12", "d": "a5600a", "B": "8b5a2b", "b": "5e3a19"})


def _sword(blade_hi, blade, blade_lo, edge):
    return from_art([
        "............kkk.",
        "...........kHbk.",
        "..........kHbdk.",
        ".........kHbdk..",
        "........kHbdk...",
        ".......kHbdk....",
        "......kHbdk.....",
        ".kk..kHbdk......",
        ".kgkkHbdk.......",
        "..kgkbdk........",
        "...kgkk.........",
        "...khgk.........",
        "..khhkgk........",
        ".khhk.kgk.......",
        ".kk....kk.......",
        "................",
    ], {"k": edge, "H": blade_hi, "b": blade, "d": blade_lo, "g": "c8a24a", "h": "6b4423"})


DIAMOND_SWORD = _sword("d7fffb", "55e6e0", "1fa5a8", "0c3a40")
IRON_SWORD = _sword("ffffff", "d4d8dc", "8e959c", "2b2f33")

_SKULL = [
    "................",
    "....kkkkkkkk....",
    "...kHHhhhhhhk...",
    "..kHhhhhhhhhsk..",
    "..khhhhhhhhhsk..",
    "..khkkkhhkkkhk..",
    "..khkkkhhkkksk..",
    "..khkkkhhkkksk..",
    "..khhhhkkhhhsk..",
    "...khhhhhhhsk...",
    "...kshkhkhksk...",
    "....kkhkhkkk....",
    ".....kkkkkk.....",
    "................",
    "................",
    "................",
]
SKULL = from_art(_SKULL, {"k": "3a352c", "H": "fffbea", "h": "e6dfc8", "s": "b3aa8e"})
WITHER_SKULL = from_art(_SKULL, {"k": "0a0a0a", "H": "8a8a8a", "h": "4a4a4a", "s": "2e2e2e"})

BED = from_art([
    "................",
    "................",
    "................",
    "................",
    "...........kkkk.",
    "..........kWWwk.",
    ".kkkkkkkkkkWwwk.",
    "kRRRRRRRRRkwwgk.",
    "kRrrrrrrrrkkkkkk",
    "krrrrrrrrrrrrrdk",
    "kddddddddddddddk",
    "kbbbbbbbbbbbbbbk",
    "kBkkkkkkkkkkkkBk",
    "kBk..........kBk",
    "kkk..........kkk",
    "................",
], {"k": "2a1410", "R": "ff6b5e", "r": "d8312b", "d": "8f1d1a", "W": "ffffff", "w": "e2e2e2",
    "g": "b5b5b5", "b": "a9743f", "B": "6e4823"})

BROKEN_BED = from_art([
    "................",
    "................",
    "................",
    "................",
    "...........kkkk.",
    "..........kWWwk.",
    ".kkkkkk.kkkWwwk.",
    "kRRRRRk.kRkwwgk.",
    "kRrrrrrk.krkkkkk",
    "krrrrrk.krrrrrdk",
    "kddddddk.kdddddk",
    "kbbbbbk.kbbbbbbk",
    "kBkkkkk.kkkkkkBk",
    "kBk..........kBk",
    "kkk..........kkk",
    "................",
], {"k": "1c1c1c", "R": "9a9a9a", "r": "707070", "d": "4a4a4a", "W": "dcdcdc", "w": "bcbcbc",
    "g": "8a8a8a", "b": "7a6a5a", "B": "4e4236"})

FLAME = from_art([
    "........k.......",
    ".......kYk......",
    "......kYyk......",
    "......kyyok.....",
    ".....kYyyok..k..",
    "..k..kyyooak.kk.",
    ".kk.kYyyooaak.k.",
    ".kokyyyoooaakak.",
    ".koyyyWWooaaaak.",
    "kooyyWWWyooaaak.",
    "kooyWWWWWyoaaak.",
    "koyyWWWWWyyoaak.",
    ".koyyWWWyyooak..",
    ".kooyyyyyoooak..",
    "..kkoooooaakk...",
    "....kkkkkkk.....",
], {"k": "5a1a05", "Y": "fff7b8", "y": "ffd23a", "o": "ff8a1c", "a": "d9440f", "W": "fffde8"})

TICKET = from_art([
    "................",
    "................",
    "................",
    "kkkkkkkkkkkkkkk.",
    "kPPPPPPPPPkPPPk.",
    "kPrrrrrrrPpPPPk.",
    ".kPpppppPPkPpPk.",
    ".kPPPPPPPPpPPPk.",
    "kPrrrrrrrPkPPPk.",
    "kPPPPPPPPPpPpPk.",
    "kSSSSSSSSSkSSSk.",
    "kkkkkkkkkkkkkkk.",
    "................",
    "................",
    "................",
    "................",
], {"k": "4a3a1c", "P": "fff1c4", "p": "d9c48e", "r": "d64a3a", "S": "e6d29a"})

CLOCK = None  # 下で図形から作る


# ---------------------------------------------------------------- 図形から作るアイコン
def _clock() -> Image.Image:
    def f(x, y):
        dx, dy = x - 8, y - 8
        d = math.hypot(dx, dy)
        if d > 7.1:
            return None
        if d > 5.6:
            return "f2c94c" if dy + dx < -1 else ("d99a1a" if dy + dx < 4 else "a5650c")
        # 文字盤
        if abs(dx) < 0.6 and -4.6 < dy < 0.6:
            return "2b2b2b"   # 長針
        if abs(dy) < 0.6 and -0.6 < dx < 3.6:
            return "2b2b2b"   # 短針
        if (round(x - 0.5), round(y - 0.5)) in ((8, 3), (8, 12), (3, 8), (12, 8)):
            return "8c7a5a"
        return "fff8e6" if dy < 0 else "f0e6cc"
    return outlined(_shape(f), "3d2a08")


def _barrier() -> Image.Image:
    """「禁止」マーク（白地に赤い輪と斜線）。"""
    def f(x, y):
        dx, dy = x - 8, y - 8
        d = math.hypot(dx, dy)
        if d > 7.2:
            return None
        if d > 5.3 or (abs(dx - dy) < 1.3 and d <= 5.6):
            return "ff7466" if (dx + dy) < -3 else ("e0302a" if (dx + dy) < 4 else "a51b16")
        return "fbf7f2" if dx + dy < 0 else "e9e2da"
    return outlined(_shape(f), "4a0d0a")


def _coin() -> Image.Image:
    def f(x, y):
        dx, dy = x - 8, y - 8
        d = math.hypot(dx, dy)
        if d > 7.0:
            return None
        if d > 5.6:
            return "fff0a0" if dx + dy < -2 else ("e6a521" if dx + dy < 4 else "b0700d")
        if 4.2 < d <= 5.0:
            return "c98612"   # 内側の縁
        if abs(dx) + abs(dy) < 2.6:
            return "fff6c2" if dx + dy < 0 else "e6a521"   # 中央の刻印
        return "ffd84a" if dx + dy < 0 else "f2bd2c"
    return outlined(_shape(f), "4d3005")


def _ingot(hi, mid, lo, dark, edge) -> Image.Image:
    """横に置いた延べ棒（上面が明るく、正面・右端・底が暗い）。"""
    rows = [
        "................",
        "................",
        "................",
        "................",
        "....kkkkkkkkk...",
        "...kHHHHHHHHHk..",
        "..kHhhhhhhhhhhk.",
        ".kmmmmmmmmmmmmlk",
        "kmmmmmmmmmmmmmlk",
        "kmmmmmmmmmmmmllk",
        "kddddddddddddddk",
        ".kkkkkkkkkkkkkk.",
        "................",
        "................",
        "................",
        "................",
    ]
    return from_art(rows, {"k": edge, "H": hi, "h": mid, "m": lo, "l": dark, "d": dark})


def _gem(kind: str) -> Image.Image:
    if kind == "diamond":
        rows = [
            "................",
            "................",
            "....kkkkkkkk....",
            "...kWHHkHHHhk...",
            "..kWHHkHHHHhhk..",
            ".kkkkkkkkkkkkkk.",
            ".kHHmmmHmmmmhhk.",
            "..kHmmmHmmmmhk..",
            "...kHmmHmmmhk...",
            "....kHmHmmhk....",
            ".....kHHmhk.....",
            "......kHhk......",
            ".......kk.......",
            "................",
            "................",
            "................",
        ]
        pal = {"k": "0c3a40", "W": "ffffff", "H": "a6fff6", "m": "40d9d0", "h": "1a9aa0"}
    else:  # emerald
        rows = [
            "................",
            "......kkkk......",
            ".....kWHHhk.....",
            "....kWHmmhhk....",
            "...kWHmmmmhhk...",
            "...kHmmmmmmhk...",
            "...kHmmMMmmhk...",
            "...kHmmMMmmhk...",
            "...kHmmMMmmhk...",
            "...kHmmmmmmhk...",
            "...kHmmmmmmdk...",
            "....kHmmmmdk....",
            ".....kmmmdk.....",
            "......kkkk......",
            "................",
            "................",
        ]
        pal = {"k": "06361a", "W": "eafff0", "H": "8af7a8", "m": "2fcf62", "M": "62e88a", "h": "16994a",
               "d": "0d6b31"}
    return from_art(rows, pal)


def _star() -> Image.Image:
    """「総合」モード用の星（4方向に光る）。"""
    def f(x, y):
        dx, dy = abs(x - 8), abs(y - 8)
        if dx + dy * 3.2 < 7.5 or dy + dx * 3.2 < 7.5 or (dx + dy) < 3.6:
            if dx + dy < 1.6:
                return "ffffff"
            return "fdf3ff" if (x - 8) + (y - 8) < 0 else "d9c8f0"
        return None
    return outlined(_shape(f), "4c3a66")


def _pane() -> Image.Image:
    """ガラス板（メニューの枠に使う、暗めの色ガラス）。"""
    def f(x, y):
        ix, iy = int(x), int(y)
        if ix in (0, 15) or iy in (0, 15):
            return "1e1e22"
        if ix in (1, 14) or iy in (1, 14):
            return "34343a"
        if ix - iy in (-2, -1) and 3 < ix < 12:
            return "4a4a52"   # 斜めの光
        if ix - iy in (5,) and 7 < ix < 13:
            return "44444c"
        return "29292e"
    return _shape(f)


@lru_cache(maxsize=16)
def wool_texture(color: str, seed: int = 1) -> Image.Image:
    """羊毛っぽい16x16テクスチャ（ブロック描画用）。"""
    base = _hex(color)
    rnd = random.Random(seed)
    img = Image.new("RGBA", (16, 16), base)
    px = img.load()
    for y in range(16):
        for x in range(16):
            n = rnd.uniform(-1, 1)
            curl = math.sin((x * 1.7 + y * 0.9) + math.sin(y * 1.3) * 2) * 0.5
            k = 1 + 0.07 * n + 0.08 * curl
            px[x, y] = tuple(max(0, min(255, int(c * k))) for c in base[:3]) + (255,)
    return img


ICONS: dict[str, Image.Image] = {
    "trophy": TROPHY, "barrier": _barrier(), "clock": _clock(), "flame": FLAME,
    "diamond_sword": DIAMOND_SWORD, "wither_skull": WITHER_SKULL, "iron_sword": IRON_SWORD, "skull": SKULL,
    "bed": BED, "broken_bed": BROKEN_BED, "coin": _coin(), "ticket": TICKET,
    "iron": _ingot("ffffff", "e6eaed", "b9bfc5", "7d838a", "2b2f33"),
    "gold": _ingot("fffbd0", "ffe066", "f0b429", "b8730c", "4a2c08"),
    "diamond": _gem("diamond"), "emerald": _gem("emerald"), "star": _star(), "pane": _pane(),
}


# ---------------------------------------------------------------- ブロック（羊毛）を立体で描く
def iso_block(tex: Image.Image, size: int) -> Image.Image:
    """インベントリのブロック表示のような、斜め上から見た立方体（size×size px）。"""
    S = size * 4
    out = Image.new("RGBA", (S, S), (0, 0, 0, 0))
    cx = S / 2
    w = S * 0.43          # 菱形の横半分
    h = w * 0.5           # 菱形の縦半分
    top_y = S * 0.08
    side = S * 0.5        # 側面の高さ
    T = (cx, top_y)
    L = (cx - w, top_y + h)
    R = (cx + w, top_y + h)
    C = (cx, top_y + 2 * h)

    def face(img, O, U, V, shade):
        # O: 原点, U: 横方向(16ドット分), V: 縦方向(16ドット分)
        f = img.convert("RGBA")
        if shade < 1:
            r, g, b, a = f.split()
            r, g, b = (ch.point(lambda v: int(v * shade)) for ch in (r, g, b))
            f = Image.merge("RGBA", (r, g, b, a))
        ux, uy = U[0] / 16, U[1] / 16
        vx, vy = V[0] / 16, V[1] / 16
        det = ux * vy - vx * uy
        a, b, d, e = vy / det, -vx / det, -uy / det, ux / det
        c = -(a * O[0] + b * O[1])
        f_ = -(d * O[0] + e * O[1])
        warped = f.transform((S, S), Image.AFFINE, (a, b, c, d, e, f_), resample=Image.NEAREST)
        out.alpha_composite(warped)

    face(tex, T, (R[0] - T[0], R[1] - T[1]), (L[0] - T[0], L[1] - T[1]), 1.0)          # 上
    face(tex, L, (C[0] - L[0], C[1] - L[1]), (0, side), 0.78)                           # 左
    face(tex, C, (R[0] - C[0], R[1] - C[1]), (0, side), 0.6)                            # 右
    return out.resize((size, size), Image.BOX)
