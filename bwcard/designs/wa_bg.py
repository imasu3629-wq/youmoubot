"""スタンダード用の和風の背景。写真を使わないときに敷きます（すべてプログラムで描いています）。

    yozakura … 夜桜（おぼろ月と桜の枝、舞う花びら）
    fuji     … 富士と霞（夕暮れの空、雪をかぶった富士、たなびく霞）
    seigaiha … 青海波と金雲（藍の波模様、朱の日輪、金箔の雲）  ← 標準
    classic  … 夜空（以前のスタンダードの背景）

上に半透明のパネルが重なるので、文字が読みにくくならないよう全体を暗めの色でまとめています。
"""
from __future__ import annotations

import math
import random
from functools import lru_cache

from PIL import Image, ImageChops, ImageDraw, ImageFilter

BACKGROUNDS = {
    "seigaiha": "青海波と金雲",
    "yozakura": "夜桜",
    "fuji": "富士と霞",
    "classic": "夜空（以前の背景）",
}
WA_BACKGROUNDS = ("seigaiha", "yozakura", "fuji")  # bot で「ランダム」にしたときの候補
DEFAULT = "seigaiha"

BW, BH = 900, 664   # 描くときの大きさ（カードを全部表示したとき）。背の低いカードには切り抜いて使う
SS = 2              # 2倍の大きさで描いてから縮小して、ふちをなめらかにする
FOCUS = {"yozakura": 0.30, "fuji": 0.42, "seigaiha": 0.40}  # 背の低いカードで切り抜くときの中心（上からの割合）


def background(name: str | None, W: int, H: int) -> Image.Image:
    """名前の背景を (W, H) の大きさで返す（RGBA）。"""
    name = name or DEFAULT
    if name not in BACKGROUNDS:
        raise ValueError(f"unknown background: {name}（使えるのは {', '.join(BACKGROUNDS)}）")
    if name == "classic":
        from .common import default_background
        return default_background(W, H).convert("RGBA")
    h = H * BW / W                      # カードの高さ（論理ピクセル）
    full = _render(name, max(BH, math.ceil(h)))   # SS倍の大きさで描いたもの
    top = min(max(0.0, BH * FOCUS[name] - h / 2), full.height / SS - h)
    im = full.crop((0, round(top * SS), BW * SS, round((top + h) * SS)))
    if im.size != (W, H):
        im = im.resize((W, H), Image.LANCZOS)
    return im.convert("RGBA")


@lru_cache(maxsize=6)
def _render(name: str, h: int) -> Image.Image:
    return {"yozakura": _yozakura, "fuji": _fuji, "seigaiha": _seigaiha}[name](BW, h)


# ---------------------------------------------------------------- 道具（座標は論理ピクセルで渡す）
class _Pad:
    def __init__(self, w: int, h: int, stops):
        self.w, self.h = w, h
        self.im = _vgrad(w * SS, h * SS, stops)
        self.d = ImageDraw.Draw(self.im, "RGBA")   # RGB画像に描くと半透明色がちゃんと混ざる

    def blur(self, r: float):
        """ここまでに描いたものをぼかす（遠くのものほど何度もぼける＝奥行きが出る）。"""
        self.im = self.im.filter(ImageFilter.GaussianBlur(r * SS))
        self.d = ImageDraw.Draw(self.im, "RGBA")

    def poly(self, pts, fill):
        self.d.polygon([(x * SS, y * SS) for x, y in pts], fill=fill)

    def circle(self, x, y, r, fill):
        self.d.ellipse(((x - r) * SS, (y - r) * SS, (x + r) * SS, (y + r) * SS), fill=fill)

    def ellipse(self, x, y, rx, ry, fill):
        self.d.ellipse(((x - rx) * SS, (y - ry) * SS, (x + rx) * SS, (y + ry) * SS), fill=fill)

    def line(self, pts, fill, width):
        self.d.line([(x * SS, y * SS) for x, y in pts], fill=fill, width=max(1, round(width * SS)))

    def glow(self, x, y, r, color, alpha):
        """ふんわり光る円（中心が明るく、外へ行くほど消える）。"""
        m = Image.new("L", self.im.size, 0)
        ImageDraw.Draw(m).ellipse(((x - r * 0.55) * SS, (y - r * 0.55) * SS, (x + r * 0.55) * SS, (y + r * 0.55) * SS),
                                  fill=alpha)
        m = m.filter(ImageFilter.GaussianBlur(r * 0.4 * SS))
        self.im.paste(Image.new("RGB", self.im.size, color), (0, 0), m)

    def paste_masked(self, img: Image.Image, mask: Image.Image):
        self.im.paste(img, (0, 0), mask)

    def finish(self, blur: float = 0) -> Image.Image:
        """仕上げ。2倍の大きさのまま返す（scale=2 のカードにはそのまま使える）。"""
        return self.im.filter(ImageFilter.GaussianBlur(blur * SS)) if blur else self.im


def _vgrad(w: int, h: int, stops) -> Image.Image:
    """縦のグラデーション。stops = [(位置0〜1, (r, g, b)), ...]"""
    col = Image.new("RGB", (1, h))
    px = col.load()
    for y in range(h):
        t = y / max(1, h - 1)
        for (t0, c0), (t1, c1) in zip(stops, stops[1:]):
            if t <= t1 or (t1, c1) == stops[-1]:
                k = 0 if t1 == t0 else min(1.0, max(0.0, (t - t0) / (t1 - t0)))
                px[0, y] = tuple(round(a + (b - a) * k) for a, b in zip(c0, c1))
                break
    return col.resize((w, h), Image.BILINEAR)


def _ridge(rnd: random.Random, w: int, base: float, amp: float, waves=((0.006, 0.6), (0.017, 0.3), (0.043, 0.1))):
    """なだらかな山並みの輪郭（下まで閉じた多角形の点）。"""
    ph = [rnd.uniform(0, math.tau) for _ in waves]
    pts = [(x, base - amp * sum(a * (0.5 + 0.5 * math.sin(x * f + p)) for (f, a), p in zip(waves, ph)))
           for x in range(-10, w + 11, 6)]
    return pts + [(w + 10, 10_000), (-10, 10_000)]


def _petal_pts(x, y, length, width, angle, centered=False):
    """桜の花びら1枚（先が割れた形）。centered なら花びらの真ん中が (x, y)。"""
    top = [(t, 0.5 * math.sin(math.pi * t * 0.82)) for t in (i / 10 for i in range(11))]
    shape = top + [(0.86, 0.0)] + [(u, -v) for u, v in reversed(top)]
    ca, sa = math.cos(angle), math.sin(angle)
    off = 0.5 if centered else 0.0
    return [(x + ((u - off) * length) * ca - (v * width) * sa, y + ((u - off) * length) * sa + (v * width) * ca)
            for u, v in shape]


def _flower(p: _Pad, x, y, r, rot, color, alpha):
    for k in range(5):
        p.poly(_petal_pts(x, y, r, r * 0.95, rot + k * math.tau / 5), color + (alpha,))
    p.circle(x, y, r * 0.22, (176, 62, 104, min(255, alpha + 30)))


def _branch(p: _Pad, rnd, x, y, ang, length, thick, depth, color, spots):
    """曲がりながら細くなる枝。花を付ける場所を spots に集める。"""
    steps = 7
    seg = length / steps
    prev = (x, y)
    for i in range(steps):
        ang += rnd.uniform(-0.2, 0.2)
        x, y = x + math.cos(ang) * seg, y + math.sin(ang) * seg
        t = thick * (1 - 0.6 * (i + 1) / steps)
        p.line([prev, (x, y)], color, t)
        p.circle(x, y, t / 2, color)
        if t < 7:
            spots.append((x, y, t))
        if depth > 0 and i >= 1 and rnd.random() < 0.5:
            sub = ang + rnd.choice((-1, 1)) * rnd.uniform(0.5, 1.0)
            _branch(p, rnd, x, y, sub, length * rnd.uniform(0.32, 0.55), t * 0.65, depth - 1, color, spots)
        prev = (x, y)
    spots.append((x, y, 0))


# ---------------------------------------------------------------- 夜桜
def _yozakura(w: int, h: int) -> Image.Image:
    rnd = random.Random(17)
    p = _Pad(w, h, [(0, (14, 16, 44)), (0.55, (30, 24, 62)), (1, (42, 26, 64))])
    # 月とおぼろ雲
    mx, my = w * 0.79, h * 0.22
    p.glow(mx, my, 320, (96, 86, 150), 110)
    p.circle(mx, my, 60, (150, 144, 128))
    p.circle(mx - 8, my - 6, 52, (160, 154, 136, 140))
    p.blur(1.2)
    for cy, x0, x1, ry in ((my + 34, mx - 190, mx + 110, 13), (my + 56, mx - 90, mx + 200, 9)):
        for i in range(12):  # 月の下の方にかかる叢雲（楕円の重ね合わせ）
            cx = x0 + (x1 - x0) * i / 11
            p.ellipse(cx, cy + rnd.uniform(-3, 3), rnd.uniform(28, 40), ry * rnd.uniform(0.8, 1.2), (38, 32, 76, 235))
    # 遠くの山並み
    p.poly(_ridge(rnd, w, h * 0.72, h * 0.16), (36, 28, 70))
    p.poly(_ridge(rnd, w, h * 0.86, h * 0.12), (24, 19, 50))
    p.blur(3.5)
    # 桜の枝（左上と右下から）
    spots: list = []
    _branch(p, rnd, -20, h * 0.10, 0.18, w * 0.52, 22, 3, (19, 13, 30), spots)
    _branch(p, rnd, -10, h * 0.44, -0.30, w * 0.26, 13, 2, (19, 13, 30), spots)
    _branch(p, rnd, w + 20, h * 0.98, math.pi + 0.42, w * 0.38, 18, 3, (19, 13, 30), spots)
    for x, y, t in spots:
        for _ in range(1 if t else 2):
            fx, fy = x + rnd.uniform(-16, 16), y + rnd.uniform(-14, 14)
            color = rnd.choice([(196, 116, 152), (210, 138, 168), (182, 100, 138)])
            _flower(p, fx, fy, rnd.uniform(6, 11), rnd.uniform(0, math.tau), color, rnd.randint(120, 190))
    p.blur(1.6)
    # 舞う花びら
    for _ in range(46):
        x, y = rnd.uniform(0, w), rnd.uniform(0, h)
        size = rnd.uniform(5, 10)
        p.poly(_petal_pts(x, y, size, size * 0.8, rnd.uniform(0, math.tau), centered=True),
               rnd.choice([(226, 150, 182), (208, 126, 162)]) + (rnd.randint(90, 170),))
    return p.finish(0.5)


# ---------------------------------------------------------------- 富士と霞
def _fuji_y(x, cx, top, base, half, flat=26, power=1.75):
    d = abs(x - cx)
    if d <= flat:
        return top
    t = min(1.0, (d - flat) / (half - flat))
    return base - (base - top) * (1 - t) ** power


def _fuji(w: int, h: int) -> Image.Image:
    rnd = random.Random(5)
    p = _Pad(w, h, [(0, (12, 18, 50)), (0.40, (36, 32, 82)), (0.66, (104, 56, 96)), (0.86, (150, 78, 76)),
                    (1, (120, 60, 70))])
    for _ in range(60):  # 星
        p.circle(rnd.uniform(0, w), rnd.uniform(0, h * 0.42), rnd.uniform(0.5, 1.3),
                 (220, 220, 255, rnd.randint(50, 150)))
    # 沈みかけた日
    sx, sy = w * 0.25, h * 0.64
    p.glow(sx, sy, 280, (214, 104, 70), 110)
    p.circle(sx, sy, 50, (198, 86, 60))
    p.blur(1.5)
    # 富士
    cx, top, base, half = w * 0.62, h * 0.27, h * 0.90, w * 0.46
    xs = [cx - half + i * (2 * half) / 120 for i in range(121)]
    p.poly([(x, _fuji_y(x, cx, top, base, half)) for x in xs] + [(cx + half, h + 10), (cx - half, h + 10)], (36, 32, 78))
    # 雪（尾根に沿って下へ筋を引く）
    snow_line, streak = top + (base - top) * 0.20, (base - top) * 0.16
    sx0 = next(x for x in xs if _fuji_y(x, cx, top, base, half) <= snow_line + streak)
    sx1 = cx + (cx - sx0)
    upper = [(x, _fuji_y(x, cx, top, base, half)) for x in xs if sx0 <= x <= sx1]
    lower = []
    for i in range(80, -1, -1):
        x = sx0 + (sx1 - sx0) * i / 80
        ridge = max(0.0, math.sin(x * 0.17 + 1.3)) ** 2 * (0.55 + 0.45 * math.sin(x * 0.041 + 0.4))
        y = snow_line + streak * ridge * (1 - abs(x - cx) / (sx1 - cx + 1)) ** 0.5
        lower.append((x, max(y, _fuji_y(x, cx, top, base, half))))
    p.poly(upper + lower, (122, 126, 160, 230))
    p.blur(0.8)
    # 霞（横にたなびく、ふちのぼやけた帯）
    for y, x0, x1, a in ((h * 0.58, -80, w * 0.52, 46), (h * 0.69, w * 0.34, w + 80, 52),
                         (h * 0.79, -80, w * 0.80, 58), (h * 0.88, w * 0.18, w + 80, 50)):
        for i in range(14):
            cx_ = x0 + (x1 - x0) * i / 13
            p.ellipse(cx_, y + rnd.uniform(-2, 2), (x1 - x0) / 13 * 0.9, rnd.uniform(9, 13), (220, 184, 206, a))
    p.blur(3.2)
    # 手前の丘
    p.poly(_ridge(rnd, w, h * 0.96, h * 0.10), (20, 15, 38))
    return p.finish(0.5)


# ---------------------------------------------------------------- 青海波と金雲
def _seigaiha_layer(w: int, h: int, R: float, base, line) -> Image.Image:
    im = Image.new("RGB", (w * SS, h * SS), base)
    d = ImageDraw.Draw(im)
    rings = [(1.0, line), (0.92, base), (0.72, line), (0.64, base), (0.44, line), (0.36, base), (0.16, line)]
    rows = int(h / (R / 2)) + 4
    for j in range(-2, rows):
        y = j * R / 2
        off = R if j % 2 else 0
        for i in range(-1, int(w / (2 * R)) + 2):
            x = i * 2 * R + off
            for k, color in rings:
                r = R * k
                d.ellipse(((x - r) * SS, (y - r) * SS, (x + r) * SS, (y + r) * SS), fill=color)
    return im


def _gold_cloud(p: _Pad, rnd, x, y, length, hb):
    """すやり霞（金の雲）。角の丸い帯を段違いに重ね、金箔の四角を貼ったような質感にする。"""
    mask = Image.new("L", p.im.size, 0)
    md = ImageDraw.Draw(mask)
    bars = ((x, y, x + length), (x + length * 0.22, y - hb * 0.82, x + length * 0.86),
            (x - length * 0.12, y + hb * 0.82, x + length * 0.48))
    for x0, y0, x1 in bars:
        md.rounded_rectangle((x0 * SS, (y0 - hb / 2) * SS, x1 * SS, (y0 + hb / 2) * SS), radius=hb / 2 * SS, fill=255)
    # 金箔（少しずつ色の違う四角）
    leaf = Image.new("RGB", p.im.size, (128, 98, 44))
    ld = ImageDraw.Draw(leaf)
    s = 17
    for gy in range(int((y - hb * 1.4) // s) - 1, int((y + hb * 1.4) // s) + 2):
        for gx in range(int((x - length * 0.15) // s) - 1, int((x + length * 1.05) // s) + 2):
            k = rnd.uniform(0.88, 1.06)
            ld.rectangle((gx * s * SS + 1, gy * s * SS + 1, (gx + 1) * s * SS - 1, (gy + 1) * s * SS - 1),
                         fill=tuple(round(c * k) for c in (150, 118, 54)))
    edge = ImageChops.subtract(mask.filter(ImageFilter.MaxFilter(5)), mask)
    p.paste_masked(leaf, mask.point(lambda v: v * 0.9))
    p.paste_masked(Image.new("RGB", p.im.size, (96, 72, 30)), edge)


def _seigaiha(w: int, h: int) -> Image.Image:
    rnd = random.Random(11)
    p = _Pad(w, h, [(0, (16, 24, 64)), (1, (10, 16, 46))])
    waves = _seigaiha_layer(w, h, 26, (16, 24, 62), (40, 58, 116))
    fade = _vgrad(w * SS, h * SS, [(0, (0, 0, 0)), (0.25, (40, 40, 40)), (0.7, (255, 255, 255)), (1, (255, 255, 255))])
    p.paste_masked(waves, fade.convert("L"))
    # 朱の日輪
    sx, sy = w * 0.80, h * 0.26
    p.glow(sx, sy, 260, (170, 62, 46), 60)
    p.circle(sx, sy, 92, (150, 46, 36))
    p.blur(0.6)
    # 金の雲
    _gold_cloud(p, rnd, -40, h * 0.11, w * 0.52, 30)
    _gold_cloud(p, rnd, w * 0.52, h * 0.37, w * 0.56, 28)
    _gold_cloud(p, rnd, -30, h * 0.84, w * 0.40, 26)
    return p.finish(0.4)
