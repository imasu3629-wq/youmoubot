"""和風 — 藍の縁取りの和紙に、筆の題と朱印。見やすさ優先で、数字は大きく、言葉は一般的な用語で。"""
from __future__ import annotations

import math
import os
import random
from functools import lru_cache

from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont

from .. import prestige as P
from ..gfx import Card
from ..models import PlayerStats
from ..text import FONT_DIR, strip_codes
from . import common as C

W, H = 800, 460

PAPER = (241, 233, 215)
INK = (30, 26, 22)
USUZUMI = (110, 102, 94)     # 薄墨（ラベル）
SHU = (194, 59, 34)          # 朱
KIN = (150, 116, 44)         # 金
AI = (27, 44, 67)            # 藍

FONTS = {
    "brush": "YujiBoku-Regular.ttf",
    "mincho": "ZenOldMincho-Regular.ttf",
    "mincho_sb": "ZenOldMincho-SemiBold.ttf",
    "mincho_bk": "ZenOldMincho-Black.ttf",
}

# プレステージ（配色の名前）→ 印に彫る漢字
PRESTIGE_KANJI = {
    "none": "石", "iron": "鉄", "gold": "金", "diamond": "金剛", "emerald": "翠玉", "sapphire": "蒼玉",
    "ruby": "紅玉", "crystal": "水晶", "opal": "遊色", "amethyst": "紫晶", "rainbow": "虹",
    "iron_prime": "極鉄", "gold_prime": "極金", "diamond_prime": "極剛", "emerald_prime": "極翠",
    "sapphire_prime": "極蒼", "ruby_prime": "極紅", "crystal_prime": "極晶", "opal_prime": "極遊",
    "amethyst_prime": "極紫", "mirror": "鏡", "light": "光", "dawn": "暁", "dusk": "黄昏", "air": "空",
    "wind": "風", "nebula": "星雲", "thunder": "雷", "earth": "大地", "water": "水", "fire": "炎",
    "sunrise": "日出", "eclipse": "日蝕", "gamma": "閃光", "majestic": "荘厳", "andesine": "長石",
    "marine": "海", "element": "元素", "galaxy": "銀河", "atomic": "原子", "sunset": "夕陽", "time": "時",
    "winter": "冬", "obsidian": "黒曜", "spring": "春", "ice": "氷", "summer": "夏", "spinel": "尖晶",
    "autumn": "秋", "mystic": "神秘", "eternal": "永劫", "burnout": "燃尽", "cooldown": "冷却",
    "obliteration": "消滅", "ender": "終焉", "brust": "炸裂", "comical": "滑稽", "lusterlost": "褪色",
    "maelstrom": "渦潮", "time_undone": "逆行", "umbrella": "傘", "luminous": "光輝",
    "tortilla": "トルティーヤ", "corn": "玉蜀黍", "bittersweet": "甘苦", "sweetsour": "甘酸", "pop": "ポップ",
    "bubblegum": "風船", "contrast": "対照", "blended": "調和", "allay": "アレイ", "blaze": "烈火",
    "creeper": "クリーパー", "drowned": "ドラウンド", "enderman": "エンダーマン", "frog": "蛙",
    "ghast": "ガスト", "hoglin": "ホグリン", "iron_golem": "鉄巨人", "jerry": "ジェリー",
    "kringle": "クリングル", "liquid": "液体", "mint": "薄荷", "neglected": "放置", "onion": "玉葱",
    "poser": "ポーザー", "quartz": "石英", "rich": "富豪", "sanguine": "血潮", "titanic": "巨大",
    "unorthodox": "異端", "volcanic": "火山", "weeping_cherry": "枝垂桜", "x_ray": "透視", "yearn": "憧憬",
    "zebra": "縞馬", "caution": "警告", "indescribable": "不可説", "forgotten": "忘却", "fuse": "導火",
    "prestigious": "至高",
}
MODE_JP = {"overall": "総合", "solo": "ソロ", "doubles": "ダブルス", "threes": "3v3v3v3", "fours": "4v4v4v4",
           "4v4": "4v4"}


# ---------------------------------------------------------------- 文字
@lru_cache(maxsize=128)
def font(kind: str, px: int) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(os.path.join(FONT_DIR, FONTS[kind]), max(1, px))


def ink(card: Card, mask: Image.Image, color=INK, alpha=255, bleed=0.0, at=(0, 0)):
    """墨で描く。mask は at（実ピクセル）に置く。bleed で紙ににじませる。"""
    if bleed:
        pad = card.p(bleed) * 3
        big = Image.new("L", (mask.width + pad * 2, mask.height + pad * 2), 0)
        big.paste(mask, (pad, pad))
        soft = big.filter(ImageFilter.GaussianBlur(card.p(bleed)))
        card._fill_mask(at[0] - pad, at[1] - pad, soft.point(lambda v: int(v * 0.3)), color + (alpha,))
    card._fill_mask(at[0], at[1], mask, color + (alpha,))


def text(card: Card, s, x, y, kind="mincho", size=16, color=INK, anchor="la", alpha=255) -> float:
    """横書きの文字。描いた幅（論理px）を返す。"""
    f = font(kind, card.p(size))
    l, t, r, b = f.getbbox(s, anchor=anchor)
    pad = 4
    m = Image.new("L", (r - l + pad * 2, b - t + pad * 2), 0)
    ImageDraw.Draw(m).text((pad - l, pad - t), s, font=f, fill=255, anchor=anchor)
    ink(card, m, color, alpha, at=(card.p(x) + l - pad, card.p(y) + t - pad))
    return f.getlength(s) / card.s


def width(card: Card, s, kind, size) -> float:
    return font(kind, card.p(size)).getlength(s) / card.s


def fit_size(card: Card, s, kind, size, max_w, min_size=10) -> float:
    while size > min_size and width(card, s, kind, size) > max_w:
        size -= 1
    return size


def vertical(card: Card, s, cx, y, kind, size, color=INK, gap=0.06, bleed=0.0) -> float:
    """縦書き。下端のy座標（論理px）を返す。"""
    f = font(kind, card.p(size))
    S = card.p(size)
    step = size * (1 + gap)
    m = Image.new("L", (S * 2, card.p(step * len(s)) + S), 0)
    d = ImageDraw.Draw(m)
    for i, ch in enumerate(s):
        d.text((S, card.p(step * i + size / 2)), ch, font=f, fill=255, anchor="mm")
    ink(card, m, color, 255, bleed, at=(card.p(cx) - S, card.p(y)))
    return y + step * len(s)


# ---------------------------------------------------------------- 布と紙
@lru_cache(maxsize=4)
def cloth(Wp: int, Hp: int) -> Image.Image:
    """藍染めの布（細かい織り目とむら）。"""
    base = Image.new("RGB", (Wp, Hp), AI)
    n = Image.effect_noise((Wp, Hp), 18).convert("L")
    mott = Image.effect_noise((max(1, Wp // 60), max(1, Hp // 60)), 40).convert("L").resize((Wp, Hp), Image.BICUBIC)
    tex = Image.blend(n, mott, 0.5)
    out = Image.composite(Image.new("RGB", (Wp, Hp), (56, 80, 112)), Image.new("RGB", (Wp, Hp), (16, 26, 40)), tex)
    return Image.blend(base, out, 0.4).convert("RGBA")


@lru_cache(maxsize=4)
def paper(Wp: int, Hp: int, seed: int = 5) -> Image.Image:
    """和紙（ほんのり繊維が見える程度）。ふちは少しだけ不揃いに。"""
    rnd = random.Random(seed)
    mott = Image.effect_noise((max(1, Wp // 90), max(1, Hp // 90)), 30).convert("L").resize((Wp, Hp), Image.BICUBIC)
    img = Image.composite(Image.new("RGB", (Wp, Hp), (245, 238, 222)), Image.new("RGB", (Wp, Hp), (236, 227, 207)),
                          mott)
    d = ImageDraw.Draw(img, "RGBA")
    sc = Wp / 1560
    for _ in range(int(500 * sc * sc) + 120):  # 白っぽい細い繊維
        x, y = rnd.uniform(0, Wp), rnd.uniform(0, Hp)
        ang = rnd.uniform(0, math.pi)
        length = rnd.uniform(30, 140) * sc
        pts = []
        for k in range(8):
            ang += rnd.uniform(-0.25, 0.25)
            pts.append((x + math.cos(ang) * length * k / 7, y + math.sin(ang) * length * k / 7))
        d.line(pts, fill=(255, 253, 246, rnd.randint(40, 90)), width=max(1, round(sc * 1.4)))
    img = img.filter(ImageFilter.GaussianBlur(1.2 * sc))
    # 少し不揃いな耳（ふち）
    mask = Image.new("L", (Wp, Hp), 0)
    edge = 6 * sc * 2

    def wobble(n):
        vals, v = [], 0.0
        for _ in range(n):
            v = v * 0.75 + rnd.uniform(-1, 1)
            vals.append(v)
        return vals

    N = 200
    t_, r_, b_, l_ = wobble(N), wobble(N), wobble(N), wobble(N)
    poly = [(i / (N - 1) * Wp, edge + t_[i] * edge * 0.35) for i in range(N)]
    poly += [(Wp - edge + r_[i] * edge * 0.35, i / (N - 1) * Hp) for i in range(N)]
    poly += [((1 - i / (N - 1)) * Wp, Hp - edge + b_[i] * edge * 0.35) for i in range(N)]
    poly += [(edge + l_[i] * edge * 0.35, (1 - i / (N - 1)) * Hp) for i in range(N)]
    ImageDraw.Draw(mask).polygon(poly, fill=255)
    out = img.convert("RGBA")
    out.putalpha(mask.filter(ImageFilter.GaussianBlur(1.0 * sc * 2)))
    return out


# ---------------------------------------------------------------- 印
@lru_cache(maxsize=32)
def _noise(size, sigma, seed) -> Image.Image:
    """毎回同じになるノイズ（モードを切り替えても印の欠け方が変わらないように）。"""
    rnd = random.Random(seed)
    data = bytes(max(0, min(255, int(rnd.gauss(128, sigma)))) for _ in range(size[0] * size[1]))
    return Image.frombytes("L", size, data)


def _glyph_centered(img: Image.Image, ch: str, f, cx: float, cy: float, fill: int):
    """字の「墨の範囲」の中心を (cx, cy) に合わせて描く。長音は縦向きに回す。"""
    l, t, r, b = f.getbbox(ch)
    g = Image.new("L", (r - l + 4, b - t + 4), 0)
    ImageDraw.Draw(g).text((2 - l, 2 - t), ch, font=f, fill=255)
    if ch in "ー〜":
        g = g.rotate(-90, expand=True)
    x, y = int(cx - g.width / 2), int(cy - g.height / 2)
    img.paste(fill, (x, y, x + g.width, y + g.height), g)


def seal(card: Card, chars: str, cx, cy, size, rotate=-4.0, seed=1):
    """朱印（白文：赤地に白い字）。5文字以上は縦長の印にする。"""
    rnd = random.Random(seed)
    S = card.p(size)
    n = len(chars)
    tall = n >= 5
    w, h = (S * 0.62, S * 1.5) if tall else (S, S)
    pad = S // 6
    big = Image.new("L", (int(w) + pad * 2, int(h) + pad * 2), 0)
    x0, y0 = pad, pad
    ImageDraw.Draw(big).rounded_rectangle((x0, y0, x0 + w, y0 + h), radius=S // 14, fill=255)
    cols = [chars] if (tall or n <= 2) else [chars[:math.ceil(n / 2)], chars[math.ceil(n / 2):]]
    inner_w, inner_h = w * 0.8, h * 0.84
    col_w = inner_w / len(cols)
    for ci, col in enumerate(cols):  # 右の列から読む
        ccx = x0 + w - (w - inner_w) / 2 - col_w * (ci + 0.5)
        cell = inner_h / len(col)
        f = font("mincho_bk", int(min(col_w * 0.9, cell * 0.92)))
        for ri, ch in enumerate(col):
            _glyph_centered(big, ch, f, ccx, y0 + (h - inner_h) / 2 + cell * (ri + 0.5), 0)
    # 押したときのムラと欠けを少しだけ
    blot = _noise((max(2, big.width // 7), max(2, big.height // 7)), 50, seed) \
        .resize(big.size, Image.BICUBIC).point(lambda v: 0 if v < 38 else 255)
    speck = _noise(big.size, 64, seed + 1).point(lambda v: 0 if v < 12 else 255)
    m = ImageChops.multiply(ImageChops.multiply(big, blot), speck)
    for _ in range(2):
        ex, ey = rnd.uniform(0, big.width), rnd.uniform(0, big.height)
        r = rnd.uniform(S * 0.01, S * 0.02)
        ImageDraw.Draw(m).ellipse((ex - r, ey - r, ex + r, ey + r), fill=0)
    m = m.filter(ImageFilter.GaussianBlur(max(0.6, S / 160)))
    m = m.rotate(rotate, resample=Image.BICUBIC, expand=True)
    card._fill_mask(card.p(cx) - m.width // 2, card.p(cy) - m.height // 2, m.point(lambda v: int(v * 0.92)),
                    SHU + (255,))


# ---------------------------------------------------------------- 筆の線
def brush_line(card: Card, x0, y, x1, thick, color=INK, alpha=235, seed=0, head=0.06, tail=0.25):
    """筆で引いた横線（入りが少し太く、払いで細くなる）。"""
    X0, X1 = card.p(x0), card.p(x1)
    T_ = thick * card.s
    if X1 - X0 < 2:
        return
    pad = int(T_ * 2) + 4
    N = max(24, (X1 - X0) // 6)
    top, bot = [], []
    for i in range(N + 1):
        t = i / N
        prof = 0.6 + 0.55 * (t / head) if t < head else (max(0.06, (1 - t) / tail) ** 0.8 if t > 1 - tail else 1.0)
        half = T_ / 2 * prof
        x = (X1 - X0) * t + pad
        yc = pad + T_
        top.append((x, yc - half))
        bot.append((x, yc + half))
    m = Image.new("L", (X1 - X0 + pad * 2, int(T_ * 2 + pad * 2)), 0)
    ImageDraw.Draw(m).polygon(top + bot[::-1], fill=255)
    m = m.filter(ImageFilter.GaussianBlur(max(0.5, T_ / 10)))
    ink(card, m, color, alpha, at=(X0 - pad, card.p(y) - pad - int(T_)))


# ---------------------------------------------------------------- 丸窓の山水
@lru_cache(maxsize=4)
def ink_landscape(Wp: int, Hp: int, seed=3) -> Image.Image:
    rnd = random.Random(seed)
    layer = Image.new("RGBA", (Wp, Hp), (0, 0, 0, 0))
    for base, amp, alpha, fade in ((0.55, 0.15, 34, 0.30), (0.68, 0.12, 58, 0.24), (0.8, 0.09, 90, 0.18)):
        ph = [rnd.uniform(0, 6.28) for _ in range(4)]
        m = Image.new("L", (Wp, Hp), 0)
        px = m.load()
        for x in range(Wp):
            t = x / Wp
            v = (math.sin(t * 5.5 + ph[0]) * 0.55 + math.sin(t * 11 + ph[1]) * 0.25 +
                 math.sin(t * 23 + ph[2]) * 0.12 + abs(math.sin(t * 3 + ph[3])) * 0.4)
            r = Hp * (base - amp * v)
            for y in range(max(0, int(r)), Hp):
                px[x, y] = int(alpha * max(0.0, 1 - (y - r) / (Hp * fade)) ** 1.6)
        m = m.filter(ImageFilter.GaussianBlur(Wp / 260))
        col = Image.new("RGBA", (Wp, Hp), INK + (255,))
        col.putalpha(m)
        layer.alpha_composite(col)
    return layer


def round_window(card: Card, skin, slim, cx, cy, r):
    D = card.p(r * 2)
    win = Image.new("RGBA", (D, D), (0, 0, 0, 0))
    win.alpha_composite(ink_landscape(D, D, seed=7))
    body = C.body_image(skin, int(D * 0.74), slim=slim)
    foot = Image.new("L", (D, D), 0)
    fy = int(D * 0.17) + body.height - D // 40
    ImageDraw.Draw(foot).ellipse((D * 0.3, fy - D * 0.035, D * 0.7, fy + D * 0.035), fill=110)
    shadow = Image.new("RGBA", (D, D), INK + (255,))
    shadow.putalpha(foot.filter(ImageFilter.GaussianBlur(D / 60)))
    win.alpha_composite(shadow)
    win.alpha_composite(body, (D // 2 - body.width // 2, int(D * 0.17)))
    circle = Image.new("L", (D * 4, D * 4), 0)
    ImageDraw.Draw(circle).ellipse((0, 0, D * 4 - 1, D * 4 - 1), fill=255)
    win.putalpha(ImageChops.multiply(win.getchannel("A"), circle.resize((D, D), Image.LANCZOS)))
    card.im.alpha_composite(win, (card.p(cx - r), card.p(cy - r)))
    ring = Image.new("L", (D * 4 + 32, D * 4 + 32), 0)
    ImageDraw.Draw(ring).ellipse((16, 16, D * 4 + 15, D * 4 + 15), outline=255, width=max(4, card.p(2) * 4))
    ring = ring.resize(((D * 4 + 32) // 4, (D * 4 + 32) // 4), Image.LANCZOS)
    card._fill_mask(card.p(cx - r) - 4, card.p(cy - r) - 4, ring, INK + (230,))


# ---------------------------------------------------------------- 本体
def render(stats: PlayerStats, mode: str = "overall", *, skin: Image.Image | None = None, slim: bool = False,
           scale: float = 2, footer: str = "", **_) -> Card:
    c = Card(W, H, scale, AI)
    c.im.alpha_composite(cloth(c.im.width, c.im.height))
    m = stats.mode(mode)

    # 和紙
    px0, py0, pw, ph = 14, 14, W - 28, H - 28
    c.shadow(px0, py0, pw, ph, 4, blur=10, offset=(0, 4), color=(0, 0, 0, 140))
    c.im.alpha_composite(paper(c.p(pw), c.p(ph)), (c.p(px0), c.p(py0)))

    # ---- 右上：筆の題 ----
    vertical(c, "寝台戦争", 742, 32, "brush", 42, bleed=1.0)

    # ---- 左上：丸窓・名前・レベル・印 ----
    round_window(c, skin, slim, 118, 128, 86)
    nx = 228
    ns = fit_size(c, stats.username, "mincho_sb", 38, 440, 20)
    text(c, stats.username, nx, 86, "mincho_sb", ns, INK, "ls")
    x = nx
    rank = "".join(ch for ch in strip_codes(stats.rank) if ch not in "[]")
    if rank:
        x += text(c, rank, x, 120, "mincho_sb", 19, KIN, "ls") + 14
    if stats.guild_tag:
        text(c, f"【{stats.guild_tag}】", x, 120, "mincho_sb", 19, INK, "ls")
    ly = 186 if (rank or stats.guild_tag) else 162   # ランクもギルドも無いときは詰める
    lw = text(c, f"{stats.level}", nx, ly, "mincho_bk", 46, INK, "ls")
    sw = text(c, "星", nx + lw + 6, ly, "mincho_sb", 22, INK, "ls")
    key = stats.level_cosmetics.get("scheme") or P.default_scheme(stats.level)
    kanji = PRESTIGE_KANJI.get(key, "星")
    seal(c, kanji, nx + lw + sw + 56, ly - 22, 60 if len(kanji) < 5 else 50, rotate=-3, seed=stats.level % 97)

    # ---- 中段：比率4つを大きく ----
    top = 236
    brush_line(c, 36, top, W - 36, 2.4, INK, 220, seed=11, tail=0.12)
    cells = [("FKDR", m.fkdr, m.final_kills, m.final_deaths), ("WLR", m.wlr, m.wins, m.losses),
             ("BBLR", m.bblr, m.beds_broken, m.beds_lost), ("KDR", m.kdr, m.kills, m.deaths)]
    cw = (W - 72) / 4
    for i, (label, v, a, b) in enumerate(cells):
        x = 36 + i * cw + 18
        if i:
            c.rrect(36 + i * cw, top + 16, 1.2, 100, 0, fill=INK + (60,))
        text(c, label, x, top + 34, "mincho_sb", 19, USUZUMI, "ls")
        val = C.r_short(v)
        text(c, val, x, top + 92, "mincho_bk", fit_size(c, val, "mincho_bk", 50, cw - 30, 24), INK, "ls")
        sub = f"{C.n(a)} / {C.n(b)}"
        if width(c, sub, "mincho_sb", 17) > cw - 26:
            sub = f"{C.short(a)} / {C.short(b)}"
        text(c, sub, x, top + 120, "mincho_sb", 17, USUZUMI, "ls")
    brush_line(c, 36, top + 140, W - 36, 2.4, INK, 220, seed=12, tail=0.12)

    # ---- 下段：連勝・試合・勝率・モード ----
    yb = top + 186
    ws = "?" if m.winstreak is None else C.n(m.winstreak)
    x = 54
    for lab, val in (("連勝", ws), ("試合", C.n(m.games)), ("勝率", f"{m.win_rate:.1f}%")):
        x += text(c, lab, x, yb, "mincho_sb", 17, USUZUMI, "ls") + 8
        x += text(c, val, x, yb, "mincho_sb", 25, INK, "ls") + 34
    mode_txt = MODE_JP.get(mode, mode)
    text(c, mode_txt, W - 54, yb, "mincho_sb", 22, SHU, "rs")
    if footer:
        text(c, footer, W - 54 - width(c, mode_txt, "mincho_sb", 22) - 20, yb, "mincho", 13, USUZUMI, "rs")
    return c
