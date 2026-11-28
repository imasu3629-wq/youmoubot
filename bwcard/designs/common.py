"""デザイン共通の小物（数値の書式・比率の色・スキン・サンプルデータ）。"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from functools import lru_cache

from PIL import Image, ImageDraw, ImageFilter

from .. import skin as SK
from ..gfx import Card
from ..models import MODES, PlayerStats


def n(v) -> str:
    """80928 → '80,928'"""
    return f"{int(v):,}"


def r2(v) -> str:
    """比率は小数2桁"""
    return f"{v:,.2f}"


def compact(v) -> str:
    """47360000 → '47.36M'、100万未満はカンマ区切り"""
    v = int(v)
    for div, suf in ((1_000_000_000, "B"), (1_000_000, "M")):
        if abs(v) >= div:
            return f"{v / div:.2f}{suf}"
    return n(v)


def short(v) -> str:
    """場所が足りないとき用: 74074 → '74.1K'、1234567 → '1.23M'"""
    v = int(v)
    if abs(v) >= 999_950:  # 1000.0K ではなく 1.00M にする
        return f"{v / 1_000_000:.2f}M" if abs(v) < 999_995_000 else f"{v / 1_000_000_000:.2f}B"
    if abs(v) >= 10_000:
        return f"{v / 1000:.1f}K"
    return n(v)


def r_short(v: float) -> str:
    """比率を短く: 19.77 / 123.5 / 7,407 / 74.1K"""
    if v < 100:
        return f"{v:.2f}"
    if v < 1000:
        return f"{v:.1f}"
    return short(v)


def date(dt: datetime | None) -> str:
    return dt.astimezone().strftime("%Y/%m/%d") if dt else "N/A"


def ago(dt: datetime | None) -> str:
    if not dt:
        return "N/A"
    sec = (datetime.now(timezone.utc) - dt).total_seconds()
    for unit, size in (("d", 86400), ("h", 3600), ("m", 60)):
        if sec >= size:
            return f"{int(sec // size)}{unit} ago"
    return "just now"


def ratio_color(v: float) -> str:
    """比率の高さで色を変える（低い→灰、高い→ピンク/水色）。"""
    for limit, code in ((1, "7"), (2, "f"), (4, "e"), (7, "6"), (12, "c"), (20, "d")):
        if v < limit:
            return code
    return "b"


def mode_title(mode: str) -> str:
    return MODES.get(mode, ("", mode.title()))[1]


# ---------------------------------------------------------------- 背景・飾り
@lru_cache(maxsize=24)
def default_background(W: int, H: int, seed: int = 3) -> Image.Image:
    """写真がないとき用の背景。ぼかした夜空とブロックの光で、ゲーム画面のような雰囲気に。"""
    rnd = random.Random(seed)
    c = Card(W, H, 1, (14, 12, 30))
    c.rrect(0, 0, W, H, 0, gradient=[(26, 20, 58), (18, 30, 60), (10, 36, 48)], gradient_dir="v")
    for cx, cy, r, col, a in ((0.2, 0.15, 0.55, (124, 77, 255), 150), (0.85, 0.85, 0.6, (0, 194, 168), 120),
                              (0.9, 0.1, 0.35, (255, 170, 60), 70), (0.4, 1.0, 0.5, (90, 60, 220), 110)):
        c.glow(W * cx, H * cy, W * r, col, a)
    img = c.im.convert("RGB")
    d = ImageDraw.Draw(img, "RGBA")  # RGB画像に描くと半透明色がちゃんと混ざる
    for _ in range(34):  # 浮かぶブロック（ぼかすと玉ボケ風）
        s_ = rnd.randint(W // 50, W // 14)
        x, y = rnd.randint(-s_, W), rnd.randint(-s_, H)
        col = rnd.choice([(255, 255, 255), (170, 130, 255), (90, 230, 210), (255, 200, 120)])
        d.rectangle((x, y, x + s_, y + s_), fill=col + (rnd.randint(8, 26),))
    return img.filter(ImageFilter.GaussianBlur(W / 150))


def floor_shadow(card: Card, cx: float, y: float, w: float, alpha=110):
    """足もとの楕円の影。"""
    W, H = card.p(w), card.p(w * 0.22)
    pad = H
    m = Image.new("L", (W + pad * 2, H + pad * 2), 0)
    ImageDraw.Draw(m).ellipse((pad, pad, pad + W - 1, pad + H - 1), fill=alpha)
    m = m.filter(ImageFilter.GaussianBlur(H / 4))
    card._fill_mask(card.p(cx) - W // 2 - pad, card.p(y) - H // 2 - pad, m, (0, 0, 0, 255))


def pixel_bar(card: Card, x, y, w, h, frac, fill=(85, 255, 255), empty=(255, 255, 255, 34), segments=20, gap=2):
    """Hypixel風の区切りつき進捗バー。"""
    seg_w = (w - gap * (segments - 1)) / segments
    filled = frac * segments
    for i in range(segments):
        sx = x + i * (seg_w + gap)
        card.rrect(sx, y, seg_w, h, 0, fill=empty)
        part = max(0.0, min(1.0, filled - i))
        if part > 0:
            card.rrect(sx, y, seg_w * part, h, 0, fill=fill)


# ---------------------------------------------------------------- スキン
def body_image(skin: Image.Image | None, height_px: int, slim: bool = False, **kw) -> Image.Image:
    return SK.render(skin if skin is not None else SK.silhouette_skin(), height_px, slim=slim, **kw)


def sample_stats() -> PlayerStats:
    """プレビュー用の架空プレイヤー。"""
    from datetime import timedelta

    from ..models import ModeStats
    m = {
        "solo": ModeStats(1820, 1302, 518, 3410, 2988, 4880, 498, 2140, 500, 3, 214_000, 41_200, 6_400, 2_210),
        "doubles": ModeStats(5120, 4296, 824, 10120, 8640, 13980, 790, 6420, 760, 11, 702_000, 140_300, 21_900, 7_440),
        "threes": ModeStats(2410, 2105, 305, 4920, 4190, 7310, 290, 3010, 280, 6, 344_000, 70_100, 9_800, 3_620),
        "fours": ModeStats(5300, 4890, 410, 10870, 9112, 14960, 412, 7110, 380, 24, 790_000, 152_400, 21_800, 8_400),
        "4v4": ModeStats(405, 247, 158, 798, 500, 462, 114, 250, 60, 2, 50_000, 8_000, 1_304, 448),
    }
    overall = ModeStats()
    for ms in m.values():
        for k in ("games", "wins", "losses", "kills", "deaths", "final_kills", "final_deaths",
                  "beds_broken", "beds_lost", "iron", "gold", "diamond", "emerald"):
            setattr(overall, k, getattr(overall, k) + getattr(ms, k))
    overall.winstreak = 24
    now = datetime.now(timezone.utc)
    # 2347☆ 付近になるよう経験値を計算（23プレステージ + 47レベル + 少し）
    exp = 23 * 487_000 + 7_000 + 43 * 5_000 + 2_310
    return PlayerStats(
        username="ExamplePlayer", uuid="", rank="§6[MVP§c++§6]", exp=exp, level_cosmetics={},
        tokens=3_284_000, slumber_tickets=4_230, slumber_total=312_880, slumber_wallet=10_000,
        modes={"overall": overall, **m}, first_login=datetime(2019, 3, 14, tzinfo=timezone.utc),
        last_login=now - timedelta(hours=3), online=False, network_exp=23_400_000, karma=8_120_455,
        achievement_points=9_870, guild_name="Example Guild", guild_tag="JPN", guild_tag_color="3",
    )
