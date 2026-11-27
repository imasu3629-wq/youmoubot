import datetime
import functools
import io
import os
import random

import requests
from PIL import Image, ImageDraw, ImageFilter, ImageFont, ImageOps

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BG_PATH = os.path.join(BASE_DIR, "assets", "card_bg.png")
FONT_DIR = os.path.join(BASE_DIR, "fonts")
JST = datetime.timezone(datetime.timedelta(hours=9))

WHITE = (255, 255, 255)
MUTED = (178, 182, 210)
DIM = (126, 130, 160)
ACCENT = ((124, 92, 255), (255, 92, 168))
WIN, LOSE, TIE = (92, 255, 150), (255, 105, 125), (255, 222, 92)
MEDALS = [(255, 204, 64), (214, 220, 232), (222, 142, 82)]

PRESTIGE = [
    (170, 170, 170), (255, 255, 255), (255, 170, 0), (85, 255, 255), (0, 170, 0),
    (0, 170, 170), (170, 0, 0), (255, 85, 255), (85, 85, 255), (170, 0, 170),
]
RAINBOW = [(255, 85, 85), (255, 170, 0), (255, 255, 85), (85, 255, 85), (85, 255, 255), (255, 85, 255), (170, 0, 170)]
RANK_COLORS = {"MVP++": (255, 170, 0), "MVP+": (85, 255, 255), "MVP": (85, 255, 255), "VIP+": (85, 255, 85), "VIP": (85, 255, 85)}


@functools.lru_cache(maxsize=64)
def font(weight: str, size: int):
    return ImageFont.truetype(os.path.join(FONT_DIR, f"Poppins-{weight}.ttf"), size)


@functools.lru_cache(maxsize=16)
def symbol_font(size: int):
    return ImageFont.truetype(os.path.join(FONT_DIR, "DejaVuSans.ttf"), size)


def fit_font(draw, text, weight, size, max_w, min_size=16):
    while size > min_size and draw.textlength(text, font=font(weight, size)) > max_w:
        size -= 2
    return font(weight, size)


def fmt(v):
    return f"{v:,}" if isinstance(v, int) else str(v)


def ratio_color(v):
    for limit, color in ((10, (255, 85, 85)), (7, (255, 85, 255)), (5, (255, 170, 0)), (3, (85, 255, 85)), (2, (85, 255, 255)), (1, (255, 255, 85))):
        if v >= limit:
            return color
    return (200, 200, 200)


def winstreak_color(v):
    for limit, color in ((100, (255, 85, 85)), (50, (255, 85, 255)), (25, (255, 170, 0)), (10, (85, 255, 85))):
        if v >= limit:
            return color
    return WHITE


def rank_color(rank: str):
    label = rank.strip("[]")
    if not label:
        return (200, 200, 200)
    return RANK_COLORS.get(label, (255, 85, 85))


# --- 背景テンプレート ---
def make_background(w=1200, h=675) -> Image.Image:
    top, bottom = (14, 16, 36), (44, 18, 66)
    mask = Image.linear_gradient("L").resize((w, h))
    img = Image.composite(Image.new("RGB", (w, h), bottom), Image.new("RGB", (w, h), top), mask).convert("RGBA")

    glow = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    g = ImageDraw.Draw(glow)
    g.ellipse((w * 0.55, -h * 0.5, w * 1.35, h * 0.6), fill=(110, 80, 255, 110))
    g.ellipse((-w * 0.35, h * 0.45, w * 0.45, h * 1.5), fill=(255, 70, 150, 80))
    img = Image.alpha_composite(img, glow.filter(ImageFilter.GaussianBlur(140)))

    blocks = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    b = ImageDraw.Draw(blocks)
    rnd = random.Random(42)
    cell = 30
    for x in range(0, w, cell):
        for y in range(0, h, cell):
            if rnd.random() < 0.07:
                b.rectangle((x + 1, y + 1, x + cell - 2, y + cell - 2), fill=(255, 255, 255, rnd.randint(6, 18)))
    for x in range(0, w, cell):
        b.line((x, 0, x, h), fill=(255, 255, 255, 6))
    for y in range(0, h, cell):
        b.line((0, y, w, y), fill=(255, 255, 255, 6))
    img = Image.alpha_composite(img, blocks)

    vignette = Image.new("L", (w, h), 0)
    ImageDraw.Draw(vignette).ellipse((-w * 0.2, -h * 0.3, w * 1.2, h * 1.3), fill=255)
    vignette = ImageOps.invert(vignette.filter(ImageFilter.GaussianBlur(120)))
    shade = Image.new("RGBA", (w, h), (4, 4, 12, 0))
    shade.putalpha(vignette.point(lambda a: int(a * 0.7)))
    return Image.alpha_composite(img, shade)


def load_background(w, h) -> Image.Image:
    if os.path.exists(BG_PATH):
        return ImageOps.fit(Image.open(BG_PATH).convert("RGBA"), (w, h), Image.LANCZOS)
    return make_background(w, h)


# --- 描画ヘルパー ---
def fetch_image(url: str):
    try:
        res = requests.get(url, timeout=8)
        if res.status_code == 200 and res.headers.get("content-type", "").startswith("image"):
            return Image.open(io.BytesIO(res.content)).convert("RGBA")
    except Exception:
        pass
    return None


def fetch_skin(uuid: str):
    return fetch_image(f"https://mc-heads.net/body/{uuid}/240")


def fetch_head(uuid: str, size: int = 128):
    return fetch_image(f"https://mc-heads.net/avatar/{uuid.replace('-', '')}/{size}")


def panels(img, boxes, radius=22, fill=(255, 255, 255, 16), outline=(255, 255, 255, 36)):
    layer = Image.new("RGBA", img.size, (0, 0, 0, 0))
    d = ImageDraw.Draw(layer)
    for box in boxes:
        if len(box) == 5:
            *box, box_fill = box
        else:
            box_fill = fill
        d.rounded_rectangle(box, radius=radius, fill=box_fill, outline=outline, width=2)
    return Image.alpha_composite(img, layer)


def accent_line(img, x0, x1, y, height=4):
    d = ImageDraw.Draw(img)
    (r0, g0, b0), (r1, g1, b1) = ACCENT
    for x in range(x0, x1):
        t = (x - x0) / max(x1 - x0, 1)
        d.line((x, y, x, y + height), fill=(int(r0 + (r1 - r0) * t), int(g0 + (g1 - g0) * t), int(b0 + (b1 - b0) * t)))


def paste_rounded(img, tile, pos, size, radius=10):
    tile = tile.resize((size, size), Image.NEAREST)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).rounded_rectangle((0, 0, size - 1, size - 1), radius=radius, fill=255)
    img.paste(tile, pos, mask)


def star_parts(star: int):
    """[1234✫] をプレステージ色で塗り分けたパーツ列"""
    text = f"[{star}"
    if star >= 1000:
        colors = [RAINBOW[i % len(RAINBOW)] for i in range(len(text) + 2)]
    else:
        colors = [PRESTIGE[min(star // 100, 9)]] * (len(text) + 2)
    parts = [(ch, colors[i], False) for i, ch in enumerate(text)]
    parts.append(("✫", colors[len(text)], True))
    parts.append(("]", colors[len(text) + 1], False))
    return parts


def draw_parts(draw, x, y, parts, size, weight="Bold", anchor="ls"):
    """(text, color, is_symbol) を左から順に描画し、終端のxを返す"""
    for text, color, is_symbol in parts:
        f = symbol_font(int(size * 0.9)) if is_symbol else font(weight, size)
        draw.text((x, y), text, font=f, fill=color, anchor=anchor)
        x += draw.textlength(text, font=f)
    return x


def parts_width(draw, parts, size, weight="Bold"):
    return sum(draw.textlength(t, font=symbol_font(int(size * 0.9)) if s else font(weight, size)) for t, _, s in parts)


def player_parts(star, rank, name):
    parts = star_parts(star) + [(" ", WHITE, False)]
    if rank:
        parts.append((rank + " ", rank_color(rank), False))
    parts.append((name, rank_color(rank), False))
    return parts


def footer(img, w, h):
    stamp = datetime.datetime.now(JST).strftime("%Y/%m/%d %H:%M JST")
    ImageDraw.Draw(img).text((w - 40, h - 24), f"youmoubot  •  data: api.bordic.xyz  •  {stamp}", font=font("Medium", 17), fill=DIM, anchor="rm")


def to_png(img) -> io.BytesIO:
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="PNG")
    buf.seek(0)
    return buf


# --- /bwstats ---
def stat_tiles(s):
    ws = s["winstreak"]
    ws_tile = ("WINSTREAK", "?" if ws is None else fmt(ws), "hidden" if ws is None else "estimated", WHITE if ws is None else winstreak_color(ws))
    fkdr = ("FKDR", fmt(s["fkdr"]), f"{s['final_kills']:,} / {s['final_deaths']:,}", ratio_color(s["fkdr"]))
    wlr = ("WLR", fmt(s["wlr"]), f"{s['wins']:,} W / {s['losses']:,} L", ratio_color(s["wlr"]))
    if s["bblr"] is not None:
        return [
            fkdr, wlr,
            ("BBLR", fmt(s["bblr"]), f"{s['beds_broken']:,} / {s['beds_lost']:,}", ratio_color(s["bblr"])),
            ("KDR", fmt(s["kdr"]), f"{s['kills']:,} / {s['deaths']:,}", ratio_color(s["kdr"])),
            ws_tile,
            ("FINAL KILLS", fmt(s["final_kills"]), "all time", WHITE),
        ]
    return [
        fkdr, wlr, ws_tile,
        ("WINS", fmt(s["wins"]), "games won", WHITE),
        ("FINAL KILLS", fmt(s["final_kills"]), "all time", WHITE),
        ("FINAL DEATHS", fmt(s["final_deaths"]), "all time", WHITE),
    ]


def build_stats_card(name, rank, s, mode_label, skin=None) -> io.BytesIO:
    W, H = 1200, 675
    gx, gy, gw, gh, gap = 400, 214, 760, 400, 20
    tw, th = (gw - gap * 2) // 3, (gh - gap) // 2
    tiles = stat_tiles(s)
    boxes = [(40, 40, 360, 635)]
    for i in range(6):
        x, y = gx + (i % 3) * (tw + gap), gy + (i // 3) * (th + gap)
        boxes.append((x, y, x + tw, y + th))
    img = panels(load_background(W, H), boxes)

    if skin:
        scale = min(230 / skin.width, 470 / skin.height)
        sk = skin.resize((int(skin.width * scale), int(skin.height * scale)), Image.NEAREST)
        shadow = Image.new("RGBA", (W, H), (0, 0, 0, 0))
        ImageDraw.Draw(shadow).ellipse((110, 588, 290, 616), fill=(0, 0, 0, 150))
        img = Image.alpha_composite(img, shadow.filter(ImageFilter.GaussianBlur(6)))
        img.alpha_composite(sk, (200 - sk.width // 2, 600 - sk.height))
    d = ImageDraw.Draw(img)
    if not skin:
        d.text((200, 338), name[:1].upper(), font=font("Bold", 160), fill=MUTED, anchor="mm")

    d.text((gx, 84), f"BEDWARS  •  {mode_label.upper()}", font=font("SemiBold", 24), fill=MUTED, anchor="ls")
    parts = player_parts(s["star"], rank, name)
    size = 54
    while size > 28 and parts_width(d, parts, size) > gw:
        size -= 2
    draw_parts(d, gx, 152, parts, size)
    accent_line(img, gx, gx + gw, 178)

    for i, (label, value, sub, color) in enumerate(tiles):
        x, y = gx + (i % 3) * (tw + gap), gy + (i // 3) * (th + gap)
        d.text((x + 24, y + 40), label, font=font("SemiBold", 21), fill=MUTED, anchor="lm")
        d.text((x + 24, y + 104), value, font=fit_font(d, value, "Bold", 58, tw - 48), fill=color, anchor="lm")
        d.text((x + 24, y + 158), sub, font=fit_font(d, sub, "Medium", 19, tw - 48, 12), fill=DIM, anchor="lm")
    footer(img, W, H)
    return to_png(img)


# --- /compare ---
def build_compare_card(players, fields, mode_label, heads=(None, None)) -> io.BytesIO:
    """players: [(name, rank, stats), (name, rank, stats)], fields: [(label, key)]"""
    (n1, r1, s1), (n2, r2, s2) = players
    rows = [(label, s1[key], s2[key]) for label, key in fields if s1[key] is not None and s2[key] is not None]
    W, row_h, top = 1200, 62, 240
    H = top + len(rows) * (row_h + 10) + 50
    boxes = [(40, 40, 1160, 206)]
    for i in range(len(rows)):
        y = top + i * (row_h + 10)
        boxes.append((40, y, 1160, y + row_h, (255, 255, 255, 18 if i % 2 == 0 else 10)))
    img = panels(load_background(W, H), boxes, radius=18)
    d = ImageDraw.Draw(img)

    score = [0, 0]
    for _, a, b in rows:
        if a > b:
            score[0] += 1
        elif b > a:
            score[1] += 1

    for side, (name, rank, s), head in ((0, players[0], heads[0]), (1, players[1], heads[1])):
        hx = 66 if side == 0 else 1160 - 26 - 116
        if head:
            paste_rounded(img, head, (hx, 65), 116, radius=14)
        tx = hx + 116 + 24 if side == 0 else hx - 24
        anchor = "ls" if side == 0 else "rs"
        nf = fit_font(d, name, "Bold", 44, 300)
        d.text((tx, 116), name, font=nf, fill=rank_color(rank), anchor=anchor)
        sp = star_parts(s["star"])
        sx = tx if side == 0 else tx - parts_width(d, sp, 28, "SemiBold")
        draw_parts(d, sx, 164, sp, 28, "SemiBold")
    d.text((600, 112), "VS", font=font("Bold", 60), fill=ACCENT[1], anchor="mm")
    d.text((600, 166), f"{score[0]}  :  {score[1]}", font=font("SemiBold", 26), fill=WHITE, anchor="mm")
    d.text((600, 194), mode_label.upper(), font=font("Medium", 17), fill=MUTED, anchor="mm")

    for i, (label, a, b) in enumerate(rows):
        cy = top + i * (row_h + 10) + row_h // 2
        ca, cb = (WIN, LOSE) if a > b else (LOSE, WIN) if b > a else (TIE, TIE)
        d.text((80, cy), fmt(a), font=font("Bold", 30), fill=ca, anchor="lm")
        d.text((1120, cy), fmt(b), font=font("Bold", 30), fill=cb, anchor="rm")
        d.text((600, cy), label.upper(), font=font("SemiBold", 22), fill=MUTED, anchor="mm")
        total = (a or 0) + (b or 0)
        if total > 0:
            left = int(240 * a / total)
            d.rounded_rectangle((360, cy + 18, 840, cy + 22), radius=2, fill=(70, 72, 100))
            d.rounded_rectangle((600 - left, cy + 18, 600, cy + 22), radius=2, fill=ca)
            d.rounded_rectangle((600, cy + 18, 600 + (240 - left), cy + 22), radius=2, fill=cb)
    footer(img, W, H)
    return to_png(img)


# --- /leaderboard ---
def build_leaderboard_card(title, names, heads) -> io.BytesIO:
    n = len(names)
    cols = 1 if n <= 10 else 2
    per_col = -(-n // cols) if n else 1
    W, row_h, gap, top = 1200, 56, 10, 206
    H = top + per_col * (row_h + gap) + 50
    cw = (1120 - (cols - 1) * 20) // cols
    boxes = []
    for i in range(n):
        x = 40 + (i // per_col) * (cw + 20)
        y = top + (i % per_col) * (row_h + gap)
        fill = (*MEDALS[i], 34) if i < 3 else (255, 255, 255, 14)
        boxes.append((x, y, x + cw, y + row_h, fill))
    img = panels(load_background(W, H), boxes, radius=16)
    d = ImageDraw.Draw(img)

    d.text((44, 84), "HYPIXEL BEDWARS LEADERBOARD", font=font("SemiBold", 24), fill=MUTED, anchor="ls")
    d.text((44, 152), title, font=font("Bold", 54), fill=WHITE, anchor="ls")
    accent_line(img, 44, 1160, 176)

    for i, name in enumerate(names):
        x = 40 + (i // per_col) * (cw + 20)
        y = top + (i % per_col) * (row_h + gap)
        cy = y + row_h // 2
        d.text((x + 44, cy), f"#{i + 1}", font=font("Bold", 26), fill=MEDALS[i] if i < 3 else MUTED, anchor="mm")
        if heads[i]:
            paste_rounded(img, heads[i], (x + 88, y + 8), 40, radius=6)
        d.text((x + 146, cy), name, font=font("SemiBold", 27), fill=WHITE, anchor="lm")
    if not names:
        d.text((600, top + 30), "No data", font=font("SemiBold", 28), fill=MUTED, anchor="mm")
    footer(img, W, H)
    return to_png(img)


if __name__ == "__main__":
    make_background().convert("RGB").save(BG_PATH)
    print("saved", BG_PATH)
