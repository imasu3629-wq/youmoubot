"""スタンダード — 参考画像2枚のいいとこ取り。表示する項目を選べる版。

    上段   : 名前・ギルドタグ・レベルバー（常に表示）
    中段   : [スキン] [12マスの統計（勝敗／ファイナル／キル／ベッドの4行）]
    下段   : [モード別の試合数] [モード別の連勝] [資源]
    最下段 : 初ログイン日など

オフにした項目は「詰める」ルールで並べ直します。
・同じ段の残りが横に広がって空きを埋める（統計のマスも、下の段の部品も同じ）
  広くなった一覧は2列になり、段の高さも低くなる
・段の中身が全部オフなら、その段ごと消えて画像の高さが縮む
・スキンだけ残って統計が全部オフのときは、スキンが下の段の左端に移る
"""
from __future__ import annotations

from PIL import Image, ImageFilter

from .. import prestige as P
from ..gfx import Card, photo_background
from ..models import MODE_SHORT, MODES, PlayerStats
from . import common as C
from .wa_bg import background as wa_background

# 統計の12マス。段ごとに（キー＝ModeStats の名前, カードに出す名前, 色）
GRID = [
    [("wins", "Wins", "a"), ("losses", "Losses", "c"), ("wlr", "WLR", "6")],
    [("final_kills", "Final Kills", "a"), ("final_deaths", "Final Deaths", "c"), ("fkdr", "FKDR", "6")],
    [("kills", "Kills", "a"), ("deaths", "Deaths", "c"), ("kdr", "KDR", "6")],
    [("beds_broken", "Beds Broken", "a"), ("beds_lost", "Beds Lost", "c"), ("bblr", "BBLR", "6")],
]

# 設定メニューに出す項目（キー → 表示名）。ここに無いもの（名前とレベルバー）は常に表示。
STAT_SECTIONS = {
    "wins": "Wins（勝ち）", "losses": "Losses（負け）", "wlr": "WLR（勝ち÷負け）",
    "final_kills": "Final Kills（ファイナルキル）", "final_deaths": "Final Deaths（ファイナルデス）",
    "fkdr": "FKDR（ファイナルキル÷デス）",
    "kills": "Kills（キル）", "deaths": "Deaths（デス）", "kdr": "KDR（キル÷デス）",
    "beds_broken": "Beds Broken（壊したベッド）", "beds_lost": "Beds Lost（壊されたベッド）",
    "bblr": "BBLR（壊した÷壊された）",
}
PART_SECTIONS = {
    "skin": "スキン",
    "games": "モード別の試合数グラフ",
    "winstreak": "モード別の連勝",
    "resources": "資源（トークン・鉄・金・ダイヤなど）",
    "info": "一番下の情報（初ログイン日など）",
}
SECTIONS = {**STAT_SECTIONS, **PART_SECTIONS}

W = 900
M, GAP = 12, 10                # 外側の余白、部品どうしの間
HEADER_H, ROW2_H, INFO_H = 84, 180, 28
CELL_H, CELL_GAP = 73.5, 8
SKIN_W, SKIN_MIN_H = 222, 230
BOTTOM_WEIGHT = {"games": 300, "winstreak": 260, "resources": 296}   # 下段の幅の比率
PANEL = (8, 8, 18, 150)
EDGE = (255, 255, 255, 28)
FROST = 6    # パネルの下だけ背景を強くぼかす（すりガラス）。模様が文字のじゃまにならず、すき間にはくっきり見える
LABEL = {"a": (140, 255, 150), "c": (255, 140, 140), "6": (255, 205, 110)}


def panel(c: Card, x, y, w, h):
    frost = getattr(c, "frost", None)
    if frost is not None:
        c.paste_rrect(frost, x, y, w, h, 10)
    c.rrect(x, y, w, h, 10, fill=PANEL, outline=EDGE, width=1)


# ---------------------------------------------------------------- 部品
def draw_header(c: Card, stats: PlayerStats, y: float):
    panel(c, M, y, W - 2 * M, HEADER_H)
    cos = stats.level_cosmetics
    cur, req = stats.level_progress
    exp = f"§7EXP §b{C.n(cur)}§7/§a{C.n(req)}"
    exp_w = c.mc_width(exp, 2)
    name = stats.display_name + (f" {stats.guild_tag_formatted}" if stats.guild_tag else "")
    u = c.mc_fit(name, W - 2 * M - 40 - exp_w - 24, (3, 2.5, 2))
    c.mc(name, 32, y + 16 + (3 - u) * 3.5, u)
    c.mc(exp, W - 32, y + 19, 2, anchor="r")
    left, right = P.format_level(stats.level, **cos), P.format_level(stats.level + 1, **cos)
    lw, rw = c.mc_width(left, 2.5), c.mc_width(right, 2.5)
    c.mc(left, 32, y + 51, 2.5)
    c.mc(right, W - 32, y + 51, 2.5, anchor="r")
    bx0, bx1 = 32 + lw + 14, W - 32 - rw - 14
    C.pixel_bar(c, bx0, y + 52, bx1 - bx0, 15, cur / req, segments=30)


def draw_skin(c: Card, skin, slim, mode, x, y, w, h):
    panel(c, x, y, w, h)
    cx = x + w / 2
    c.mc("§cBed§fWars §fStats", cx, y + 14, 2, anchor="m")
    body_h = h - 76
    body = C.body_image(skin, c.p(body_h), slim=slim)
    C.floor_shadow(c, cx, y + 38 + body_h - 4, min(104, body_h * 0.42))
    c.paste_px(body, round(cx * c.s - body.width / 2), c.p(y + 38))
    c.mc(f"§7({C.mode_title(mode)})", cx, y + h - 26, 2, anchor="m")


def draw_grid(c: Card, m, rows, x, y, w, h):
    """rows は GRID から表示するマスだけを残したもの。段ごとに、残ったマスで横幅を分け合う。"""
    ch = (h - (len(rows) - 1) * CELL_GAP) / len(rows)
    for r, row in enumerate(rows):
        cw = (w - (len(row) - 1) * CELL_GAP) / len(row)
        for col, (key, label, color) in enumerate(row):
            value = getattr(m, key)
            cx, cy = x + col * (cw + CELL_GAP), y + r * (ch + CELL_GAP)
            panel(c, cx, cy, cw, ch)
            top = cy + (ch - 62) / 2          # セルが高くなっても中身は真ん中に
            c.mc(label, cx + cw / 2, top + 8, 2, anchor="m", default=LABEL[color])
            text = (C.r2(value) if value < 1000 else C.r_short(value)) if isinstance(value, float) else C.n(value)
            u = c.mc_fit(text, cw - 24, (4, 3.5, 3))
            c.mc(f"§{color}{text}", cx + cw / 2, top + 29 + (4 - u) * 3.5, u, anchor="m")


def draw_games(c: Card, stats: PlayerStats, mode, x, y, w, h):
    panel(c, x, y, w, h)
    games = {k: stats.mode(k).games for k in MODE_SHORT}
    total = sum(games.values()) or 1
    c.mc(f"§7Games: §f{C.n(stats.mode(mode).games)}", x + w / 2, y + 12, 2, anchor="m")
    peak = max(games.values()) or 1
    area = w - 52
    bw = min(60, area / 5 * 0.62)
    step = (area - bw) / 4
    base, maxh = y + h - 38, h - 88
    for i, (k, g) in enumerate(games.items()):
        bx = x + 26 + i * step
        bh = max(3, maxh * g / peak)
        cur = k == mode
        c.rrect(bx, base - bh, bw, bh, 3, fill=(85, 255, 255, 230) if cur else (255, 255, 255, 46))
        pct = f"{round(g * 100 / total)}%"
        if bh > 24:
            c.mc(f"§{'0' if cur else 'a'}{pct}", bx + bw / 2, base - bh + 7, 1.5, anchor="m", shadow=not cur)
        else:
            c.mc(f"§a{pct}", bx + bw / 2, base - bh - 15, 1.5, anchor="m")
        c.mc(f"§{'f' if cur else 'a'}{MODE_SHORT[k]}", bx + bw / 2, base + 10, 2, anchor="m")


def _list(c: Card, entries, x, y, w, h, title=None):
    """（ラベル, 値の候補…）の一覧。値は収まる最初の候補を使う。幅が広いときは2列にする。"""
    ty = y + 12
    if title:
        c.mc(title, x + w / 2, ty, 2, anchor="m")
        ty += 26
    cols = 2 if w >= 520 else 1
    per = -(-len(entries) // cols)
    col_w = (w - 32 - (cols - 1) * 36) / cols
    line = min(22.5, (y + h - 12 - ty) / per)
    for i, (label, *values) in enumerate(entries):
        cx = x + 16 + (i // per) * (col_w + 36)
        ly = ty + (i % per) * line
        room = col_w - c.mc_width(label, 2) - 10
        fit = next(((u, v) for v in values for u in (2, 1.5) if c.mc_width(v, u) <= room), None)
        if fit is None:  # どの候補も入らないほど狭いときは、ラベルの方を削る
            fit = (1.5, values[-1])
            label = c.mc_trunc(label, col_w - c.mc_width(values[-1], 1.5) - 10, 2)
        u, val = fit
        c.mc(label, cx, ly, 2)
        c.mc(val, cx + col_w, ly + (2 - u) * 3.5, u, anchor="r")


def draw_winstreak(c: Card, stats: PlayerStats, mode, x, y, w, h):
    panel(c, x, y, w, h)
    entries = []
    for k in ["overall", *MODE_SHORT]:
        ws = stats.mode(k).winstreak
        val = "§c?" if ws is None else f"§a{C.n(ws)}"
        entries.append((f"{'§f» ' if k == mode else '§8  '}§{'f' if k == mode else '7'}{MODES[k][1]}", val))
    _list(c, entries, x, y, w, h, "§7Winstreak")


def draw_resources(c: Card, stats: PlayerStats, m, x, y, w, h):
    panel(c, x, y, w, h)

    def tickets(fmt):
        wallet = f"§7/§b{fmt(stats.slumber_wallet)}" if stats.slumber_wallet else ""
        return f"§b{fmt(stats.slumber_tickets)}{wallet}"

    entries = [
        ("§2• §7Tokens", f"§2{C.compact(stats.tokens)}", f"§2{C.short(stats.tokens)}"),
        ("§7• §7Iron", f"§f{C.compact(m.iron)}", f"§f{C.short(m.iron)}"),
        ("§6• §7Gold", f"§6{C.compact(m.gold)}", f"§6{C.short(m.gold)}"),
        ("§b• §7Diamonds", f"§b{C.compact(m.diamond)}", f"§b{C.short(m.diamond)}"),
        ("§a• §7Emeralds", f"§a{C.compact(m.emerald)}", f"§a{C.short(m.emerald)}"),
        ("§b• §7Tickets", tickets(C.n), tickets(C.short), f"§b{C.short(stats.slumber_tickets)}"),  # 狭いと所持数だけ
        ("§d• §7Lifetime", f"§d{C.compact(stats.slumber_total)}", f"§d{C.short(stats.slumber_total)}"),
    ]
    _list(c, entries, x, y + 2, w, h - 2)


def draw_info(c: Card, stats: PlayerStats, footer, y):
    panel(c, M, y, W - 2 * M, INFO_H)
    info = f"§7First Login: §f{C.date(stats.first_login)}   §7Network Level: §e{stats.network_level:,.1f}"
    if stats.guild_name:
        info += f"   §7Guild: §{stats.guild_tag_color}{stats.guild_name}"
    if c.mc_width(info, 1.5) > W - 2 * M - 200:
        info = info.split("   §7Guild")[0]
    c.mc(info, 28, y + 8, 1.5)
    status = {True: "§a● Online", False: "§7● Offline"}.get(stats.online, "")
    tail = "   ".join(t for t in (status, f"§8{footer}" if footer else "") if t)
    if tail:
        c.mc(tail, W - 28, y + 8, 1.5, anchor="r")


def pref_height(key: str, w: float) -> float:
    """下段の部品が、その幅で必要とする高さ（一覧が2列になると低くて済む）。"""
    if key == "games":
        return ROW2_H
    rows = {"winstreak": 6, "resources": 7}[key]
    title = 26 if key == "winstreak" else 0
    if w >= 520:
        rows = -(-rows // 2)
    return max(96, min(ROW2_H, 24 + title + rows * 22.5))


# ---------------------------------------------------------------- 全体
def render(stats: PlayerStats, mode: str = "overall", *, skin: Image.Image | None = None, slim: bool = False,
           background: Image.Image | str | None = None, scale: float = 2, footer: str = "", show=None,
           **_) -> Card:
    show = set(SECTIONS) if show is None else set(show)
    m = stats.mode(mode)
    rows = [r for r in ([cell for cell in row if cell[0] in show] for row in GRID) if r]
    skin_on = "skin" in show
    bottom = [k for k in ("games", "winstreak", "resources") if k in show]
    # 統計が全部オフのときは、スキンを下段の左端に移す（横長の空いたパネルにしない）
    skin_in_bottom = skin_on and not rows and bool(bottom)
    if skin_in_bottom:
        skin_on = False
    avail = W - 2 * M - (len(bottom) - 1) * GAP - ((SKIN_W + GAP) if skin_in_bottom else 0)
    total_w = sum(BOTTOM_WEIGHT[k] for k in bottom) or 1
    widths = {k: avail * BOTTOM_WEIGHT[k] / total_w for k in bottom}
    row2_h = max((pref_height(k, widths[k]) for k in bottom), default=0)
    if skin_in_bottom:
        row2_h = max(row2_h, ROW2_H)

    # ---- 高さを先に決める（空の段は消す） ----
    main_h = 0.0
    if rows or skin_on:
        n = len(rows)
        grid_h = n * CELL_H + (n - 1) * CELL_GAP if n else 0
        main_h = max(grid_h, SKIN_MIN_H if skin_on else 0)
    H = M + HEADER_H + (GAP + main_h if main_h else 0) + (GAP + row2_h if bottom else 0) + \
        (GAP + INFO_H if "info" in show else 0) + M
    H = round(H)

    c = Card(W, H, scale)
    if isinstance(background, Image.Image):   # スクショなどの写真
        bg = photo_background(background, c.im.width, c.im.height)
    else:                                     # 名前（"yozakura" など）か None＝標準の和風背景
        bg = wa_background(background, c.im.width, c.im.height)
    c.im.alpha_composite(bg)
    c.frost = bg.filter(ImageFilter.GaussianBlur(FROST * scale))

    y = M
    draw_header(c, stats, y)
    y += HEADER_H + GAP

    # ---- 中段：スキン＋統計（スキンが無ければ統計が全幅に。統計も下段も無いときだけスキンが全幅に） ----
    if main_h:
        if skin_on:
            sw = SKIN_W if rows else W - 2 * M
            draw_skin(c, skin, slim, mode, M, y, sw, main_h)
        if rows:
            gx = M + SKIN_W + GAP if skin_on else M
            draw_grid(c, m, rows, gx, y, W - M - gx, main_h)
        y += main_h + GAP

    # ---- 下段：残った部品で横幅を分け合う ----
    if bottom:
        x = M
        if skin_in_bottom:
            draw_skin(c, skin, slim, mode, x, y, SKIN_W, row2_h)
            x += SKIN_W + GAP
        for k in bottom:
            w = widths[k]
            if k == "games":
                draw_games(c, stats, mode, x, y, w, row2_h)
            elif k == "winstreak":
                draw_winstreak(c, stats, mode, x, y, w, row2_h)
            else:
                draw_resources(c, stats, m, x, y, w, row2_h)
            x += w + GAP
        y += row2_h + GAP

    if "info" in show:
        draw_info(c, stats, footer, y)
    return c
