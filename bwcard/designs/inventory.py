"""インベントリ風 — マイクラのアイテム説明文（ツールチップ）1枚で見せる、見やすさ優先のカード。

プレイヤーの頭・名前・レベルの下に、アイコン付きで比率を4つ大きく並べます。
すべて「GUIの1ドット」単位で描くので、拡大してもドットがそろいます。背景は透明です。
"""
from __future__ import annotations

from PIL import Image

from .. import prestige as P
from .. import skin as SK
from .. import text as T
from ..gfx import Card
from ..models import PlayerStats
from . import common as C
from .items import ICONS

GW, GH = 184, 132          # キャンバス（GUIドット）
GUI_PX = 4                 # 1 GUIドット = 論理4px（scale=2 なら 8px）

BG = (16, 2, 18)                            # 説明文の背景（濃い紫）
BORDER_TOP, BORDER_BOT = (37, 1, 92), (23, 1, 52)
SUB = (150, 150, 150)                       # 小さい数字の色


def prestige_name(stats: PlayerStats) -> tuple[str, str]:
    key = stats.level_cosmetics.get("scheme") or P.default_scheme(stats.level)
    name = "Stone" if key == "none" else key.replace("_", " ").title()
    parts = P._part_colors(P.SCHEMES[key], len(str(stats.level)) + 3)

    def lum(c):
        r, g, b = T.MC_COLORS[c]
        return 0.3 * r + 0.59 * g + 0.11 * b
    return name, next((c for c in parts[1:] + parts[:1] if lum(c) >= 120), "7")


class Gui:
    """GUIドット単位で描くための小道具。"""

    def __init__(self, img: Image.Image, g: int):
        self.im, self.g = img, g

    def rect(self, x, y, w, h, color):
        g = self.g
        self.im.paste(tuple(color[:3]) + (255,), (round(x * g), round(y * g), round((x + w) * g), round((y + h) * g)))

    def icon(self, name: str, x, y, size=16):
        px = round(size * self.g)
        self.im.alpha_composite(ICONS[name].resize((px, px), Image.NEAREST), (round(x * self.g), round(y * self.g)))

    def text(self, s, x, y, unit=1.0, default="f", anchor="l") -> float:
        u = max(1, round(unit * self.g))
        return T.draw(self.im, x * self.g, y * self.g, s, u, default, anchor, True) / self.g

    def width(self, s, unit=1.0) -> float:
        return T.measure(s, max(1, round(unit * self.g))) / self.g


def tooltip_box(gui: Gui, x, y, w, h):
    """マイクラの説明文と同じ形（角が1ドット欠けた濃い紫の箱と、紫のグラデーション枠）。"""
    r = gui.rect
    r(x + 1, y, w - 2, h, BG)
    r(x, y + 1, w, h - 2, BG)
    r(x + 1, y + 1, w - 2, 1, BORDER_TOP)
    r(x + 1, y + h - 2, w - 2, 1, BORDER_BOT)
    for i in range(h - 4):
        t = i / max(1, h - 5)
        col = tuple(round(BORDER_TOP[k] + (BORDER_BOT[k] - BORDER_TOP[k]) * t) for k in range(3))
        r(x + 1, y + 2 + i, 1, 1, col)
        r(x + w - 2, y + 2 + i, 1, 1, col)


def render(stats: PlayerStats, mode: str = "overall", *, skin: Image.Image | None = None, slim: bool = False,
           scale: float = 2, footer: str = "", **_) -> Card:
    g = max(1, round(GUI_PX * scale))
    gh = GH + (10 if footer else 0)          # bot名は箱の下に小さく
    card = Card(GW * g / scale, gh * g / scale, scale, (0, 0, 0, 0))
    card.transparent = True
    gui = Gui(card.im, g)
    m = stats.mode(mode)

    tooltip_box(gui, 2, 2, GW - 4, GH - 4)
    L, R = 10, GW - 10                       # 内側の左右

    # ---- 頭・名前・モード・レベル ----
    head = SK.render(skin if skin is not None else SK.silhouette_skin(), 26 * g, parts="head",
                     yaw=-40, pitch=26, supersample=2)
    card.im.alpha_composite(head, (round((L + 13) * g - head.width / 2), 8 * g))
    tx = L + 32
    name = stats.display_name
    gui.text(name, tx, 9, 1 if gui.width(name) <= R - tx else 0.75)
    gui.text(f"§7Bed Wars {C.mode_title(mode)}", tx, 20)
    pname, pcol = prestige_name(stats)
    gui.text(f"{stats.level_formatted} §{pcol}{pname}", tx, 31)
    gui.rect(L, 43, R - L, 1, BORDER_TOP)

    # ---- 比率4つ（アイコン・名前・大きな数字・内訳） ----
    cells = [("diamond_sword", "FKDR", m.fkdr, m.final_kills, m.final_deaths),
             ("trophy", "WLR", m.wlr, m.wins, m.losses),
             ("bed", "BBLR", m.bblr, m.beds_broken, m.beds_lost),
             ("iron_sword", "KDR", m.kdr, m.kills, m.deaths)]
    col_w = (R - L) / 2
    for i, (icon, label, v, a, b) in enumerate(cells):
        x = L + (i % 2) * col_w
        y = 49 + (i // 2) * 34
        gui.icon(icon, x, y + 4)
        vx = x + 20
        gui.text(f"§7{label}", vx, y)
        val = C.r_short(v)
        big = 1.75 if gui.width(val, 1.75) <= col_w - 22 else 1.25
        gui.text(f"§{C.ratio_color(v)}{val}", vx, y + 9 + (1.75 - big) * 3.5, big)
        sub = f"{C.n(a)}/{C.n(b)}"
        if gui.width(sub, 0.75) > col_w - 22:
            sub = f"{C.short(a)}/{C.short(b)}"
        gui.text(sub, vx, y + 24, 0.75, default=SUB)

    # ---- 下：連勝と試合数 ----
    gui.rect(L, 117, R - L, 1, BORDER_TOP)
    ws = "§c?" if m.winstreak is None else f"§a{C.n(m.winstreak)}"
    gui.icon("flame", L, 120, 9)
    gui.text(f"§7Winstreak {ws}", L + 12, 121, 0.75)
    gui.icon("clock", L + col_w, 120, 9)
    gui.text(f"§7Games §f{C.n(m.games)}", L + col_w + 12, 121, 0.75)
    if footer:
        T.draw(card.im, (GW - 3) * g, (GH + 1) * g, footer, max(1, round(0.75 * g)), (140, 140, 140), "r", False)
    return card
