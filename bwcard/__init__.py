"""Hypixel ベッドウォーズの統計カード（画像）を作るパッケージ。

    from bwcard import render_card
    buf = render_card(stats, design="standard", mode="overall", skin=skin_img)
    await interaction.followup.send(file=discord.File(buf, f"bedwars.{buf.ext}"))
"""
from __future__ import annotations

from PIL import Image

from .models import MODES, ModeStats, PlayerStats, parse_player
from .skin import load_skin
from .designs.standard import PART_SECTIONS, SECTIONS, STAT_SECTIONS
from .designs.wa_bg import BACKGROUNDS, WA_BACKGROUNDS

__all__ = ["render_card", "parse_player", "load_skin", "PlayerStats", "ModeStats", "MODES", "DESIGNS",
           "SECTIONS", "STAT_SECTIONS", "PART_SECTIONS", "BACKGROUNDS", "WA_BACKGROUNDS"]

DESIGNS = {
    "standard": "スタンダード（参考画像2枚のいいとこ取り）",
    "inventory": "インベントリ風（アイテムの説明文）",
    "washi": "和風（和紙と朱印）",
}
# 標準の画像形式（ドット絵は背景透明のPNG、和紙の質感はJPEGの方がずっと軽い）
DEFAULT_FORMAT = {"standard": "PNG", "inventory": "PNG", "washi": "JPEG"}


def render_card(stats: PlayerStats, design: str = "standard", mode: str = "overall", *,
                skin: Image.Image | None = None, slim: bool = False,
                background: Image.Image | str | None = None, scale: float = 2,
                footer: str = "", fmt: str = "auto", show=None):
    """統計カードを描いて画像（BytesIO の仲間）を返す。拡張子は戻り値の .ext に入っています。

    stats      … parse_player() で作った PlayerStats
    design     … 'standard'（いいとこ取り）/ 'inventory'（マイクラの説明文風）/ 'washi'（和風）
    mode       … 'overall' / 'solo' / 'doubles' / 'threes' / 'fours' / '4v4'
    skin       … load_skin() で読んだスキン画像（None なら無地のシルエット）
    slim       … スキンが細い腕（Alex型）なら True
    background … standard の背景。写真（PIL画像）か、内蔵の背景の名前（BACKGROUNDS を参照）。None なら 'seigaiha'
    scale      … 2 が標準（Discordで拡大してもくっきり）。軽くしたいなら 1
    footer     … 小さく入れる文字（bot名など）
    fmt        … 'auto'（おすすめ）/ 'PNG' / 'JPEG' / 'WEBP'
    show       … standard で表示する項目のキーの集まり（None なら全部）。キーは SECTIONS を参照

        buf = render_card(stats, "standard", skin=skin)
        await interaction.followup.send(file=discord.File(buf, f"bedwars.{buf.ext}"))
    """
    from .designs import inventory, standard, washi

    mod = {"standard": standard, "inventory": inventory, "washi": washi}.get(design)
    if mod is None:
        raise ValueError(f"unknown design: {design}")
    if mode not in MODES:
        raise ValueError(f"unknown mode: {mode}")
    if show is not None:
        show = set(show)
        unknown = show - set(SECTIONS)
        if unknown:
            raise ValueError(f"unknown section: {', '.join(sorted(unknown))}（使えるのは {', '.join(SECTIONS)}）")
    card = mod.render(stats, mode, skin=skin, slim=slim, background=background, scale=scale, footer=footer,
                      show=show)
    if fmt == "auto":  # 写真の背景を使うときは JPEG の方がずっと軽い
        fmt = "JPEG" if (design == "standard" and isinstance(background, Image.Image)) else DEFAULT_FORMAT[design]
    return card.to_file(fmt)
