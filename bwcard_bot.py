"""bwcard の統計カードを Aurora のデータで出すための部品（/bw と /config 用）。"""
from __future__ import annotations
import asyncio
import base64
import io
import json
import os
import random
import time
from pathlib import Path

import discord
import requests
from PIL import Image

from bwcard import MODES, PART_SECTIONS, SECTIONS, STAT_SECTIONS, WA_BACKGROUNDS, load_skin, parse_player
from bwcard import prestige as P
from bwcard import render_card
from bwcard.designs.standard import GRID

FOOTER = os.environ.get("CARD_FOOTER", "youmoubot")
BACKGROUND_DIR = Path(os.environ.get("BACKGROUND_DIR", "backgrounds"))
CARD_BACKGROUND = os.environ.get("CARD_BACKGROUND", "random")
SETTINGS_FILE = Path(os.environ.get("SETTINGS_FILE", "settings.json"))
SESSION_URL = "https://sessionserver.mojang.com/session/minecraft/profile/{uuid}"
SNAPSHOT_URL = "https://api.bordic.xyz/v3/player/snapshot"
AURORA_KEY = os.environ.get("AURORA_API_KEY", "")

# Aurora のキャッシュに無いデータの部品は出さない（キーで snapshot が取れたときは全部出す）
AURORA_MISSING = {"games", "resources"}
OVERALL_ONLY = {"kills", "deaths", "kdr", "beds_broken", "beds_lost", "bblr"}
PART_CHOICES = PART_SECTIONS if AURORA_KEY else {k: v for k, v in PART_SECTIONS.items() if k not in AURORA_MISSING}
USER_SECTIONS = {**STAT_SECTIONS, **PART_CHOICES}


def level_to_exp(level: int) -> int:
    prestige, rest = divmod(int(level or 0), 100)
    return prestige * 487_000 + sum(P.exp_required(i) for i in range(rest))


def fetch_skin(uuid: str):
    try:
        data = requests.get(SESSION_URL.format(uuid=uuid.replace("-", "")), timeout=8).json()
        prop = next((p for p in data.get("properties", []) if p.get("name") == "textures"), None)
        tex = json.loads(base64.b64decode(prop["value"]))["textures"].get("SKIN") if prop else None
        if not tex:
            return None, False
        res = requests.get(tex["url"].replace("http://", "https://"), timeout=8)
        slim = (tex.get("metadata") or {}).get("model") == "slim"
        return load_skin(Image.open(io.BytesIO(res.content))), slim
    except (requests.RequestException, KeyError, ValueError, OSError):
        return None, False


def fetch_snapshot(uuid: str) -> dict | None:
    """Aurora の snapshot（Hypixel 形式の Bedwars 全項目）。キーが無いか取れないときは None。"""
    if not AURORA_KEY:
        return None
    try:
        res = requests.get(SNAPSHOT_URL, params={"uuid": uuid.replace("-", ""), "t": int(time.time() * 1000),
                                                 "key": AURORA_KEY}, timeout=10)
        data = res.json() if res.status_code == 200 else {}
    except (requests.RequestException, ValueError):
        return None
    return data.get("snapshot") or None


def load_card_data(player: dict, uuid: str):
    """Aurora の player から (PlayerStats, スキン, 細腕か, 全項目あるか) を作る。"""
    player = dict(player)
    stats = dict(player.get("stats") or {})
    bw = dict(stats.get("Bedwars") or {})
    snapshot = fetch_snapshot(uuid)
    if snapshot:
        played = lambda d: (d.get("wins_bedwars") or 0) + (d.get("losses_bedwars") or 0)  # noqa: E731
        newer = played(snapshot) >= played(bw)  # 試合数は減らないので、多い方が新しい
        bw.update({k: v for k, v in snapshot.items() if v is not None and (newer or k not in bw)})
    if not bw.get("Experience"):
        bw["Experience"] = level_to_exp((player.get("achievements") or {}).get("bedwars_level") or 0)
    stats["Bedwars"] = bw
    player["stats"] = stats
    player.setdefault("uuid", uuid)
    skin, slim = fetch_skin(uuid)
    return parse_player(player), skin, slim, bool(snapshot and "games_played_bedwars" in snapshot)


class Settings:
    """{ユーザーID: [表示する項目のキー]} を JSON ファイルに保存する。未設定の人は全部表示。"""

    def __init__(self, path: Path):
        self.path = path
        try:
            self.data = json.loads(path.read_text(encoding="utf-8"))
        except (FileNotFoundError, ValueError):
            self.data = {}

    def show(self, user_id: int) -> set:
        saved = self.data.get(str(user_id))
        return set(USER_SECTIONS) if saved is None else {k for k in saved if k in USER_SECTIONS}

    def set_show(self, user_id: int, keys) -> None:
        self.data[str(user_id)] = [k for k in USER_SECTIONS if k in set(keys)]
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(self.path)

    def reset(self, user_id: int) -> None:
        self.set_show(user_id, USER_SECTIONS)


settings = Settings(SETTINGS_FILE)


def pick_background():
    files = [] if not BACKGROUND_DIR.is_dir() else [p for p in BACKGROUND_DIR.glob("*") if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")]
    if files:
        return random.choice(files)
    return random.choice(WA_BACKGROUNDS) if CARD_BACKGROUND == "random" else CARD_BACKGROUND


async def make_file(data, mode: str, show, background) -> discord.File:
    stats, skin, slim, full = data
    if not full:
        show = set(show) - AURORA_MISSING - (OVERALL_ONLY if mode != "overall" else set())
    bg = Image.open(background) if isinstance(background, Path) else background
    buf = await asyncio.to_thread(render_card, stats, "standard", mode, skin=skin, slim=slim,
                                  background=bg, footer=FOOTER, show=show)
    m = stats.modes[mode]
    alt = (f"{stats.username} Bedwars {MODES[mode][1]}: Star {stats.level}, FKDR {m.fkdr:.2f}, "
           f"WLR {m.wlr:.2f}, Final Kills {m.final_kills}, Wins {m.wins}")
    return discord.File(buf, filename=f"bedwars_{stats.username}_{mode}.{buf.ext}", description=alt)


def _split_label(label: str) -> tuple[str, str | None]:
    """'Wins（勝ち）' → ('Wins', '勝ち')"""
    name, _, rest = label.partition("（")
    return name, (rest.rstrip("）") or None)


def _mark(on: bool) -> str:
    return "✅" if on else "▫️"


class SectionSelect(discord.ui.Select):
    def __init__(self, user_id: int, choices: dict, title: str, row: int):
        on = settings.show(user_id)
        self.choices = choices
        count = sum(k in on for k in choices)
        options = []
        for key, label in choices.items():
            name, desc = _split_label(label)
            options.append(discord.SelectOption(label=name, value=key, description=desc, default=key in on))
        super().__init__(placeholder=f"{title}（{count}/{len(choices)} 表示中）", row=row,
                         min_values=0, max_values=len(choices), options=options)

    async def callback(self, interaction: discord.Interaction):
        others = settings.show(interaction.user.id) - set(self.choices)
        settings.set_show(interaction.user.id, others | set(self.values))
        await self.view.saved(interaction, "選んだ項目を保存しました")


class BulkButton(discord.ui.Button):
    def __init__(self, label: str, style: discord.ButtonStyle, keys, show: bool, done: str):
        super().__init__(label=label, style=style, row=2)
        self.keys, self.show, self.done = set(keys), show, done

    async def callback(self, interaction: discord.Interaction):
        on = settings.show(interaction.user.id)
        settings.set_show(interaction.user.id, (on | self.keys) if self.show else (on - self.keys))
        await self.view.saved(interaction, self.done)


class SettingsView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=300)
        self.user_id = user_id
        self._rebuild()

    def _rebuild(self):
        self.clear_items()
        self.add_item(SectionSelect(self.user_id, STAT_SECTIONS, "📊 統計のマス", 0))
        self.add_item(SectionSelect(self.user_id, PART_CHOICES, "🧩 そのほかの部品", 1))
        self.add_item(BulkButton("すべて表示", discord.ButtonStyle.success, USER_SECTIONS, True,
                                 "すべて表示に戻しました"))
        self.add_item(BulkButton("統計をすべて隠す", discord.ButtonStyle.danger, STAT_SECTIONS, False,
                                 "統計のマスをすべて隠しました"))

    def embed(self, notice: str | None = None) -> discord.Embed:
        on = settings.show(self.user_id)
        total, count = len(USER_SECTIONS), len(on & set(USER_SECTIONS))
        embed = discord.Embed(
            title="⚙️ /bw カードの表示設定",
            description=(f"✅ **{notice}**\n\n" if notice else "")
            + "下のメニューで表示する項目を選ぶと、すぐに保存されます。\n名前とレベルバーはいつも表示されます。",
            color=0x2ECC71 if count == total else (0xE67E22 if count else 0x95A5A6),
        )
        grid = []
        for row in GRID:
            cells = [f"{_mark(k in on)} {_split_label(STAT_SECTIONS[k])[0]}" for k, _, _ in row if k in STAT_SECTIONS]
            grid.append("　".join(cells))
        stat_count = sum(k in on for k in STAT_SECTIONS)
        embed.add_field(name=f"📊 統計のマス　{stat_count}/{len(STAT_SECTIONS)}", value="\n".join(grid), inline=False)
        parts = [f"{_mark(k in on)} {PART_CHOICES[k]}" for k in PART_CHOICES]
        part_count = sum(k in on for k in PART_CHOICES)
        embed.add_field(name=f"🧩 そのほかの部品　{part_count}/{len(PART_CHOICES)}", value="\n".join(parts), inline=False)
        if not AURORA_KEY:
            embed.add_field(name="ℹ️ メモ", value="Kills・Deaths・KDR・ベッドの項目は、総合モードのときだけ表示されます。",
                            inline=False)
        embed.set_footer(text=f"表示中 {count} / {total} 項目 ・ 設定はあなたのカードだけに反映されます")
        return embed

    async def saved(self, interaction: discord.Interaction, notice: str):
        self._rebuild()
        await interaction.response.edit_message(content=None, embed=self.embed(notice), view=self)


class ModeSelect(discord.ui.Select):
    def __init__(self, view):
        super().__init__(placeholder="モードを切り替え", row=0, options=[
            discord.SelectOption(label=label, value=key, default=key == view.mode)
            for key, (_, label) in MODES.items()])

    async def callback(self, interaction: discord.Interaction):
        await self.view.update(interaction, mode=self.values[0])


class CardView(discord.ui.View):
    def __init__(self, owner_id: int, data, mode: str):
        super().__init__(timeout=300)
        self.owner_id, self.data, self.mode = owner_id, data, mode
        self.background = pick_background()
        self.message = None
        self._lock = asyncio.Lock()
        self._rebuild()

    def _rebuild(self):
        self.clear_items()
        self.add_item(ModeSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.owner_id:
            await interaction.response.send_message(
                "切り替えはコマンドを使った人だけができます。自分のカードに出す項目は /config で選べます。",
                ephemeral=True)
            return False
        return True

    async def render(self) -> discord.File:
        return await make_file(self.data, self.mode, settings.show(self.owner_id), self.background)

    async def update(self, interaction: discord.Interaction, *, mode: str):
        self.mode = mode
        self._rebuild()
        await interaction.response.defer()
        async with self._lock:
            await interaction.edit_original_response(attachments=[await self.render()], view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        if self.message:
            try:
                await self.message.edit(view=self)
            except discord.HTTPException:
                pass
