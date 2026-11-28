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


class SectionSelect(discord.ui.Select):
    def __init__(self, user_id: int, choices: dict, placeholder: str, row: int):
        on = settings.show(user_id)
        self.choices = choices
        super().__init__(placeholder=placeholder, row=row, min_values=0, max_values=len(choices), options=[
            discord.SelectOption(label=label, value=key, default=key in on) for key, label in choices.items()])

    async def callback(self, interaction: discord.Interaction):
        others = settings.show(interaction.user.id) - set(self.choices)
        settings.set_show(interaction.user.id, others | set(self.values))
        await self.view.saved(interaction)


class ResetButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="全部表示に戻す", style=discord.ButtonStyle.secondary, row=2)

    async def callback(self, interaction: discord.Interaction):
        settings.reset(interaction.user.id)
        await self.view.saved(interaction)


class SettingsView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=300)
        self.user_id = user_id
        self._rebuild()

    def _rebuild(self):
        self.clear_items()
        self.add_item(SectionSelect(self.user_id, STAT_SECTIONS, "統計のマス（Wins・FKDRなど12個）", 0))
        self.add_item(SectionSelect(self.user_id, PART_CHOICES, "スキン・連勝・一番下の情報", 1))
        self.add_item(ResetButton())

    def summary(self) -> str:
        on = settings.show(self.user_id)
        off = [USER_SECTIONS[k].split("（")[0] for k in USER_SECTIONS if k not in on]
        text = f"表示中：{len(on)} / {len(USER_SECTIONS)} 項目"
        return text + (f"\n非表示：{'、'.join(off)}" if off else "")

    async def saved(self, interaction: discord.Interaction):
        self._rebuild()
        await interaction.response.edit_message(content=f"✅ 保存しました。\n{self.summary()}", view=self)


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
