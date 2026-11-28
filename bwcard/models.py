"""カードに渡すデータの形と、Hypixel API のレスポンスからの変換。"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timezone

from . import prestige as P

# モードID → (APIのキーの接頭辞, 表示名)
MODES: dict[str, tuple[str, str]] = {
    "overall": ("", "Overall"),
    "solo": ("eight_one_", "Solo"),
    "doubles": ("eight_two_", "Doubles"),
    "threes": ("four_three_", "3v3v3v3"),
    "fours": ("four_four_", "4v4v4v4"),
    "4v4": ("two_four_", "4v4"),
}
MODE_SHORT = {"solo": "1s", "doubles": "2s", "threes": "3s", "fours": "4s", "4v4": "4v4"}


def ratio(a: float, b: float) -> float:
    return a / b if b else float(a)


@dataclass
class ModeStats:
    games: int = 0
    wins: int = 0
    losses: int = 0
    kills: int = 0
    deaths: int = 0
    final_kills: int = 0
    final_deaths: int = 0
    beds_broken: int = 0
    beds_lost: int = 0
    winstreak: int | None = None  # API設定で非公開なら None
    iron: int = 0
    gold: int = 0
    diamond: int = 0
    emerald: int = 0

    @property
    def wlr(self): return ratio(self.wins, self.losses)
    @property
    def kdr(self): return ratio(self.kills, self.deaths)
    @property
    def fkdr(self): return ratio(self.final_kills, self.final_deaths)
    @property
    def bblr(self): return ratio(self.beds_broken, self.beds_lost)
    @property
    def win_rate(self): return ratio(self.wins * 100, self.wins + self.losses)
    @property
    def finals_per_game(self): return ratio(self.final_kills, self.games)
    @property
    def beds_per_game(self): return ratio(self.beds_broken, self.games)
    @property
    def kills_per_game(self): return ratio(self.kills, self.games)


@dataclass
class PlayerStats:
    username: str
    uuid: str = ""
    rank: str = ""                    # 例 '§6[MVP§c++§6]'
    exp: int = 0
    level_cosmetics: dict = field(default_factory=dict)
    tokens: int = 0
    slumber_tickets: int = 0
    slumber_total: int = 0
    slumber_wallet: int = 0
    modes: dict[str, ModeStats] = field(default_factory=dict)
    first_login: datetime | None = None
    last_login: datetime | None = None
    online: bool | None = None
    network_exp: int = 0
    karma: int = 0
    achievement_points: int = 0
    guild_name: str | None = None
    guild_tag: str | None = None
    guild_tag_color: str = "7"

    # ---- レベル関連 ----
    @property
    def level(self) -> int:
        return P.level_info(self.exp)[0]

    @property
    def level_exact(self) -> float:
        return P.level_float(self.exp)

    @property
    def level_progress(self) -> tuple[int, int]:
        _, cur, req = P.level_info(self.exp)
        return cur, req

    @property
    def level_formatted(self) -> str:
        """プレイヤーが選んでいる見た目（括弧・星・配色）を反映した表示。"""
        return P.format_level(self.level, **self.level_cosmetics)

    def formatted_level_of(self, level: int) -> str:
        """標準の見た目でのレベル表示（「次のレベル」用）。"""
        return P.format_level(level)

    @property
    def prestige_colors(self) -> list[tuple[int, int, int]]:
        return P.level_colors(self.level, self.level_cosmetics.get("scheme"))

    # ---- 名前 ----
    @property
    def name_color(self) -> str:
        return P.last_color(self.rank, "7") if self.rank else "7"

    @property
    def display_name(self) -> str:
        return f"{self.rank} §{self.name_color}{self.username}" if self.rank else f"§7{self.username}"

    @property
    def guild_tag_formatted(self) -> str:
        return f"§{self.guild_tag_color}[{self.guild_tag}]" if self.guild_tag else ""

    @property
    def network_level(self) -> float:
        return (math.sqrt(2 * self.network_exp + 30625) / 50) - 2.5 if self.network_exp else 1.0

    def mode(self, mode: str) -> ModeStats:
        return self.modes.get(mode) or ModeStats()


def _ts(ms) -> datetime | None:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc) if ms else None


_WALLETS = {
    "MINI_WALLET": 25, "LIGHT_SLUMBERS_WALLET": 99, "LIGHT_IMPERIAL_WALLET": 500,
    "EXPLORERS_WALLET": 5000, "HOTEL_STAFF_WALLET": 10_000, "PLATINUM_MEMBERSHIP_WALLET": 100_000,
}


def parse_player(player: dict, guild: dict | None = None, status: dict | None = None) -> PlayerStats:
    """Hypixel API v2 の player（/v2/player の "player"）を PlayerStats に変換。

    guild  … /v2/guild?player=UUID の "guild"（任意）
    status … /v2/status?uuid=UUID の "session"（任意）
    """
    player = player or {}
    bw = (player.get("stats") or {}).get("Bedwars") or {}

    modes = {}
    for key, (pre, _) in MODES.items():
        g = lambda k: int(bw.get(f"{pre}{k}") or 0)  # noqa: E731
        ws = bw.get(f"{pre}winstreak")
        modes[key] = ModeStats(
            games=g("games_played_bedwars"), wins=g("wins_bedwars"), losses=g("losses_bedwars"),
            kills=g("kills_bedwars"), deaths=g("deaths_bedwars"),
            final_kills=g("final_kills_bedwars"), final_deaths=g("final_deaths_bedwars"),
            beds_broken=g("beds_broken_bedwars"), beds_lost=g("beds_lost_bedwars"),
            winstreak=int(ws) if ws is not None else None,
            iron=g("iron_resources_collected_bedwars"), gold=g("gold_resources_collected_bedwars"),
            diamond=g("diamond_resources_collected_bedwars"),
            emerald=g("emerald_resources_collected_bedwars"),
        )

    slumber = bw.get("slumber") or {}
    stats = PlayerStats(
        username=player.get("displayname") or "Unknown",
        uuid=player.get("uuid") or "",
        rank=P.rank_prefix(player),
        exp=int(bw.get("Experience") or 0),
        level_cosmetics=P.cosmetics_from_api(bw),
        tokens=int(bw.get("coins") or 0),
        slumber_tickets=int(slumber.get("tickets") or 0),
        slumber_total=int(slumber.get("total_tickets_earned") or 0),
        slumber_wallet=_WALLETS.get(slumber.get("bag_type") or "", 0),
        modes=modes,
        first_login=_ts(player.get("firstLogin")),
        last_login=_ts(player.get("lastLogin")),
        network_exp=int(player.get("networkExp") or 0),
        karma=int(player.get("karma") or 0),
        achievement_points=int(player.get("achievementPoints") or 0),
    )
    if guild:
        stats.guild_name = guild.get("name")
        stats.guild_tag = guild.get("tag")
        stats.guild_tag_color = P.COLOR_NAMES.get(guild.get("tagColor") or "GRAY", "7")
    if status is not None:
        stats.online = bool(status.get("online"))
    return stats
