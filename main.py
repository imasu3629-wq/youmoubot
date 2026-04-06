import os
import re
import io
import base64
import asyncio
from datetime import time
from threading import Thread
from typing import Optional
from zoneinfo import ZoneInfo

import discord
import requests
from discord import app_commands
from discord.ext import tasks
from flask import Flask

from database import (
    IntegrityError,
    add_hidden_urchin_mcid,
    add_manual_tag,
    delete_registered_player_data_by_uuid,
    delete_player_registration_by_uuid,
    get_all_registered_players,
    get_cached_uuid,
    get_config_value,
    get_player_by_discord,
    get_player_by_uuid,
    get_player_by_username,
    get_registered_player_by_mcid,
    get_player_stats_by_uuid,
    get_ranking_message_state,
    get_registered_mcids_for_autocomplete,
    get_top_player_stats,
    has_manual_tag,
    init_db,
    is_hidden_urchin_mcid,
    list_hidden_urchin_mcids,
    list_manual_tags_for_mcid,
    list_mcids_by_manual_tag,
    remove_hidden_urchin_mcid,
    remove_manual_tag,
    register_verified_player,
    save_uuid_cache,
    set_config_value,
    update_player_head_image,
    update_player_uuid,
    upsert_ranking_message_state,
    upsert_player_stats,
)
from ranking_renderer import (
    RankingRenderError,
    fetch_head_base64_from_uuid,
    render_ranking_image,
    render_stats_image,
)
from urchin_tags import fetch_urchin_tags, get_highest_priority_urchin_tag

REQUEST_TIMEOUT = 10
HYPIXEL_PLAYER_URL = "https://api.hypixel.net/v2/player"
FLASHLIGHT_PLAYERDATA_URL = "https://flashlight.prismoverlay.com/v1/playerdata"
MOJANG_PROFILE_URL = "https://api.mojang.com/users/profiles/minecraft/{mcid}"
SESSION_PROFILE_URL = "https://sessionserver.mojang.com/session/minecraft/profile/{uuid}"
ADMIN_GUILD_ID = 1343197242533871676
ADMIN_ROLE_ID = 1490306531940368506
DAILY_UPDATE_TIME = time(hour=3, minute=0, tzinfo=ZoneInfo("Asia/Tokyo"))
UPDATE_ALL_DELAY_SECONDS = 1.0
RANKING_GUILD_ID = 1343197242533871676
RANKING_CHANNELS = {
    "star": 1490341322719232010,
    "fkdr": 1490341920004636744,
    "bblr": 1490341978792267897,
    "wlr": 1490342088762724465,
}
RANKING_PAGE_SIZE = 10
RANKING_MAX_PLAYERS = 100
STARTUP_RANKINGS_POSTED = False
MANUAL_TAG_ASSET_MAP = {
    "zero": "Zero.PNG",
}

# --- 24時間稼働設定 ---
app = Flask("")


@app.route("/")
def home():
    return "Bot is alive!"



def run():
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)



def keep_alive():
    thread = Thread(target=run)
    thread.start()


TOKEN = os.environ["DISCORD_TOKEN"]
current_api_key = os.environ.get("HYPIXEL_KEY", "")

intents = discord.Intents.default()
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


class VerificationError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class PlayerDataFetchError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class AdminPermissionError(app_commands.CheckFailure):
    pass


def admin_only():
    async def predicate(interaction: discord.Interaction) -> bool:
        if interaction.guild is None:
            raise AdminPermissionError("❌ This command cannot be used in DMs.")
        if interaction.guild.id != ADMIN_GUILD_ID:
            raise AdminPermissionError("❌ This command can only be used in the designated guild.")
        if not isinstance(interaction.user, discord.Member):
            raise AdminPermissionError("❌ Could not validate your server membership.")
        if not any(role.id == ADMIN_ROLE_ID for role in interaction.user.roles):
            raise AdminPermissionError("❌ You do not have permission to use this command.")
        return True

    return app_commands.check(predicate)



def normalize_text(value: str) -> str:
    return "".join(value.strip().lower().split()).lstrip("@")



def get_discord_identity_candidates(user: discord.abc.User):
    candidates = {normalize_text(user.name), normalize_text(f"@{user.name}")}
    if getattr(user, "global_name", None):
        candidates.add(normalize_text(user.global_name))
        candidates.add(normalize_text(f"@{user.global_name}"))
    if getattr(user, "discriminator", "0") not in (None, "0"):
        tag = f"{user.name}#{user.discriminator}"
        candidates.add(normalize_text(tag))
        candidates.add(normalize_text(f"@{tag}"))
    return {candidate for candidate in candidates if candidate}



def fetch_current_name(uuid: str):
    response = requests.get(
        SESSION_PROFILE_URL.format(uuid=uuid), timeout=REQUEST_TIMEOUT
    )
    if response.status_code != 200:
        return None
    return response.json().get("name")



def fetch_player_profile(mcid: str):
    normalized_mcid = mcid.strip()
    if not normalized_mcid:
        return None

    cached_uuid = get_cached_uuid(normalized_mcid)
    if cached_uuid:
        official_name = fetch_current_name(cached_uuid)
        return {"uuid": cached_uuid, "name": official_name or normalized_mcid}

    response = requests.get(
        MOJANG_PROFILE_URL.format(mcid=normalized_mcid), timeout=REQUEST_TIMEOUT
    )
    if response.status_code == 204:
        return None
    if response.status_code != 200:
        raise VerificationError("❌ Mojang API からプレイヤー情報を取得できませんでした。")

    data = response.json()
    minecraft_uuid = data.get("id")
    official_name = data.get("name")
    if not minecraft_uuid:
        return None

    save_uuid_cache(normalized_mcid, minecraft_uuid)
    if official_name:
        save_uuid_cache(official_name, minecraft_uuid)

    return {"uuid": minecraft_uuid, "name": official_name or normalized_mcid}



def fetch_hypixel_player(uuid: str):
    api_key = get_hypixel_api_key()
    if not api_key:
        raise VerificationError("❌ Hypixel API key is not configured. Use /updateapi first.")

    response = requests.get(
        HYPIXEL_PLAYER_URL,
        params={"key": api_key, "uuid": uuid},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code == 429:
        raise VerificationError("⚠️ Hypixel API のレート制限に達しました。少し待ってから再試行してください。")
    if response.status_code != 200:
        raise VerificationError("❌ Hypixel API からプレイヤー情報を取得できませんでした。")

    payload = response.json()
    if not payload.get("success", True):
        raise VerificationError("❌ Hypixel API がエラーを返しました。")

    player = payload.get("player")
    if not player:
        raise VerificationError("❌ Hypixel にプレイヤーデータが見つかりませんでした。")
    return player



def get_hypixel_api_key() -> str:
    stored_key = get_config_value("hypixel_api_key")
    if stored_key:
        return stored_key
    return current_api_key


def _safe_int(value) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_ratio(numerator: int, denominator: int) -> float:
    return round(numerator / max(denominator, 1), 2)


def _calculate_bedwars_star_from_experience(total_experience: int) -> int:
    exp = max(_safe_int(total_experience), 0)
    exp_per_prestige = 482_000
    prestiges = exp // exp_per_prestige
    remaining_exp = exp % exp_per_prestige

    if remaining_exp < 500:
        level_within_prestige = 0
    elif remaining_exp < 1_500:
        level_within_prestige = 1
    elif remaining_exp < 3_500:
        level_within_prestige = 2
    elif remaining_exp < 7_000:
        level_within_prestige = 3
    else:
        level_within_prestige = 4 + ((remaining_exp - 7_000) // 5_000)

    return int(prestiges * 100 + min(level_within_prestige, 99))


def _normalize_uuid(uuid: str) -> str:
    compact = str(uuid or "").replace("-", "").strip().lower()
    if len(compact) != 32 or any(ch not in "0123456789abcdef" for ch in compact):
        raise PlayerDataFetchError("❌ Invalid UUID format.")
    return compact


def _get_nested_value(payload: dict, path: tuple[str, ...]):
    current = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _first_non_none(payload: dict, paths: list[tuple[str, ...]]):
    for path in paths:
        value = _get_nested_value(payload, path)
        if value is not None:
            return value
    return None


def _sanitize_filename(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]+", "_", value.strip().lower())
    return cleaned or "ranking"


def _ensure_head_image_base64(uuid: str) -> Optional[str]:
    head_image_base64 = fetch_head_base64_from_uuid(uuid)
    if head_image_base64:
        update_player_head_image(uuid, head_image_base64)
    return head_image_base64


def fetch_playerdata_from_flashlight(uuid: str) -> dict:
    normalized_uuid = _normalize_uuid(uuid)
    try:
        response = requests.get(
            FLASHLIGHT_PLAYERDATA_URL,
            params={"uuid": normalized_uuid},
            timeout=REQUEST_TIMEOUT,
        )
    except requests.Timeout:
        raise PlayerDataFetchError("⚠️ Flashlight API request timed out.")
    except requests.RequestException as error:
        raise PlayerDataFetchError(f"⚠️ Flashlight API request failed: {error}")

    if response.status_code == 404:
        raise PlayerDataFetchError("❌ Flashlight にプレイヤーデータが見つかりませんでした。")
    if response.status_code != 200:
        raise PlayerDataFetchError(
            f"❌ Flashlight API returned non-200 response: {response.status_code}"
        )

    try:
        payload = response.json()
    except ValueError:
        raise PlayerDataFetchError("❌ Flashlight API returned invalid JSON.")

    if not isinstance(payload, dict):
        raise PlayerDataFetchError("❌ Flashlight API payload is malformed.")

    return payload


def normalize_flashlight_playerdata(requested_uuid: str, payload: dict) -> dict:
    bedwars_blob = _first_non_none(
        payload,
        [
            ("bedwars",),
            ("stats", "bedwars"),
            ("stats", "Bedwars"),
            ("player", "stats", "bedwars"),
            ("player", "stats", "Bedwars"),
            ("data", "bedwars"),
            ("data", "stats", "bedwars"),
            ("playerData", "bedwars"),
            ("playerData", "stats", "bedwars"),
        ],
    )
    if not isinstance(bedwars_blob, dict):
        raise PlayerDataFetchError("❌ Flashlight payload is missing Bedwars stats.")

    achievements_blob = _first_non_none(
        payload,
        [
            ("achievements",),
            ("player", "achievements"),
            ("data", "achievements"),
            ("playerData", "achievements"),
        ],
    )
    if not isinstance(achievements_blob, dict):
        achievements_blob = {}

    resolved_uuid = str(
        _first_non_none(
            payload,
            [("uuid",), ("player", "uuid"), ("data", "uuid"), ("playerData", "uuid")],
        )
        or requested_uuid
    )
    resolved_name = str(
        _first_non_none(
            payload,
            [
                ("minecraft_name",),
                ("username",),
                ("name",),
                ("displayname",),
                ("player", "displayname"),
                ("player", "name"),
                ("data", "name"),
                ("playerData", "name"),
            ],
        )
        or fetch_current_name(requested_uuid)
        or requested_uuid
    )

    bedwars_experience = _safe_int(
        _first_non_none(
            bedwars_blob,
            [
                ("Experience",),
                ("experience",),
                ("exp",),
                ("Exp",),
            ],
        )
    )
    bedwars_star = _calculate_bedwars_star_from_experience(bedwars_experience)
    if bedwars_star == 0:
        bedwars_star = _safe_int(
            _first_non_none(
                payload,
                [
                    ("bedwars_star",),
                    ("bedwars", "star"),
                    ("stats", "bedwars", "star"),
                    ("stats", "bedwars", "level"),
                    ("stats", "Bedwars", "star"),
                    ("stats", "Bedwars", "level"),
                    ("player", "achievements", "bedwars_level"),
                    ("achievements", "bedwars_level"),
                ],
            )
        )
    if bedwars_star == 0:
        bedwars_star = _safe_int(
            _first_non_none(
                achievements_blob,
                [("bedwars_level",), ("bedwars_star",), ("bedwarsLevel",)],
            )
        )

    normalized = {
        "minecraft_uuid": _normalize_uuid(resolved_uuid),
        "minecraft_name": resolved_name,
        "bedwars_star": bedwars_star,
        "wins": _safe_int(_first_non_none(bedwars_blob, [("wins",), ("wins_bedwars",)])),
        "losses": _safe_int(_first_non_none(bedwars_blob, [("losses",), ("losses_bedwars",)])),
        "final_kills": _safe_int(_first_non_none(bedwars_blob, [("final_kills",), ("final_kills_bedwars",)])),
        "final_deaths": _safe_int(_first_non_none(bedwars_blob, [("final_deaths",), ("final_deaths_bedwars",)])),
        "beds_broken": _safe_int(_first_non_none(bedwars_blob, [("beds_broken",), ("beds_broken_bedwars",)])),
        "beds_lost": _safe_int(_first_non_none(bedwars_blob, [("beds_lost",), ("beds_lost_bedwars",)])),
        "kills": _safe_int(_first_non_none(bedwars_blob, [("kills",), ("kills_bedwars",)])),
        "deaths": _safe_int(_first_non_none(bedwars_blob, [("deaths",), ("deaths_bedwars",)])),
        "games_played": _safe_int(
            _first_non_none(bedwars_blob, [("games_played",), ("games_played_bedwars",)])
        ),
        "winstreak": _safe_int(_first_non_none(bedwars_blob, [("winstreak",), ("win_streak",)])),
    }
    normalized["fkdr"] = _safe_ratio(normalized["final_kills"], normalized["final_deaths"])
    normalized["wlr"] = _safe_ratio(normalized["wins"], normalized["losses"])
    normalized["kdr"] = _safe_ratio(normalized["kills"], normalized["deaths"])
    normalized["raw_flashlight_json"] = payload
    return normalized


def refresh_player_identity(mcid_or_uuid: str) -> Optional[str]:
    target = mcid_or_uuid.strip()
    if not target:
        return None

    is_uuid_format = False
    compact = target.replace("-", "").lower()
    if len(compact) == 32 and all(ch in "0123456789abcdef" for ch in compact):
        is_uuid_format = True

    if is_uuid_format:
        current_name = fetch_current_name(compact)
        if not current_name:
            return compact
        profile = fetch_player_profile(current_name)
    else:
        profile = fetch_player_profile(target)

    if not profile:
        return None

    new_uuid = profile["uuid"]
    new_name = profile["name"]
    save_uuid_cache(target, new_uuid)
    if new_name:
        save_uuid_cache(new_name, new_uuid)

    existing = get_player_by_uuid(target) if is_uuid_format else get_player_by_username(target)

    if existing and existing["minecraft_uuid"] != new_uuid:
        update_player_uuid(existing["minecraft_uuid"], new_uuid, new_name)

    _ensure_head_image_base64(new_uuid)
    return new_uuid


def fetch_and_store_player_stats(uuid: str):
    payload = fetch_playerdata_from_flashlight(uuid)
    normalized = normalize_flashlight_playerdata(uuid, payload)
    head_image_base64 = fetch_head_base64_from_uuid(normalized["minecraft_uuid"])
    upsert_player_stats(
        minecraft_uuid=normalized["minecraft_uuid"],
        minecraft_name=normalized["minecraft_name"],
        bedwars_star=normalized["bedwars_star"],
        wins=normalized["wins"],
        losses=normalized["losses"],
        final_kills=normalized["final_kills"],
        final_deaths=normalized["final_deaths"],
        beds_broken=normalized["beds_broken"],
        beds_lost=normalized["beds_lost"],
        kills=normalized["kills"],
        deaths=normalized["deaths"],
        games_played=normalized["games_played"],
        winstreak=normalized["winstreak"],
        fkdr=normalized["fkdr"],
        wlr=normalized["wlr"],
        kdr=normalized["kdr"],
        head_image_base64=head_image_base64,
        raw_flashlight_json=normalized["raw_flashlight_json"],
    )


def resolve_target_uuid(mcid_or_uuid: str) -> Optional[str]:
    value = mcid_or_uuid.strip()
    if not value:
        return None

    compact = value.replace("-", "").lower()
    if len(compact) == 32 and all(ch in "0123456789abcdef" for ch in compact):
        return _normalize_uuid(compact)

    profile = fetch_player_profile(value)
    if not profile:
        return None
    return profile["uuid"]


def get_linked_discord_text(player_data) -> str | None:
    social_media = player_data.get("socialMedia", {})
    links = social_media.get("links", {}) if social_media else {}
    discord_text = links.get("DISCORD")
    if not discord_text:
        return None
    return str(discord_text).strip()



def verify_hypixel_link(interaction_user: discord.abc.User, linked_discord_text: str):
    normalized_link = normalize_text(linked_discord_text)
    if not normalized_link:
        return False
    return normalized_link in get_discord_identity_candidates(interaction_user)



def register_verified_account(discord_user: discord.abc.User, mcid: str):
    existing_for_user = get_player_by_discord(discord_user.id)
    if existing_for_user:
        raise VerificationError("⚠️ You already have a registered MCID.")

    profile = fetch_player_profile(mcid)
    if not profile:
        raise VerificationError("❌ Invalid MCID / username not found.")

    existing_for_uuid = get_player_by_uuid(profile["uuid"])
    if existing_for_uuid:
        raise VerificationError("⚠️ This Minecraft account is already registered to another Discord user.")

    player_data = fetch_hypixel_player(profile["uuid"])
    linked_discord_text = get_linked_discord_text(player_data)
    if not linked_discord_text:
        raise VerificationError("❌ This Hypixel account does not have a linked Discord account.")

    if not verify_hypixel_link(discord_user, linked_discord_text):
        raise VerificationError("❌ The linked Discord account on Hypixel does not match your Discord account.")

    try:
        register_verified_player(
            minecraft_uuid=profile["uuid"],
            minecraft_username=profile["name"],
            discord_user_id=discord_user.id,
            linked_discord_text=linked_discord_text,
            verification_status="verified",
            registered_by_admin=False,
        )
    except IntegrityError:
        raise VerificationError("⚠️ Database uniqueness conflict occurred while saving the registration.")

    return {
        "minecraft_uuid": profile["uuid"],
        "minecraft_username": profile["name"],
        "linked_discord_text": linked_discord_text,
        "verification_status": "verified",
    }


def _chunk_rows(rows: list[dict], size: int) -> list[list[dict]]:
    return [rows[i:i + size] for i in range(0, len(rows), size)]


def _normalize_tag_key(tag_name: str) -> str:
    return str(tag_name or "").strip().lower()


def list_supported_manual_tags() -> list[str]:
    return sorted(MANUAL_TAG_ASSET_MAP.keys())


def _resolve_urchin_tag_for_mcid(mcid: str) -> dict | None:
    tags = fetch_urchin_tags(str(mcid or ""))
    return get_highest_priority_urchin_tag(tags)


def resolve_display_tag_for_mcid(
    mcid: str,
    hidden_urchin: Optional[bool] = None,
    urchin_tag: Optional[dict] = None,
) -> dict[str, str] | None:
    normalized_mcid = str(mcid or "").strip()
    if not normalized_mcid:
        return None

    resolved_hidden_urchin = is_hidden_urchin_mcid(normalized_mcid) if hidden_urchin is None else hidden_urchin
    resolved_urchin_tag = _resolve_urchin_tag_for_mcid(normalized_mcid) if urchin_tag is None else urchin_tag
    if resolved_urchin_tag and not resolved_hidden_urchin:
        return {"source": "urchin", "tag": str(resolved_urchin_tag.get("tag") or "")}

    manual_tags = list_manual_tags_for_mcid(normalized_mcid)
    for manual_tag in manual_tags:
        if _normalize_tag_key(manual_tag) in MANUAL_TAG_ASSET_MAP:
            return {"source": "manual", "tag": _normalize_tag_key(manual_tag)}
    return None


def _attach_resolved_tag(row: dict | None) -> dict | None:
    if not row:
        return row
    mcid = str(row.get("minecraft_name") or "")
    hidden_urchin = is_hidden_urchin_mcid(mcid)
    urchin_tag = _resolve_urchin_tag_for_mcid(mcid)
    display_tag = resolve_display_tag_for_mcid(mcid, hidden_urchin=hidden_urchin, urchin_tag=urchin_tag)

    enriched_row = dict(row)
    enriched_row["hidden_urchin_tag"] = hidden_urchin
    enriched_row["urchin_tag"] = urchin_tag if (urchin_tag and not hidden_urchin) else None
    enriched_row["display_tag"] = display_tag
    return enriched_row


async def update_all_players() -> tuple[int, int]:
    players = get_all_registered_players()
    success = 0
    failed = 0

    for player in players:
        try:
            refreshed_uuid = refresh_player_identity(player["minecraft_username"]) or player["minecraft_uuid"]
            fetch_and_store_player_stats(refreshed_uuid)
            success += 1
        except Exception as error:
            failed += 1
            print(
                "⚠️ Failed to update player "
                f"{player.get('minecraft_username', '(unknown)')} "
                f"({player.get('minecraft_uuid', '(unknown uuid)')}): {error}"
            )
        finally:
            await asyncio.sleep(UPDATE_ALL_DELAY_SECONDS)

    return success, failed


async def _refresh_ranking_channel(
    guild: discord.Guild,
    ranking_type: str,
    channel_id: int,
) -> tuple[int, int]:
    channel = guild.get_channel(channel_id)
    if not isinstance(channel, discord.TextChannel):
        print(f"⚠️ Ranking channel not found or not text: type={ranking_type}, channel_id={channel_id}")
        return 0, 0

    old_state = get_ranking_message_state(guild.id, channel_id, ranking_type)
    old_message_ids = old_state["message_ids"] if old_state else []
    deleted = await _delete_old_ranking_messages(channel, ranking_type, old_message_ids)

    rows = get_top_player_stats(ranking_type, limit=RANKING_MAX_PLAYERS)
    if not rows:
        upsert_ranking_message_state(guild.id, channel_id, ranking_type, [])
        return deleted, 0

    hydrated_rows = []
    for row in rows:
        if not row["head_image_base64"]:
            head_image_base64 = _ensure_head_image_base64(row["minecraft_uuid"])
            if head_image_base64:
                fetch_and_store_player_stats(row["minecraft_uuid"])
        refreshed_row = get_player_stats_by_uuid(row["minecraft_uuid"])
        hydrated_rows.append(_attach_resolved_tag(refreshed_row or row))

    pages = _chunk_rows(hydrated_rows, RANKING_PAGE_SIZE)
    sent_message_ids = []
    for page_idx, page_rows in enumerate(pages):
        image_bytes = render_ranking_image(
            page_rows,
            ranking_type,
            show_title=(page_idx == 0),
            rank_start=(page_idx * RANKING_PAGE_SIZE) + 1,
        )
        filename = (
            f"bedwars_{_sanitize_filename(ranking_type)}_ranking_"
            f"p{page_idx + 1}.png"
        )
        file = discord.File(image_bytes, filename=filename)
        sent_message = await channel.send(file=file)
        sent_message_ids.append(sent_message.id)

    upsert_ranking_message_state(guild.id, channel_id, ranking_type, sent_message_ids)
    return deleted, len(sent_message_ids)


async def _delete_old_ranking_messages(
    channel: discord.TextChannel,
    ranking_type: str,
    stored_message_ids: list[int],
) -> int:
    deleted = 0
    seen_message_ids = set()
    for message_id in stored_message_ids:
        try:
            message = await channel.fetch_message(int(message_id))
            if message.author.id != bot.user.id:
                continue
            await message.delete()
            deleted += 1
            seen_message_ids.add(int(message_id))
        except discord.NotFound:
            continue
        except discord.Forbidden as error:
            print(f"⚠️ No permission to delete ranking message {message_id}: {error}")
        except discord.HTTPException as error:
            print(f"⚠️ Failed to delete ranking message {message_id}: {error}")

    ranking_prefix = f"bedwars_{_sanitize_filename(ranking_type)}_ranking_"
    try:
        async for message in channel.history(limit=200):
            if message.author.id != bot.user.id or message.id in seen_message_ids:
                continue
            if not message.attachments:
                continue
            if any(att.filename.startswith(ranking_prefix) for att in message.attachments):
                try:
                    await message.delete()
                    deleted += 1
                except discord.Forbidden as error:
                    print(f"⚠️ No permission to delete ranking message {message.id}: {error}")
                except discord.HTTPException as error:
                    print(f"⚠️ Failed to delete ranking message {message.id}: {error}")
    except discord.Forbidden as error:
        print(f"⚠️ No permission to read channel history for ranking cleanup: {error}")
    except discord.HTTPException as error:
        print(f"⚠️ Failed to read channel history for ranking cleanup: {error}")

    return deleted


async def refresh_all_rankings() -> dict[str, tuple[int, int]]:
    guild = bot.get_guild(RANKING_GUILD_ID)
    if guild is None:
        raise RuntimeError(f"Ranking target guild not found: {RANKING_GUILD_ID}")

    results: dict[str, tuple[int, int]] = {}
    for ranking_type, channel_id in RANKING_CHANNELS.items():
        try:
            results[ranking_type] = await _refresh_ranking_channel(guild, ranking_type, channel_id)
        except Exception as error:
            print(f"⚠️ Failed to refresh ranking {ranking_type}: {error}")
            results[ranking_type] = (0, 0)
    return results


def admin_force_register_mcid(mcid: str):
    profile = fetch_player_profile(mcid)
    if not profile:
        raise VerificationError("❌ Invalid MCID / username not found.")

    existing_for_uuid = get_player_by_uuid(profile["uuid"])
    if existing_for_uuid:
        raise VerificationError("⚠️ This MCID is already registered.")

    try:
        register_verified_player(
            minecraft_uuid=profile["uuid"],
            minecraft_username=profile["name"],
            discord_user_id=None,
            linked_discord_text=None,
            verification_status="forced_verified",
            registered_by_admin=True,
        )
    except IntegrityError:
        raise VerificationError("⚠️ This MCID is already registered.")

    return {
        "minecraft_uuid": profile["uuid"],
        "minecraft_username": profile["name"],
        "verification_status": "forced_verified",
    }




async def registered_mcid_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    candidates = get_registered_mcids_for_autocomplete(current, limit=25)
    return [app_commands.Choice(name=mcid, value=mcid) for mcid in candidates]


async def manual_tag_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    normalized_current = _normalize_tag_key(current)
    choices = []
    for tag_name in list_supported_manual_tags():
        if normalized_current and not tag_name.startswith(normalized_current):
            continue
        choices.append(app_commands.Choice(name=tag_name.capitalize(), value=tag_name))
    return choices[:25]


async def manual_tagged_mcid_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    namespace = getattr(interaction, "namespace", None)
    selected_tag = _normalize_tag_key(getattr(namespace, "tag_name", "") if namespace else "")
    if not selected_tag:
        return await registered_mcid_autocomplete(interaction, current)
    mcids = list_mcids_by_manual_tag(selected_tag, prefix=current, limit=25)
    return [app_commands.Choice(name=mcid, value=mcid) for mcid in mcids]


@bot.event
async def on_ready():
    global STARTUP_RANKINGS_POSTED
    init_db()
    if not daily_update_all_task.is_running():
        daily_update_all_task.start()
    if not STARTUP_RANKINGS_POSTED:
        try:
            results = await refresh_all_rankings()
            print(f"📌 Startup ranking refresh complete: {results}")
            STARTUP_RANKINGS_POSTED = True
        except Exception as error:
            print(f"⚠️ Startup ranking refresh failed: {error}")
    try:
        synced = await tree.sync()
        print(f"✅ {len(synced)}個のコマンドを同期しました")
    except Exception as error:
        print(f"❌ 同期エラー: {error}")
    print(f"✅ Logged in as {bot.user.name}")


@tree.command(name="verify", description="Hypixel連携Discordを使ってMCIDを検証・登録します")
async def verify(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)
    try:
        result = register_verified_account(interaction.user, mcid)
    except VerificationError as error:
        await interaction.followup.send(error.message, ephemeral=True)
        return
    except requests.RequestException:
        await interaction.followup.send(
            "⚠️ ネットワークエラーが発生しました。少し待ってから再試行してください。",
            ephemeral=True,
        )
        return
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}", ephemeral=True)
        return

    embed = discord.Embed(
        title="✅ Verification successful",
        description="Your MCID has been registered.",
        color=0x2ECC71,
    )
    embed.add_field(name="MCID", value=result["minecraft_username"], inline=False)
    embed.add_field(name="Minecraft UUID", value=result["minecraft_uuid"], inline=False)
    embed.add_field(
        name="Linked Discord on Hypixel",
        value=result["linked_discord_text"],
        inline=False,
    )
    embed.add_field(
        name="Verification Status",
        value=result["verification_status"],
        inline=False,
    )
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="whoami", description="自分の登録済みMCID情報を確認します")
async def whoami(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    row = get_player_by_discord(interaction.user.id)
    if not row:
        await interaction.followup.send("ℹ️ 現在登録されている MCID はありません。", ephemeral=True)
        return

    embed = discord.Embed(title="📌 Registered MCID", color=0x3498DB)
    embed.add_field(name="MCID", value=row["minecraft_username"], inline=False)
    embed.add_field(name="Minecraft UUID", value=row["minecraft_uuid"], inline=False)
    embed.add_field(name="Linked Discord", value=row["linked_discord_text"] or "(none)", inline=False)
    embed.add_field(name="Status", value=row["verification_status"], inline=False)
    embed.add_field(name="Registered At", value=row["registered_at"], inline=False)
    embed.add_field(name="Updated At", value=row["updated_at"], inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="lookup", description="登録済みMCIDを確認します（管理者用）")
@admin_only()
async def lookup(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)

    try:
        profile = fetch_player_profile(mcid)
    except VerificationError as error:
        await interaction.followup.send(error.message, ephemeral=True)
        return
    except requests.RequestException:
        await interaction.followup.send("⚠️ Mojang API への接続に失敗しました。", ephemeral=True)
        return

    if not profile:
        await interaction.followup.send("❌ Invalid MCID / username not found.", ephemeral=True)
        return

    row = get_player_by_uuid(profile["uuid"])
    if not row:
        await interaction.followup.send("ℹ️ This MCID is not registered.", ephemeral=True)
        return

    embed = discord.Embed(title="🔍 Registration Lookup", color=0x9B59B6)
    embed.add_field(name="Minecraft Username", value=row["minecraft_username"], inline=False)
    embed.add_field(name="Minecraft UUID", value=row["minecraft_uuid"], inline=False)
    embed.add_field(name="Discord User ID", value=row["discord_user_id"] or "(none)", inline=False)
    embed.add_field(name="Verification Status", value=row["verification_status"], inline=False)
    embed.add_field(name="Registered At", value=row["registered_at"], inline=False)
    embed.add_field(name="Registered By Admin", value="true" if row["registered_by_admin"] else "false", inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="resetverify", description="登録済みMCIDをリセットします（管理者用）")
@app_commands.describe(mcid="Minecraft username")
@admin_only()
async def resetverify(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)

    try:
        profile = fetch_player_profile(mcid)
    except VerificationError as error:
        await interaction.followup.send(error.message, ephemeral=True)
        return
    except requests.RequestException:
        await interaction.followup.send("⚠️ Mojang API への接続に失敗しました。", ephemeral=True)
        return

    deleted = False
    if profile:
        deleted = delete_player_registration_by_uuid(profile["uuid"])

    if not deleted:
        await interaction.followup.send("ℹ️ This MCID is not registered.", ephemeral=True)
        return

    await interaction.followup.send("✅ Registration reset successfully.", ephemeral=True)


@tree.command(name="add", description="管理者がMCIDを強制登録します")
@admin_only()
async def add(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)

    try:
        result = admin_force_register_mcid(mcid)
    except VerificationError as error:
        await interaction.followup.send(error.message, ephemeral=True)
        return
    except requests.RequestException:
        await interaction.followup.send(
            "⚠️ Network error while contacting Mojang/Hypixel services. Please try again.",
            ephemeral=True,
        )
        return
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}", ephemeral=True)
        return

    embed = discord.Embed(
        title="✅ MCID registered successfully through admin override.",
        color=0x2ECC71,
    )
    embed.add_field(name="MCID", value=result["minecraft_username"], inline=False)
    embed.add_field(name="Minecraft UUID", value=result["minecraft_uuid"], inline=False)
    embed.add_field(name="Verification Status", value=result["verification_status"], inline=False)
    await interaction.followup.send(embed=embed, ephemeral=True)


@tree.command(name="delete", description="管理者が登録済みMCIDを削除します")
@admin_only()
async def delete(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)

    try:
        profile = fetch_player_profile(mcid)
    except VerificationError as error:
        await interaction.followup.send(error.message, ephemeral=True)
        return
    except requests.RequestException:
        await interaction.followup.send(
            "⚠️ Network error while contacting Mojang services. Please try again.",
            ephemeral=True,
        )
        return

    if not profile:
        await interaction.followup.send("❌ Invalid MCID / username not found.", ephemeral=True)
        return

    deleted = delete_registered_player_data_by_uuid(profile["uuid"])
    if not deleted:
        await interaction.followup.send("ℹ️ This MCID is not registered.", ephemeral=True)
        return

    await interaction.followup.send("✅ Registered MCID and related data deleted successfully.", ephemeral=True)


@tree.command(name="kaku", description="Urchinタグ表示を非表示にします（管理者用）")
@app_commands.describe(mcid="Minecraft username")
@app_commands.autocomplete(mcid=registered_mcid_autocomplete)
@admin_only()
async def kaku(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)

    registered = get_registered_player_by_mcid(mcid)
    if not registered:
        await interaction.followup.send("❌ This MCID is not registered in the bot database.", ephemeral=True)
        return

    added = add_hidden_urchin_mcid(str(registered["mcid"]), interaction.user.id)
    if not added:
        await interaction.followup.send("ℹ️ This MCID is already in the hidden-Urchin list.", ephemeral=True)
        return
    await interaction.followup.send(f"✅ Hidden Urchin tag output for `{registered['mcid']}`.", ephemeral=True)


@tree.command(name="kakulist", description="Urchinタグ非表示MCID一覧を表示します（管理者用）")
@admin_only()
async def kakulist(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    hidden_mcids = list_hidden_urchin_mcids()
    if not hidden_mcids:
        await interaction.followup.send("ℹ️ Hidden-Urchin list is empty.", ephemeral=True)
        return
    formatted = "\n".join(f"- {mcid}" for mcid in hidden_mcids)
    await interaction.followup.send(f"📋 Hidden Urchin MCIDs:\n{formatted}", ephemeral=True)


@tree.command(name="kakuno", description="Urchinタグ非表示を解除します（管理者用）")
@app_commands.describe(mcid="Minecraft username")
@app_commands.autocomplete(mcid=registered_mcid_autocomplete)
@admin_only()
async def kakuno(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)

    removed = remove_hidden_urchin_mcid(mcid)
    if not removed:
        await interaction.followup.send("ℹ️ This MCID is not in the hidden-Urchin list.", ephemeral=True)
        return
    await interaction.followup.send(f"✅ Restored Urchin tag visibility for `{mcid}`.", ephemeral=True)


@tree.command(name="tag", description="手動カスタムタグを設定します（管理者用）")
@app_commands.describe(tag_name="Manual custom tag", mcid="Minecraft username")
@app_commands.autocomplete(tag_name=manual_tag_autocomplete, mcid=registered_mcid_autocomplete)
@admin_only()
async def tag(interaction: discord.Interaction, tag_name: str, mcid: str):
    await interaction.response.defer(ephemeral=True)

    normalized_tag = _normalize_tag_key(tag_name)
    if normalized_tag not in list_supported_manual_tags():
        await interaction.followup.send("❌ Unsupported manual tag.", ephemeral=True)
        return

    registered = get_registered_player_by_mcid(mcid)
    if not registered:
        await interaction.followup.send("❌ This MCID is not registered in the bot database.", ephemeral=True)
        return

    canonical_mcid = str(registered["mcid"])
    if has_manual_tag(canonical_mcid, normalized_tag):
        await interaction.followup.send(f"ℹ️ `{canonical_mcid}` already has manual tag `{normalized_tag}`.", ephemeral=True)
        return

    added = add_manual_tag(canonical_mcid, normalized_tag, interaction.user.id)
    if not added:
        await interaction.followup.send("⚠️ Failed to add manual tag.", ephemeral=True)
        return
    await interaction.followup.send(f"✅ Added manual tag `{normalized_tag}` to `{canonical_mcid}`.", ephemeral=True)


@tree.command(name="tagremove", description="手動カスタムタグを削除します（管理者用）")
@app_commands.describe(tag_name="Manual custom tag", mcid="Minecraft username")
@app_commands.autocomplete(tag_name=manual_tag_autocomplete, mcid=manual_tagged_mcid_autocomplete)
@admin_only()
async def tagremove(interaction: discord.Interaction, tag_name: str, mcid: str):
    await interaction.response.defer(ephemeral=True)

    normalized_tag = _normalize_tag_key(tag_name)
    if normalized_tag not in list_supported_manual_tags():
        await interaction.followup.send("❌ Unsupported manual tag.", ephemeral=True)
        return

    registered = get_registered_player_by_mcid(mcid)
    if not registered:
        await interaction.followup.send("❌ This MCID is not registered in the bot database.", ephemeral=True)
        return

    canonical_mcid = str(registered["mcid"])
    removed = remove_manual_tag(canonical_mcid, normalized_tag)
    if not removed:
        await interaction.followup.send(f"ℹ️ `{canonical_mcid}` does not have manual tag `{normalized_tag}`.", ephemeral=True)
        return
    await interaction.followup.send(f"✅ Removed manual tag `{normalized_tag}` from `{canonical_mcid}`.", ephemeral=True)




@tree.command(name="updateapi", description="Hypixel API キーを更新します（管理者用）")
@admin_only()
async def updateapi(interaction: discord.Interaction, api_key: str):
    await interaction.response.defer(ephemeral=True)

    cleaned_key = api_key.strip()
    if not cleaned_key:
        await interaction.followup.send("❌ API key cannot be empty.", ephemeral=True)
        return

    global current_api_key
    current_api_key = cleaned_key
    set_config_value("hypixel_api_key", cleaned_key)
    await interaction.followup.send("Hypixel API key updated successfully.", ephemeral=True)


@tree.command(name="update", description="全登録プレイヤーの Bedwars stats を更新します（管理者用）")
@app_commands.choices(
    target=[
        app_commands.Choice(name="all", value="all"),
    ]
)
@admin_only()
async def update(interaction: discord.Interaction, target: app_commands.Choice[str]):
    await interaction.response.defer(ephemeral=True)

    try:
        if target.value != "all":
            await interaction.followup.send("❌ Only '/update all' is supported.", ephemeral=True)
            return
        success, failed = await update_all_players()
        ranking_results = await refresh_all_rankings()
        ranking_summary = ", ".join(
            f"{ranking_type}: posted={posted}"
            for ranking_type, (_, posted) in ranking_results.items()
        )
        await interaction.followup.send(
            f"✅ Update complete. Success: {success}, Failed: {failed}\nRanking refresh: {ranking_summary}",
            ephemeral=True,
        )
    except (VerificationError, PlayerDataFetchError) as error:
        await interaction.followup.send(error.message, ephemeral=True)
    except requests.RequestException:
        await interaction.followup.send(
            "⚠️ Network error while contacting Mojang/Flashlight services. Please try again.",
            ephemeral=True,
        )
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}", ephemeral=True)


@tasks.loop(time=DAILY_UPDATE_TIME)
async def daily_update_all_task():
    success, failed = await update_all_players()
    print(f"🕒 Daily update complete. Success: {success}, Failed: {failed}")
    ranking_results = await refresh_all_rankings()
    print(f"📊 Daily ranking refresh complete: {ranking_results}")


@daily_update_all_task.before_loop
async def before_daily_update_all_task():
    await bot.wait_until_ready()


@tree.command(name="stats", description="指定MCIDのBedwars統計を表示します")
async def stats(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer()

    try:
        uuid = resolve_target_uuid(mcid)
        if not uuid:
            await interaction.followup.send("❌ Invalid MCID / username not found.")
            return

        fetch_and_store_player_stats(uuid)
        row = get_player_stats_by_uuid(uuid)
        if not row:
            await interaction.followup.send("ℹ️ 統計データが見つかりませんでした。")
            return

        if not row["head_image_base64"]:
            head_image_base64 = _ensure_head_image_base64(uuid)
            if head_image_base64:
                fetch_and_store_player_stats(uuid)
                row = get_player_stats_by_uuid(uuid)
        if not row:
            await interaction.followup.send("ℹ️ 統計データが見つかりませんでした。")
            return

        row = _attach_resolved_tag(row)
        image_bytes = render_stats_image(row)
        filename = f"bedwars_stats_{_sanitize_filename(str(row['minecraft_name']))}.png"
        file = discord.File(image_bytes, filename=filename)
        await interaction.followup.send(file=file)
    except (VerificationError, PlayerDataFetchError) as error:
        await interaction.followup.send(error.message)
    except requests.RequestException:
        await interaction.followup.send(
            "⚠️ Network error while contacting Mojang/Flashlight services. Please try again.",
        )
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}")


@tree.command(name="ranking", description="保存済み Bedwars 統計からランキング画像を作成します")
@app_commands.describe(ranking_type="表示するランキング種別")
@app_commands.choices(
    ranking_type=[
        app_commands.Choice(name="FKDR", value="fkdr"),
        app_commands.Choice(name="Wins", value="wins"),
        app_commands.Choice(name="Star", value="star"),
        app_commands.Choice(name="BBLR", value="bblr"),
        app_commands.Choice(name="WLR", value="wlr"),
        app_commands.Choice(name="KDR", value="kdr"),
        app_commands.Choice(name="Final Kills", value="final_kills"),
        app_commands.Choice(name="Beds Broken", value="beds_broken"),
        app_commands.Choice(name="Winstreak", value="winstreak"),
    ]
)
async def ranking(interaction: discord.Interaction, ranking_type: app_commands.Choice[str]):
    await interaction.response.defer()

    try:
        rows = get_top_player_stats(ranking_type.value, limit=RANKING_MAX_PLAYERS)
        if not rows:
            await interaction.followup.send("ℹ️ ランキングに表示できるデータがありません。先に /update を実行してください。")
            return
        hydrated_rows = []
        for row in rows:
            head_image_base64 = row["head_image_base64"]
            if not row["head_image_base64"]:
                head_image_base64 = _ensure_head_image_base64(row["minecraft_uuid"])
                if head_image_base64:
                    fetch_and_store_player_stats(row["minecraft_uuid"])
            refreshed_row = get_player_stats_by_uuid(row["minecraft_uuid"])
            hydrated_rows.append(_attach_resolved_tag(refreshed_row or row))

        pages = _chunk_rows(hydrated_rows, RANKING_PAGE_SIZE)
        for page_idx, page_rows in enumerate(pages):
            image_bytes = render_ranking_image(
                page_rows,
                ranking_type.value,
                show_title=(page_idx == 0),
                rank_start=(page_idx * RANKING_PAGE_SIZE) + 1,
            )
            filename = (
                f"bedwars_{_sanitize_filename(ranking_type.value)}_ranking_"
                f"p{page_idx + 1}.png"
            )
            file = discord.File(image_bytes, filename=filename)
            await interaction.followup.send(file=file)
    except RankingRenderError:
        await interaction.followup.send("⚠️ ランキング画像の生成に失敗しました。")
    except PlayerDataFetchError as error:
        await interaction.followup.send(error.message)
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}")


@tree.command(name="rankingrefresh", description="ランキング投稿を手動更新します（管理者用）")
@admin_only()
async def rankingrefresh(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    try:
        results = await refresh_all_rankings()
        lines = []
        for ranking_type, (deleted_count, posted_count) in results.items():
            lines.append(f"{ranking_type}: deleted={deleted_count}, posted={posted_count}")
        await interaction.followup.send(
            "✅ Ranking refresh completed.\n" + "\n".join(lines),
            ephemeral=True,
        )
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}", ephemeral=True)


@tree.command(name="head", description="指定MCIDの最新ヘッド画像を表示します")
async def head(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer()
    try:
        profile = fetch_player_profile(mcid)
        if not profile:
            await interaction.followup.send("❌ Invalid MCID / username not found.")
            return

        head_image_base64 = fetch_head_base64_from_uuid(profile["uuid"])
        if not head_image_base64:
            await interaction.followup.send("❌ ヘッド画像の取得に失敗しました。")
            return

        update_player_head_image(profile["uuid"], head_image_base64)
        file = discord.File(
            io.BytesIO(base64.b64decode(head_image_base64)),
            filename=f"{_sanitize_filename(profile['name'])}_head.png",
        )
        await interaction.followup.send(file=file)
    except requests.RequestException:
        await interaction.followup.send(
            "⚠️ Network error while contacting Mojang services. Please try again.",
        )
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}")


@tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, AdminPermissionError):
        if interaction.response.is_done():
            await interaction.followup.send(str(error), ephemeral=True)
        else:
            await interaction.response.send_message(str(error), ephemeral=True)
        return
    raise error


keep_alive()
bot.run(TOKEN)
