import os
from threading import Thread
from typing import Optional

import discord
import requests
from discord import app_commands
from flask import Flask
from sqlite3 import IntegrityError

from database import (
    delete_player_registration_by_uuid,
    get_all_registered_players,
    get_cached_uuid,
    get_config_value,
    get_player_by_discord,
    get_player_by_uuid,
    init_db,
    register_verified_player,
    save_uuid_cache,
    set_config_value,
    upsert_player_stats,
)

REQUEST_TIMEOUT = 10
HYPIXEL_PLAYER_URL = "https://api.hypixel.net/v2/player"
MOJANG_PROFILE_URL = "https://api.mojang.com/users/profiles/minecraft/{mcid}"
SESSION_PROFILE_URL = "https://sessionserver.mojang.com/session/minecraft/profile/{uuid}"
AUTHORIZED_USERS = [1278574483195559977]

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


def fetch_and_store_player_stats(uuid: str):
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
    if not payload.get("success", False):
        raise VerificationError("❌ Hypixel API がエラーを返しました。")

    player = payload.get("player")
    if not player:
        raise VerificationError("❌ Hypixel にプレイヤーデータが見つかりませんでした。")

    achievements = player.get("achievements") or {}
    bedwars_stats = ((player.get("stats") or {}).get("Bedwars")) or {}

    minecraft_uuid = str(player.get("uuid") or uuid)
    minecraft_name = str(player.get("displayname") or fetch_current_name(uuid) or uuid)
    bedwars_star = _safe_int(achievements.get("bedwars_level"))
    wins = _safe_int(bedwars_stats.get("wins_bedwars"))
    losses = _safe_int(bedwars_stats.get("losses_bedwars"))
    final_kills = _safe_int(bedwars_stats.get("final_kills_bedwars"))
    final_deaths = _safe_int(bedwars_stats.get("final_deaths_bedwars"))
    beds_broken = _safe_int(bedwars_stats.get("beds_broken_bedwars"))
    beds_lost = _safe_int(bedwars_stats.get("beds_lost_bedwars"))
    kills = _safe_int(bedwars_stats.get("kills_bedwars"))
    deaths = _safe_int(bedwars_stats.get("deaths_bedwars"))
    games_played = _safe_int(bedwars_stats.get("games_played_bedwars"))
    winstreak = _safe_int(bedwars_stats.get("winstreak"))

    upsert_player_stats(
        minecraft_uuid=minecraft_uuid,
        minecraft_name=minecraft_name,
        bedwars_star=bedwars_star,
        wins=wins,
        losses=losses,
        final_kills=final_kills,
        final_deaths=final_deaths,
        beds_broken=beds_broken,
        beds_lost=beds_lost,
        kills=kills,
        deaths=deaths,
        games_played=games_played,
        winstreak=winstreak,
        fkdr=_safe_ratio(final_kills, final_deaths),
        wlr=_safe_ratio(wins, losses),
        kdr=_safe_ratio(kills, deaths),
    )


def resolve_target_uuid(mcid_or_uuid: str) -> Optional[str]:
    value = mcid_or_uuid.strip()
    if not value:
        return None

    compact = value.replace("-", "").lower()
    if len(compact) == 32 and all(ch in "0123456789abcdef" for ch in compact):
        return compact

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


def is_admin(user_id: int) -> bool:
    return user_id in AUTHORIZED_USERS


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


@bot.event
async def on_ready():
    init_db()
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
async def lookup(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ You do not have permission to use this command.", ephemeral=True)
        return

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
async def resetverify(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ You do not have permission to use this command.", ephemeral=True)
        return

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
async def add(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ You do not have permission to use this command.", ephemeral=True)
        return

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


@tree.command(name="updateapi", description="Hypixel API キーを更新します（管理者用）")
async def updateapi(interaction: discord.Interaction, api_key: str):
    await interaction.response.defer(ephemeral=True)
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ You do not have permission to use this command.", ephemeral=True)
        return

    cleaned_key = api_key.strip()
    if not cleaned_key:
        await interaction.followup.send("❌ API key cannot be empty.", ephemeral=True)
        return

    global current_api_key
    current_api_key = cleaned_key
    set_config_value("hypixel_api_key", cleaned_key)
    await interaction.followup.send("Hypixel API key updated successfully.", ephemeral=True)


@tree.command(name="update", description="Hypixel Bedwars stats を更新します（管理者用）")
async def update(interaction: discord.Interaction, mcid_or_all: str):
    await interaction.response.defer(ephemeral=True)
    if not is_admin(interaction.user.id):
        await interaction.followup.send("❌ You do not have permission to use this command.", ephemeral=True)
        return

    target = mcid_or_all.strip()
    if not target:
        await interaction.followup.send("❌ Please provide a target (all, MC username, or UUID).", ephemeral=True)
        return

    try:
        if target.lower() == "all":
            players = get_all_registered_players()
            success = 0
            failed = 0
            for player in players:
                try:
                    fetch_and_store_player_stats(player["minecraft_uuid"])
                    success += 1
                except Exception:
                    failed += 1

            await interaction.followup.send(
                f"Update complete. Success: {success}, Failed: {failed}",
                ephemeral=True,
            )
            return

        uuid = resolve_target_uuid(target)
        if not uuid:
            await interaction.followup.send("❌ Invalid MCID / username not found.", ephemeral=True)
            return

        fetch_and_store_player_stats(uuid)
        await interaction.followup.send("✅ Player stats updated successfully.", ephemeral=True)
    except VerificationError as error:
        await interaction.followup.send(error.message, ephemeral=True)
    except requests.RequestException:
        await interaction.followup.send(
            "⚠️ Network error while contacting Mojang/Hypixel services. Please try again.",
            ephemeral=True,
        )
    except Exception as error:
        await interaction.followup.send(f"⚠️ エラーが発生しました: {error}", ephemeral=True)


keep_alive()
bot.run(TOKEN)
