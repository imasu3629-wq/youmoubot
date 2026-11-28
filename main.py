import discord
from discord import app_commands
from discord.ext import tasks
import json
import requests
import asyncio
import time
import datetime
import functools
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup
import os
from pathlib import Path
import io
from PIL import Image, ImageDraw, ImageFont
from flask import Flask
from threading import Thread
from database import (
    init_db, get_cached_uuid, save_uuid_cache,
    register_player, update_stats, is_registered, is_registered_by_discord,
    get_registered_by_discord, get_ranking_by_fkdr, get_ranking_by_star,
    delete_player, get_all_registrations, get_ranking, current_month
)
from bedwars import MODES, LEADERBOARDS, COMPARE_FIELDS, extract_bw_stats, find_leaderboard
from cards import build_compare_card, build_leaderboard_card, fetch_head
from bwcard_bot import CardView, SettingsView, load_card_data, pick_background
import ai
import tickets
import verify

# --- 24時間稼働設定 ---
app = Flask('')
@app.route('/')
def home(): return "Bot is alive!"

def run():
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)

def keep_alive():
    t = Thread(target=run)
    t.start()

# --- 環境変数 ---
TOKEN = os.environ['DISCORD_TOKEN']
current_api_key = os.environ.get('HYPIXEL_KEY', '')
AUTHORIZED_USERS = [1278574483195559977]


def message_content_allowed() -> bool:
    """Developer Portal で Message Content Intent がオンか（オフのまま要求すると接続できない）。"""
    for attempt in range(3):
        try:
            res = requests.get("https://discord.com/api/v10/applications/@me", headers={"Authorization": f"Bot {TOKEN}"}, timeout=10)
            res.raise_for_status()
            return bool(res.json().get("flags", 0) & (1 << 18 | 1 << 19))
        except (requests.RequestException, ValueError) as e:
            print(f"⚠️ Intent の確認に失敗 ({attempt + 1}/3): {e}")
            time.sleep(5)
    raise SystemExit("Discord API に接続できないので終了します（systemd が再起動します）")


intents = discord.Intents.default()
intents.message_content = message_content_allowed()
if not intents.message_content:
    print("⚠️ Message Content Intent がオフなので、AIはメンションされたときだけ返事します")
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


# --- UUID取得ヘルパー（キャッシュ付き） ---
def fetch_current_name(uuid: str):
    res = requests.get(f"https://sessionserver.mojang.com/session/minecraft/profile/{uuid}")
    if res.status_code != 200:
        return None
    return res.json().get("name")


def fetch_player_profile(mcid: str):
    normalized_mcid = mcid.strip()
    if not normalized_mcid:
        return None

    cached_uuid = get_cached_uuid(normalized_mcid)
    if cached_uuid:
        official_name = fetch_current_name(cached_uuid)
        return {"uuid": cached_uuid, "name": official_name or normalized_mcid}

    res = requests.get(f"https://api.mojang.com/users/profiles/minecraft/{normalized_mcid}")
    if res.status_code != 200:
        return None

    data = res.json()
    uuid = data.get("id")
    official_name = data.get("name")
    if not uuid:
        return None

    save_uuid_cache(normalized_mcid, uuid)
    if official_name and official_name.lower() != normalized_mcid.lower():
        save_uuid_cache(official_name, uuid)

    return {"uuid": uuid, "name": official_name or normalized_mcid}


def fetch_uuid(mcid: str):
    profile = fetch_player_profile(mcid)
    return profile["uuid"] if profile else None


# --- Hypixel ランク取得ヘルパー ---
def get_rank(player_data):
    if not player_data:
        return ""
    # 1. スタッフランク
    rank = player_data.get("rank")
    if rank and rank != "NORMAL":
        return f"[{rank}]"
    # 2. MVP++ (monthlyPackageRank)
    monthly_rank = player_data.get("monthlyPackageRank")
    if monthly_rank and monthly_rank != "NONE":
        return "[MVP++]" if monthly_rank == "SUPERSTAR" else f"[{monthly_rank}]"
    # 3. 一般寄付ランク (newPackageRank)
    package_rank = player_data.get("newPackageRank")
    if package_rank and package_rank != "NONE":
        label = package_rank.replace("_PLUS", "+")
        return f"[{label}]"
    return ""


# --- Hypixel stats取得ヘルパー ---
AURORA_BASE = "https://api.bordic.xyz"


def aurora_get(path: str, **params):
    # API-Key: ws はAurora推定の総合winstreakを返させる指定（認証キーではない）
    res = requests.get(
        f"{AURORA_BASE}/{path}",
        params=params,
        headers={"API-Key": "ws"},
        timeout=10,
    )
    return res.json()


def fetch_hypixel_player(uuid: str):
    return aurora_get("v3/cache/hypixel", uuid=uuid).get("player")


def fetch_hypixel_stats(uuid: str):
    p = fetch_hypixel_player(uuid)
    if not p:
        return None, None, None
    bw = (p.get("stats") or {}).get("Bedwars") or {}
    star = (p.get("achievements") or {}).get("bedwars_level") or 0
    fk = bw.get("final_kills_bedwars") or 0
    fd = bw.get("final_deaths_bedwars") or 1
    fkdr = round(fk / max(fd, 1), 2)
    rank_label = get_rank(p)
    return star, fkdr, rank_label


def fetch_ranking_stats(uuid: str):
    """ランキング用の (star, fkdr, wins, final_kills, final_deaths)。データが無いときは None。"""
    p = fetch_hypixel_player(uuid)
    if not p:
        return None
    s = extract_bw_stats(p)
    return s["star"], s["fkdr"], s["wins"], s["final_kills"], s["final_deaths"]


# --- ランキング画像生成 ---
FONT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Minecraftia.ttf")

def get_rank_color(fkdr: float):
    if fkdr >= 2000: return (255, 85, 85)    # 赤
    if fkdr >= 1000: return (255, 85, 255)   # ピンク
    if fkdr >= 100:  return (255, 170, 0)    # 金
    if fkdr >= 10:   return (85, 255, 85)    # 緑
    if fkdr >= 4:    return (85, 255, 255)   # 水色
    if fkdr >= 2:    return (255, 255, 85)   # 黄
    return (170, 170, 170)                   # グレー

def build_ranking_image(rows, mode: str) -> io.BytesIO:
    W, ROW_H, PADDING = 700, 52, 16
    H = PADDING * 2 + ROW_H * max(len(rows), 1) + 60
    img = Image.new("RGBA", (W, H), (15, 15, 20, 255))
    draw = ImageDraw.Draw(img)

    # フォント
    try:
        font_title = ImageFont.truetype(FONT_PATH, 22)
        font_main  = ImageFont.truetype(FONT_PATH, 18)
        font_small = ImageFont.truetype(FONT_PATH, 14)
    except Exception:
        font_title = font_main = font_small = ImageFont.load_default()

    # タイトル
    title = "FKDR Ranking TOP10" if mode == "fkdr" else "Star Ranking TOP10"
    draw.text((PADDING, PADDING), title, font=font_title, fill=(255, 215, 0))

    medals = ["1st", "2nd", "3rd"]
    y = PADDING + 48

    for i, r in enumerate(rows):
        # 行背景（交互）
        bg = (25, 25, 35) if i % 2 == 0 else (20, 20, 28)
        draw.rectangle([(0, y - 6), (W, y + ROW_H - 10)], fill=bg)

        # 順位
        rank_text = medals[i] if i < 3 else f"{i+1}."
        rank_color = [(255, 215, 0), (192, 192, 192), (205, 127, 50)][i] if i < 3 else (150, 150, 150)
        draw.text((PADDING, y), rank_text, font=font_small, fill=rank_color)

        # MCID
        draw.text((80, y), r["mcid"], font=font_main, fill=(255, 255, 255))

        # スコア（右側）
        if mode == "fkdr":
            score_text = f"FKDR: {r['fkdr']}"
            color = get_rank_color(r["fkdr"])
        else:
            score_text = f"Star: {r['star']}"
            color = (255, 215, 0)
        draw.text((W - 200, y), score_text, font=font_main, fill=color)

        y += ROW_H

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf


# --- ランキング選択View（最初に表示） ---
class RankingSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label="⚔️ FKDR順", style=discord.ButtonStyle.primary)
    async def fkdr_ranking(self, interaction: discord.Interaction, button: discord.ui.Button):
        rows = get_ranking_by_fkdr()
        buf = build_ranking_image(rows, "fkdr")
        await interaction.response.edit_message(content=None, attachments=[discord.File(buf, filename="ranking.png")], view=RankingBackView())

    @discord.ui.button(label="⭐ スター順", style=discord.ButtonStyle.secondary)
    async def star_ranking(self, interaction: discord.Interaction, button: discord.ui.Button):
        rows = get_ranking_by_star()
        buf = build_ranking_image(rows, "star")
        await interaction.response.edit_message(content=None, attachments=[discord.File(buf, filename="ranking.png")], view=RankingBackView())


# --- ランキング表示後の戻るView ---
class RankingBackView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label="↩️ 選択に戻る", style=discord.ButtonStyle.danger)
    async def back(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(
            content="📊 どちらのランキングを表示しますか？",
            embed=None,
            attachments=[],
            view=RankingSelectView()
        )


def build_ranking_embed(rows, mode: str):
    medals = ["🥇", "🥈", "🥉"]
    if mode == "fkdr":
        title = "⚔️ FKDR ランキング TOP10"
        color = 0xe74c3c
        lines = [
            f"{medals[i] if i < 3 else f'`{i+1}.`'} **{r['mcid']}** — FKDR: `{r['fkdr']}`"
            for i, r in enumerate(rows)
        ]
    else:
        title = "⭐ スター ランキング TOP10"
        color = 0xf1c40f
        lines = [
            f"{medals[i] if i < 3 else f'`{i+1}.`'} **{r['mcid']}** — ⭐{r['star']}"
            for i, r in enumerate(rows)
        ]
    embed = discord.Embed(
        title=title,
        description="\n".join(lines) if lines else "データがありません",
        color=color
    )
    embed.set_footer(text="↩️ 選択に戻るで切り替えできます")
    return embed


# ====================
# コマンド
# ====================

@bot.event
async def on_ready():
    init_db()
    bot.add_view(verify.VerifyPanelView())
    bot.add_view(tickets.TicketPanelView())
    bot.add_view(tickets.TicketCloseView())
    if not update_ranking_boards.is_running():
        update_ranking_boards.start()
        bot.loop.create_task(post_new_boards())
    try:
        synced = await tree.sync()
        print(f"✅ {len(synced)}個のコマンドを同期しました")
    except Exception as e:
        print(f"❌ 同期エラー: {e}")
    print(f'✅ Logged in as {bot.user.name}')


# --- /register ---
@tree.command(name="register", description="MCIDを登録してBedwars戦績を記録します（管理者用）")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def register(interaction: discord.Interaction, mcid: str):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ このコマンドは管理者だけが使えます。MCID申請チャンネルから申請してください。", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    await do_register(interaction, mcid)


async def register_mcid(mcid: str, discord_id: int):
    """MCIDを登録する。成功なら登録完了の Embed、失敗ならエラー文を返す。"""
    profile = await asyncio.to_thread(fetch_player_profile, mcid)
    if not profile:
        return "❌ プレイヤーが見つかりません。MCIDを確認してください。"
    uuid = profile["uuid"]
    official_mcid = profile["name"]
    if is_registered(uuid):
        return f"⚠️ `{official_mcid}` はすでに登録されています。"
    s = await asyncio.to_thread(fetch_ranking_stats, uuid)
    if s is None:
        return "❌ Hypixelからデータを取得できませんでした。"
    star, fkdr = s[:2]
    if not register_player(uuid, official_mcid, discord_id, *s):
        return f"⚠️ `{official_mcid}` はすでに登録されています。"
    embed = discord.Embed(title="✅ 登録完了", color=0x2ecc71)
    embed.add_field(name="MCID", value=official_mcid, inline=True)
    embed.add_field(name="⭐ Star", value=str(star), inline=True)
    embed.add_field(name="⚔️ FKDR", value=str(fkdr), inline=True)
    embed.set_footer(text="ランキングに反映されました")
    return embed


async def do_register(interaction: discord.Interaction, mcid: str):
    try:
        result = await register_mcid(mcid, interaction.user.id)
    except Exception as e:
        result = f"⚠️ エラーが発生しました: {e}"
    if isinstance(result, discord.Embed):
        await interaction.followup.send(embed=result, ephemeral=True)
    else:
        await interaction.followup.send(result, ephemeral=True)


# --- MCID申請チャンネル（管理者が ✅ を付けたら登録） ---
APPROVE_EMOJI = "✅"
REGISTERED_EMOJI = "🎉"
approval_channels = ai.Channels(Path(os.path.abspath(__file__)).with_name("approval_channels.json"))


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if (str(payload.emoji) != APPROVE_EMOJI or payload.channel_id not in approval_channels.ids
            or not payload.member or payload.member.bot
            or not payload.member.guild_permissions.administrator):
        return
    channel = bot.get_channel(payload.channel_id) or await bot.fetch_channel(payload.channel_id)
    try:
        message = await channel.fetch_message(payload.message_id)
    except discord.HTTPException:
        return
    words = message.content.split()
    if message.author.bot or not words:
        return
    if message.edited_at:
        result = "⚠️ 編集されたメッセージは登録できません。MCIDを送り直してください。"
    else:
        try:
            result = await register_mcid(words[0], message.author.id)
        except Exception as e:
            result = f"⚠️ エラーが発生しました: {e}"
    ok = isinstance(result, discord.Embed)
    try:
        await message.add_reaction(REGISTERED_EMOJI if ok else "❌")
    except discord.HTTPException:
        if ok:
            await message.reply(embed=result, mention_author=False)
            return
    if not ok:
        await message.reply(result, mention_author=False)


@tree.command(name="approvalchannel", description="このチャンネルをMCID申請チャンネルにするかを切り替えます")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def approvalchannel(interaction: discord.Interaction):
    on = approval_channels.toggle(interaction.channel_id)
    msg = (f"✅ このチャンネルをMCID申請チャンネルにしました。送られたMCIDに管理者が {APPROVE_EMOJI} を付けると登録します。"
           if on else "⏹️ このチャンネルのMCID申請を止めました。")
    if on and not intents.message_content:
        msg += "\n⚠️ Message Content Intent が無効なので、送られたMCIDを読めません。Developer Portalで有効にしてください。"
    await interaction.response.send_message(msg, ephemeral=True)


# --- /registered ---
@tree.command(name="registered", description="自分が登録したMCID一覧を表示します")
async def registered(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        rows = get_registered_by_discord(interaction.user.id)
        if not rows:
            await interaction.followup.send("📭 登録されたMCIDはありません。`/register [MCID]` で登録できます。", ephemeral=True)
            return

        embed = discord.Embed(title="📋 登録済みMCID一覧", color=0x3498db)
        lines = [
            f"**{r['mcid']}** — ⭐{r['star']} FKDR:`{r['fkdr']}` （更新: {r['updated_at'][:10]}）"
            for r in rows
        ]
        embed.description = "\n".join(lines)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"⚠️ エラー: {e}", ephemeral=True)


# --- /refresh ---
@tree.command(name="refresh", description="登録済みMCIDの戦績を最新データに更新します")
async def refresh(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)
    try:
        profile = fetch_player_profile(mcid)
        if not profile:
            await interaction.followup.send("❌ プレイヤーが見つかりません。", ephemeral=True)
            return

        uuid = profile["uuid"]
        official_mcid = profile["name"]

        if not is_registered_by_discord(interaction.user.id, uuid):
            await interaction.followup.send("❌ このMCIDはあなたが登録したものではありません。", ephemeral=True)
            return

        s = await asyncio.to_thread(fetch_ranking_stats, uuid)
        if s is None:
            await interaction.followup.send("❌ Hypixelからデータを取得できませんでした。", ephemeral=True)
            return
        star, fkdr = s[:2]

        update_stats(uuid, official_mcid, *s)

        embed = discord.Embed(title="🔄 戦績を更新しました", color=0x9b59b6)
        embed.add_field(name="MCID", value=official_mcid, inline=True)
        embed.add_field(name="⭐ Star", value=str(star), inline=True)
        embed.add_field(name="⚔️ FKDR", value=str(fkdr), inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"⚠️ エラー: {e}", ephemeral=True)


# --- /ranking ---
@tree.command(name="ranking", description="BedwarsランキングをFKDR順またはスター順で表示します")
async def ranking(interaction: discord.Interaction):
    await interaction.response.send_message(
        content="📊 どちらのランキングを表示しますか？",
        view=RankingSelectView()
    )


# --- /stats（既存・UUID対応に改良） ---
def fkdr_comment(fkdr: float) -> str:
    if fkdr >= 2000:
        return "YAJUandU114514!!"
    elif fkdr >= 1000:
        return "強すぎィィィいくくぅぅ"
    elif fkdr >= 100:
        return "強いねぇぇ(泣)"
    elif fkdr >= 10:
        return "なかなかやる"
    elif fkdr >= 4:
        return "割と強い"
    elif fkdr >= 2:
        return "普通レベル"
    else:
        return "初心者だろう"


@tree.command(name="stats", description="Hypixelの戦績を表示します")
async def stats(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer()
    try:
        uuid = fetch_uuid(mcid)
        if not uuid:
            await interaction.followup.send("❌ プレイヤーが見つかりません。")
            return
        star, fkdr, rank_label = fetch_hypixel_stats(uuid)
        if star is None:
            await interaction.followup.send("❌ Hypixelにデータがありません。")
            return
        comment = fkdr_comment(fkdr)
        if mcid.lower() == "haru_12m":
            comment = "ハルはdefやろ"
        if mcid.lower() == "youmouop":
            rank_label = "[YOUMOU]"
        display_name = f"{rank_label} {mcid}" if rank_label else mcid
        embed = discord.Embed(title=f"{display_name} の戦績", color=0x00ff00)
        embed.add_field(name="⭐ Star", value=str(star), inline=True)
        embed.add_field(name="⚔️ FKDR", value=f"{fkdr}  |  {comment}", inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"⚠️ エラー: {e}")


# --- /skin（既存） ---
@tree.command(name="skin", description="指定したMCIDのスキン画像を表示します")
async def skin(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer()
    try:
        uuid = fetch_uuid(mcid)
        if not uuid:
            await interaction.followup.send("❌ プレイヤーが見つかりません。")
            return
        render_url = f"https://visage.surgeplay.com/full/384/{uuid}.png"
        raw_skin_url = f"https://visage.surgeplay.com/skin/{uuid}.png"
        embed = discord.Embed(title=f"👕 {mcid} のスキン", color=0x9b59b6)
        embed.set_image(url=render_url)
        embed.add_field(name="📥 配布用データ", value=f"[この画像を保存して適用]({raw_skin_url})", inline=False)
        await interaction.followup.send(embed=embed)
    except:
        await interaction.followup.send("⚠️ スキン取得エラー")


# --- NameMC スクレイピングヘルパー ---
def fetch_namemc_history(uuid: str):
    url = f"https://namemc.com/profile/{uuid}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        return []
    soup = BeautifulSoup(res.text, "html.parser")
    rows = soup.select("table#name-history tbody tr")
    history = []
    for row in rows:
        cols = row.select("td")
        if len(cols) >= 1:
            name = cols[0].get_text(strip=True)
            date = cols[1].get_text(strip=True) if len(cols) > 1 else "最初のID"
            history.append({"username": name, "changed_at": date})
    return history


# --- /history（NameMC版） ---
@tree.command(name="history", description="MCIDの変更履歴を表示します")
async def history(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer()
    try:
        uuid = fetch_uuid(mcid)
        if not uuid:
            await interaction.followup.send("❌ プレイヤーが見つかりません。")
            return
        history_data = fetch_namemc_history(uuid)
        if history_data:
            embed = discord.Embed(title=f"📜 {mcid} のID変更履歴", color=0x3498db)
            lines = []
            for entry in history_data:
                name = entry["username"]
                date = entry["changed_at"]
                if date and date != "最初のID":
                    lines.append(f"📅 `{date}` ➔ **{name}**")
                else:
                    lines.append(f"🌱 `最初のID` ➔ **{name}**")
            embed.description = "\n".join(lines)
            embed.set_footer(text="Data provided by NameMC")
            await interaction.followup.send(embed=embed)
        else:
            await interaction.followup.send(f"ℹ️ {mcid} の履歴データが見つかりませんでした。")
    except Exception as e:
        await interaction.followup.send("⚠️ 履歴取得中にエラーが発生しました。")

# --- /delete ---
@tree.command(name="delete", description="登録したMCIDをランキングから削除します")
async def delete(interaction: discord.Interaction, mcid: str):
    await interaction.response.defer(ephemeral=True)
    try:
        uuid = fetch_uuid(mcid)
        if not uuid:
            await interaction.followup.send("❌ プレイヤーが見つかりません。", ephemeral=True)
            return
        if not is_registered_by_discord(interaction.user.id, uuid):
            await interaction.followup.send("❌ このMCIDはあなたが登録したものではありません。", ephemeral=True)
            return
        success = delete_player(interaction.user.id, uuid)
        if success:
            await interaction.followup.send(f"🗑️ `{mcid}` をランキングから削除しました。", ephemeral=True)
        else:
            await interaction.followup.send("❌ 削除に失敗しました。", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"⚠️ エラー: {e}", ephemeral=True)


# ====================
# Bedwars 詳細コマンド
# ====================

MODE_CHOICES = [app_commands.Choice(name=label, value=key) for key, (label, _) in MODES.items()]
CARD_MODE_LABELS = {"overall": "Overall", "solo": "Solo", "doubles": "Doubles", "threes": "3v3v3v3", "fours": "4v4v4v4", "4v4": "4v4"}
LEADERBOARD_CHOICES = [app_commands.Choice(name=label, value=key) for key, (label, _, _) in LEADERBOARDS.items()]


def fetch_player_with_stats(mcid: str, mode: str):
    profile = fetch_player_profile(mcid)
    if not profile:
        return None, None, "❌ プレイヤーが見つかりません。"
    player = fetch_hypixel_player(profile["uuid"])
    if not player:
        return profile, None, f"❌ `{profile['name']}` のHypixelデータがありません。"
    return profile, player, None


ai.lookup_player = fetch_player_with_stats


# --- /bw ---
@tree.command(name="bw", description="Bedwarsの詳細戦績を表示します")
@app_commands.describe(mcid="MCID", mode="モード（省略時は総合）")
@app_commands.choices(mode=MODE_CHOICES)
async def bw(interaction: discord.Interaction, mcid: str, mode: app_commands.Choice[str] = None):
    await interaction.response.defer()
    mode_key = mode.value if mode else "overall"
    try:
        profile, player, error = await asyncio.to_thread(fetch_player_with_stats, mcid, mode_key)
        if error:
            await interaction.followup.send(error)
            return
        data = await asyncio.to_thread(load_card_data, player, profile["uuid"])
        view = CardView(interaction.user.id, data, mode_key)
        view.message = await interaction.followup.send(file=await view.render(), view=view, wait=True)
    except Exception as e:
        await interaction.followup.send(f"⚠️ エラー: {e}")


@tree.command(name="config", description="/bw のカードに表示する項目を設定します")
async def config(interaction: discord.Interaction):
    sv = SettingsView(interaction.user.id)
    await interaction.response.send_message(embed=sv.embed(), view=sv, ephemeral=True)


# --- /compare ---
@tree.command(name="compare", description="2人のBedwars戦績を比較します")
@app_commands.describe(mcid1="1人目のMCID", mcid2="2人目のMCID", mode="モード（省略時は総合）")
@app_commands.choices(mode=MODE_CHOICES)
async def compare(interaction: discord.Interaction, mcid1: str, mcid2: str, mode: app_commands.Choice[str] = None):
    await interaction.response.defer()
    mode_key = mode.value if mode else "overall"
    try:
        (p1, pl1, e1), (p2, pl2, e2) = await asyncio.gather(
            asyncio.to_thread(fetch_player_with_stats, mcid1, mode_key),
            asyncio.to_thread(fetch_player_with_stats, mcid2, mode_key),
        )
        if e1 or e2:
            await interaction.followup.send(e1 or e2)
            return
        heads = await asyncio.gather(
            asyncio.to_thread(fetch_head, p1["uuid"]),
            asyncio.to_thread(fetch_head, p2["uuid"]),
        )
        players = [
            (p1["name"], get_rank(pl1), extract_bw_stats(pl1, mode_key)),
            (p2["name"], get_rank(pl2), extract_bw_stats(pl2, mode_key)),
        ]
        fields = [(label.split(" ", 1)[1], key) for label, key in COMPARE_FIELDS]
        buf = await asyncio.to_thread(build_compare_card, players, fields, CARD_MODE_LABELS[mode_key], heads)
        alt = f"{p1['name']} vs {p2['name']} ({CARD_MODE_LABELS[mode_key]}): " + ", ".join(
            f"{label} {players[0][2][key]} / {players[1][2][key]}"
            for label, key in fields if players[0][2][key] is not None
        )
        await interaction.followup.send(file=discord.File(buf, filename="compare.png", description=alt[:1000]))
    except Exception as e:
        await interaction.followup.send(f"⚠️ エラー: {e}")


# --- /leaderboard ---
LEADERBOARD_CACHE_SECONDS = 600
_leaderboard_cache = {"time": 0, "data": None}
_name_cache = {}


def fetch_leaderboards():
    if _leaderboard_cache["data"] and time.time() - _leaderboard_cache["time"] < LEADERBOARD_CACHE_SECONDS:
        return _leaderboard_cache["data"]
    res = aurora_get("v3/cache/hypixel/leaderboards")
    if not res.get("success"):
        raise RuntimeError(res.get("cause", "リーダーボード取得失敗"))
    _leaderboard_cache["data"] = res["leaderboards"]
    _leaderboard_cache["time"] = time.time()
    return _leaderboard_cache["data"]


def cached_name(uuid: str):
    uuid = uuid.replace("-", "")
    if uuid not in _name_cache:
        name = fetch_current_name(uuid)
        if not name:
            return uuid[:8]
        _name_cache[uuid] = name
    return _name_cache[uuid]


@tree.command(name="leaderboard", description="Hypixel公式のBedwarsリーダーボードを表示します")
@app_commands.describe(category="ランキングの種類", count="表示人数（1〜25、省略時は10）")
@app_commands.choices(category=LEADERBOARD_CHOICES)
async def leaderboard(interaction: discord.Interaction, category: app_commands.Choice[str], count: app_commands.Range[int, 1, 25] = 10):
    await interaction.response.defer()
    try:
        data = await asyncio.to_thread(fetch_leaderboards)
        lb = find_leaderboard(data, category.value)
        if not lb:
            await interaction.followup.send("❌ このリーダーボードは現在取得できません。")
            return
        leaders = lb.get("leaders", [])[:count]
        names, heads = await asyncio.gather(
            asyncio.gather(*(asyncio.to_thread(cached_name, u) for u in leaders)),
            asyncio.gather(*(asyncio.to_thread(fetch_head, u, 64) for u in leaders)),
        )
        _, keyword, period = LEADERBOARDS[category.value]
        title = f"{keyword.title()}  •  {period.title()}"
        buf = await asyncio.to_thread(build_leaderboard_card, title, list(names), list(heads))
        alt = f"Bedwars leaderboard {title}: " + ", ".join(f"{i + 1}. {n}" for i, n in enumerate(names))
        await interaction.followup.send(file=discord.File(buf, filename="leaderboard.png", description=alt[:1000]))
    except Exception as e:
        await interaction.followup.send(f"⚠️ エラー: {e}")


# --- 自動更新ランキング ---
RANKING_BOARDS = {
    "fkdr": ("FKDR", lambda r: f"{r['fkdr']:.2f}"),
    "star": ("Star", lambda r: f"{r['star']:,}"),
    "wins": ("Wins", lambda r: f"{r['wins']:,}"),
    "final_kills": ("Final Kills", lambda r: f"{r['final_kills']:,}"),
    "m_fkdr": ("Monthly FKDR", lambda r: f"{r['value']:.2f}"),
    "m_star": ("Monthly Star", lambda r: f"+{r['value']:,}"),
    "m_wins": ("Monthly Wins", lambda r: f"+{r['value']:,}"),
    "m_final_kills": ("Monthly Final Kills", lambda r: f"+{r['value']:,}"),
}
BOARDS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ranking_channels.json")
boards_lock = asyncio.Lock()


def load_boards() -> dict:
    """{"サーバーID:種類": {"kind": 種類, "channel": チャンネルID, "message": 投稿ID}}"""
    try:
        with open(BOARDS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def save_boards(boards: dict):
    with open(BOARDS_FILE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(boards, f)
    os.replace(BOARDS_FILE + ".tmp", BOARDS_FILE)


REFRESH_WORKERS = 8
BOARD_SIZE, BOARD_PAGE = 100, 25


def refresh_one(r):
    try:
        s = fetch_ranking_stats(r["uuid"])
        if s:
            update_stats(r["uuid"], fetch_current_name(r["uuid"]) or r["mcid"], *s)
    except Exception as e:
        print(f"⚠️ {r['mcid']} の更新に失敗: {e}")


def refresh_all_registrations():
    with ThreadPoolExecutor(REFRESH_WORKERS) as pool:
        list(pool.map(refresh_one, get_all_registrations()))


@functools.lru_cache(maxsize=1024)
def cached_head(uuid: str):
    return fetch_head(uuid, 64)


def build_board_files(kind: str) -> list[discord.File]:
    """上位100人を25人ずつの画像に分ける。"""
    label, fmt = RANKING_BOARDS[kind]
    rows = get_ranking(kind, BOARD_SIZE)
    with ThreadPoolExecutor(REFRESH_WORKERS) as pool:
        heads = list(pool.map(cached_head, [r["uuid"] for r in rows]))
    background = pick_background()
    files = []
    for start in range(0, max(len(rows), 1), BOARD_PAGE):
        page = rows[start:start + BOARD_PAGE]
        names = [r["mcid"] for r in page]
        title = f"{label} Ranking" + (f"  #{start + 1}-{start + len(page)}" if len(rows) > BOARD_PAGE else "")
        buf = build_leaderboard_card(title, names, heads[start:start + BOARD_PAGE], [fmt(r) for r in page], start=start, cols=1, background=background)
        alt = f"{label} ランキング: " + ", ".join(f"{start + i + 1}. {n} {fmt(r)}" for i, (n, r) in enumerate(zip(names, page)))
        files.append(discord.File(buf, filename=f"ranking_{kind}_{start // BOARD_PAGE + 1}.png", description=alt[:1000]))
    return files


async def post_board(entry: dict) -> bool:
    """ランキングを投稿、または前の投稿を書き換える。投稿IDが変わったら True。"""
    kind = entry["kind"]
    channel = bot.get_channel(entry["channel"]) or await bot.fetch_channel(entry["channel"])
    files = await asyncio.to_thread(build_board_files, kind)
    content = f"🕐 最終更新: <t:{int(time.time())}:R>（毎日0時・日本時間に自動更新）"
    if kind.startswith("m_"):
        content += f"\n📅 {current_month()} の記録（毎月1日にリセット）"
    if entry.get("message"):
        try:
            await channel.get_partial_message(entry["message"]).edit(content=content, attachments=files)
            return False
        except discord.NotFound:
            files = await asyncio.to_thread(build_board_files, kind)
    entry["message"] = (await channel.send(content, files=files)).id
    return True


JST = datetime.timezone(datetime.timedelta(hours=9), "JST")
update_lock = asyncio.Lock()


@tasks.loop(time=datetime.time(0, 0, tzinfo=JST))
async def update_ranking_boards():
    async with update_lock:
        await run_ranking_update()


async def post_new_boards():
    """まだ投稿していないランキング（設定ファイルに直接追加したもの）を投稿する。"""
    async with boards_lock:
        boards = load_boards()
        changed = False
        for key, entry in boards.items():
            if not entry.get("message"):
                try:
                    changed |= await post_board(entry)
                except Exception as e:
                    print(f"⚠️ {key} ランキングの投稿に失敗: {e}")
        if changed:
            save_boards(boards)


async def run_ranking_update(guild_id: int | None = None):
    """登録者の戦績を取り直し、ランキング投稿を更新する。guild_id を渡すとそのサーバーの投稿だけ。"""
    if guild_id is None and not load_boards():
        return
    cached_head.cache_clear()
    await asyncio.to_thread(refresh_all_registrations)
    async with boards_lock:
        boards = load_boards()
        changed = False
        for key, entry in boards.items():
            if guild_id is not None and not key.startswith(f"{guild_id}:"):
                continue
            try:
                changed |= await post_board(entry)
            except Exception as e:
                print(f"⚠️ {key} ランキングの投稿に失敗: {e}")
        if changed:
            save_boards(boards)


@tree.command(name="rankingchannel", description="このチャンネルに自動更新ランキングを投稿します")
@app_commands.describe(kind="ランキングの種類")
@app_commands.choices(kind=[app_commands.Choice(name=label, value=k) for k, (label, _) in RANKING_BOARDS.items()])
@app_commands.default_permissions(manage_channels=True)
@app_commands.guild_only()
async def rankingchannel(interaction: discord.Interaction, kind: app_commands.Choice[str]):
    await interaction.response.defer(ephemeral=True)
    key = f"{interaction.guild_id}:{kind.value}"
    async with boards_lock:
        boards = load_boards()
        old = boards.get(key) or {}
        entry = {"kind": kind.value, "channel": interaction.channel_id}
        if old.get("channel") == interaction.channel_id:
            entry["message"] = old.get("message")
        try:
            await post_board(entry)
        except Exception as e:
            await interaction.followup.send(f"⚠️ 投稿できませんでした: {e}", ephemeral=True)
            return
        if old.get("message") and old.get("message") != entry["message"]:
            try:
                await bot.get_partial_messageable(old["channel"]).get_partial_message(old["message"]).delete()
            except discord.HTTPException:
                pass
        boards[key] = entry
        save_boards(boards)
    await interaction.followup.send(f"✅ このチャンネルに {kind.name} ランキングを投稿しました。毎日0時（日本時間）に更新します。", ephemeral=True)


@tree.command(name="rankingupdate", description="登録者の戦績を取り直して、このサーバーのランキングを今すぐ更新します（管理者用）")
@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
async def rankingupdate(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ このコマンドは管理者だけが使えます。", ephemeral=True)
        return
    if update_lock.locked():
        await interaction.response.send_message("⏳ ランキングを更新中です。終わるまで待ってください。", ephemeral=True)
        return
    await interaction.response.defer(ephemeral=True)
    async with update_lock:
        await run_ranking_update(interaction.guild_id)
    await interaction.followup.send("✅ ランキングを更新しました。", ephemeral=True)


# --- パネル ---
@tree.command(name="panel", description="認証・チケットのボタン付きパネルをこのチャンネルに投稿します")
@app_commands.describe(kind="パネルの種類")
@app_commands.choices(kind=[
    app_commands.Choice(name="認証", value="verify"),
    app_commands.Choice(name="チケット", value="ticket"),
])
@app_commands.default_permissions(manage_channels=True)
@app_commands.guild_only()
async def panel(interaction: discord.Interaction, kind: app_commands.Choice[str]):
    view = verify.VerifyPanelView() if kind.value == "verify" else tickets.TicketPanelView()
    try:
        await interaction.channel.send(embed=tickets.panel_embed(kind.value), view=view)
    except discord.HTTPException as e:
        await interaction.response.send_message(f"⚠️ 投稿できませんでした: {e}", ephemeral=True)
        return
    await interaction.response.send_message(f"✅ {kind.name}パネルを投稿しました。", ephemeral=True)


# --- AIチャンネル ---
@bot.event
async def on_message(message: discord.Message):
    await ai.handle_message(bot, message)


@tree.command(name="aichannel", description="このチャンネルでAIが返事するかを切り替えます")
@app_commands.default_permissions(manage_channels=True)
@app_commands.guild_only()
async def aichannel(interaction: discord.Interaction):
    on = ai.channels.toggle(interaction.channel_id)
    msg = "✅ このチャンネルでAIが返事するようにしました。" if on else "⏹️ このチャンネルでのAIの返事を止めました。"
    if on and not ai.DEEPSEEK_KEY:
        msg += "\n⚠️ DEEPSEEK_API_KEY が設定されていないので、今は返事しません。"
    perms = interaction.app_permissions
    if on and not (perms.read_message_history and perms.send_messages):
        msg += "\n⚠️ botにこのチャンネルの「メッセージ履歴を読む」と「メッセージを送信」の権限がないので、返事できません。"
    await interaction.response.send_message(msg, ephemeral=True)


# --- /setkey（既存） ---
@tree.command(name="setkey", description="APIキーを更新")
async def setkey(interaction: discord.Interaction, new_key: str):
    global current_api_key
    if interaction.user.id not in AUTHORIZED_USERS:
        await interaction.response.send_message("❌ 権限なし", ephemeral=True)
        return
    current_api_key = new_key
    await interaction.response.send_message("✅ 更新完了", ephemeral=True)


keep_alive()
bot.run(TOKEN)
