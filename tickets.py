import asyncio
import json
import os

import discord

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "tickets.json")
CATEGORY_NAME = "チケット"
lock = asyncio.Lock()


def load_state() -> dict:
    """{サーバーID: {"next": 次の番号, "open": {チャンネルID: {"role", "user", "number"}}}}"""
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def save_state(state: dict):
    with open(STATE_FILE + ".tmp", "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(STATE_FILE + ".tmp", STATE_FILE)


class TicketPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="チケットを作成", emoji="🎫", style=discord.ButtonStyle.primary, custom_id="ticket:open")
    async def open_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await open_ticket(interaction)


class TicketCloseView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="チケットを閉じる", emoji="🔒", style=discord.ButtonStyle.danger, custom_id="ticket:close")
    async def close_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await close_ticket(interaction)


async def open_ticket(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    guild, user = interaction.guild, interaction.user
    hidden = discord.PermissionOverwrite(view_channel=False)
    bot_perms = discord.PermissionOverwrite(
        view_channel=True, send_messages=True, read_message_history=True, manage_channels=True
    )
    async with lock:
        state = load_state()
        g = state.setdefault(str(guild.id), {"next": 1, "open": {}})
        for cid, t in g["open"].items():
            if t["user"] == user.id and guild.get_channel(int(cid)):
                await interaction.followup.send(f"⚠️ すでにチケット <#{cid}> が開いています。", ephemeral=True)
                return
        number = g["next"]
        name = f"ticket-{number}"
        role = channel = None
        try:
            role = await guild.create_role(name=name, reason=f"{user} のチケット")
            await user.add_roles(role, reason=f"{name} を作成")
            category = discord.utils.get(guild.categories, name=CATEGORY_NAME) or await guild.create_category(
                CATEGORY_NAME, overwrites={guild.default_role: hidden, guild.me: bot_perms}
            )
            channel = await guild.create_text_channel(name, category=category, overwrites={
                guild.default_role: hidden,
                role: discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, read_message_history=True,
                    attach_files=True, embed_links=True
                ),
                guild.me: bot_perms,
            })
        except discord.HTTPException as e:
            for obj in (channel, role):
                if obj:
                    await obj.delete()
            await interaction.followup.send(f"⚠️ チケットを作成できませんでした: {e}", ephemeral=True)
            return
        g["next"] = number + 1
        g["open"][str(channel.id)] = {"role": role.id, "user": user.id, "number": number}
        save_state(state)

    embed = discord.Embed(
        title=f"🎫 チケット #{number}",
        description="ご用件を書いてください。管理者が対応します。\n終わったら下のボタンで閉じてください（チャンネルとロールが削除されます）。",
        color=0x5865F2,
    )
    await channel.send(user.mention, embed=embed, view=TicketCloseView())
    await interaction.followup.send(f"✅ チケット {channel.mention} を作成しました。", ephemeral=True)


async def close_ticket(interaction: discord.Interaction):
    guild, channel = interaction.guild, interaction.channel
    async with lock:
        state = load_state()
        ticket = state.get(str(guild.id), {}).get("open", {}).pop(str(channel.id), None)
        if ticket:
            save_state(state)
    if not ticket:
        await interaction.response.send_message("⚠️ このチャンネルはチケットではありません。", ephemeral=True)
        return
    await interaction.response.send_message("🔒 チケットを閉じます…")
    role = guild.get_role(ticket["role"])
    if role:
        await role.delete(reason=f"ticket-{ticket['number']} を閉じた")
    await channel.delete(reason=f"{interaction.user} がチケットを閉じた")
