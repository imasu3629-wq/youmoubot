import random

import discord

ROLE_NAME = "認証済み"


class VerifyModal(discord.ui.Modal):
    def __init__(self):
        super().__init__(title="認証")
        a, b = random.randint(1, 20), random.randint(1, 20)
        if random.random() < 0.5:
            self.question, self.answer = f"{a} + {b}", a + b
        else:
            a, b = max(a, b), min(a, b)
            self.question, self.answer = f"{a} - {b}", a - b
        self.input = discord.ui.TextInput(label=f"{self.question} = ?", placeholder="答えを数字で入力", max_length=3)
        self.add_item(self.input)

    async def on_submit(self, interaction: discord.Interaction):
        if self.input.value.strip() != str(self.answer):
            await interaction.response.send_message("❌ 不正解です。もう一度ボタンを押してください。", ephemeral=True)
            return
        guild = interaction.guild
        try:
            role = discord.utils.get(guild.roles, name=ROLE_NAME) or await guild.create_role(
                name=ROLE_NAME, permissions=discord.Permissions(view_channel=True), reason="認証用ロール"
            )
            await interaction.user.add_roles(role, reason="計算問題に正解")
        except discord.HTTPException as e:
            await interaction.response.send_message(f"⚠️ ロールを付けられませんでした: {e}", ephemeral=True)
            return
        await interaction.response.send_message("✅ 認証しました！ほかのチャンネルが見られるようになりました。", ephemeral=True)


class VerifyPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="認証する", emoji="✅", style=discord.ButtonStyle.success, custom_id="verify:open")
    async def open_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if discord.utils.get(interaction.user.roles, name=ROLE_NAME):
            await interaction.response.send_message("✅ すでに認証済みです。", ephemeral=True)
            return
        await interaction.response.send_modal(VerifyModal())
