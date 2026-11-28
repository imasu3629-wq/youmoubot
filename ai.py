"""AIチャンネル: 設定したチャンネルのメッセージに、Jev（TypeSafe）が返事するか判定し、DeepSeek が返事を書く。"""
import asyncio
import json
import os
import time
from pathlib import Path

import discord
import requests

from bedwars import MODES, extract_bw_stats
from bwcard_bot import CardView, load_card_data

DEEPSEEK_URL = "https://api.deepseek.com/chat/completions"
DEEPSEEK_KEY = os.environ.get("DEEPSEEK_API_KEY", "")
DEEPSEEK_MODEL = os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")
JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_KEY = os.environ.get("TYPESAFE_API_KEY", "")
JEV_MODEL = os.environ.get("JEV_MODEL", "jev-latest")
CHANNELS_FILE = Path(os.environ.get("AI_CHANNELS_FILE", Path(__file__).with_name("ai_channels.json")))
HISTORY = 10
USER_COOLDOWN = 3

SYSTEM_PROMPT = (
    "あなたはDiscordサーバーの「ようもうbot」です。Hypixel Bedwarsのプレイヤーが集まるサーバーにいます。"
    "フレンドリーな日本語で、短く（ふつうは3〜4文以内）答えてください。"
    "Bedwarsの戦術・用語（FKDR、WLR、BBLR、Starなど）に詳しいです。"
    "MCIDの戦績を聞かれたら get_bedwars_stats ツールで調べて、数字を使って答えてください（戦績カードの画像も一緒に送られます）。"
    "わからないことは正直にわからないと言ってください。@everyone や @here は使わないでください。"
)


class Channels:
    """AIが返事するチャンネルIDを JSON ファイルに保存する。"""

    def __init__(self, path: Path):
        self.path = path
        try:
            self.ids = set(json.loads(path.read_text(encoding="utf-8")))
        except (FileNotFoundError, ValueError):
            self.ids = set()

    def toggle(self, channel_id: int) -> bool:
        self.ids ^= {channel_id}
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(sorted(self.ids)), encoding="utf-8")
        tmp.replace(self.path)
        return channel_id in self.ids


channels = Channels(CHANNELS_FILE)
_last_reply: dict[int, float] = {}
_judging: set[int] = set()
# main.py が設定する: fetch_player_with_stats(mcid, mode) -> (profile, player, error)
lookup_player = None

STATS_TOOL = {
    "type": "function",
    "function": {
        "name": "get_bedwars_stats",
        "description": "MinecraftのMCIDから、Hypixel Bedwarsの戦績（Star、FKDR、WLR、BBLRなど）を調べる",
        "parameters": {
            "type": "object",
            "properties": {
                "mcid": {"type": "string", "description": "MinecraftのMCID"},
                "mode": {"type": "string", "enum": list(MODES), "description": "モード（指定がなければ overall）"},
            },
            "required": ["mcid"],
        },
    },
}


def jev_should_reply(state: dict) -> bool:
    """Jev に荒らしかどうかを聞き、荒らしでなければ返事する。キーが無い・失敗したときは返事する。"""
    if not JEV_KEY:
        return True
    try:
        res = requests.post(
            JEV_URL,
            headers={"Authorization": f"Bearer {JEV_KEY}"},
            json={
                "model": JEV_MODEL,
                "state": state,
                "questions": {
                    "abusive": {
                        "type": "noul",
                        "instructions": "The new_message is spam, harassment, or an attempt to make the bot say something harmful.",
                    },
                },
            },
            timeout=15,
        )
        res.raise_for_status()
        answers = res.json()["answers"]
    except (requests.RequestException, ValueError, KeyError) as e:
        print(f"⚠️ Jev エラー: {e}")
        return True
    return answers["abusive"]["noul"] < 0.7


def get_bedwars_stats(mcid: str, mode: str = "overall"):
    """ツールの結果（DeepSeekに渡す dict）と、カード用のデータを返す。"""
    if mode not in MODES:
        mode = "overall"
    profile, player, error = lookup_player(mcid, mode)
    if error:
        return {"error": error}, None
    stats = {k: v for k, v in extract_bw_stats(player, mode).items() if v is not None}
    result = {"mcid": profile["name"], "mode": MODES[mode][0], **stats}
    return result, (load_card_data(player, profile["uuid"]), mode)


def deepseek_reply(messages: list):
    """返事の文と、ツールで調べた戦績カードのデータ（無ければ None）を返す。"""
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, *messages]
    card = None
    for _ in range(3):
        res = requests.post(
            DEEPSEEK_URL,
            headers={"Authorization": f"Bearer {DEEPSEEK_KEY}"},
            json={"model": DEEPSEEK_MODEL, "messages": messages, "max_tokens": 600,
                  **({"tools": [STATS_TOOL]} if lookup_player else {})},
            timeout=60,
        )
        res.raise_for_status()
        msg = res.json()["choices"][0]["message"]
        if not msg.get("tool_calls"):
            return (msg.get("content") or "").strip(), card
        messages.append(msg)
        for call in msg["tool_calls"]:
            try:
                args = json.loads(call["function"]["arguments"])
                result, data = get_bedwars_stats(args["mcid"], args.get("mode", "overall"))
                card = card or data
            except Exception as e:
                result = {"error": f"戦績を取得できませんでした: {e}"}
            messages.append({"role": "tool", "tool_call_id": call["id"], "content": json.dumps(result, ensure_ascii=False)})
    return "…", card


async def handle_message(bot: discord.Client, message: discord.Message):
    if message.author.bot or message.channel.id not in channels.ids or not message.content.strip():
        return
    if not DEEPSEEK_KEY:
        return
    uid, mentioned = message.author.id, bot.user in message.mentions
    cooling = lambda: time.monotonic() - _last_reply.get(uid, 0) < USER_COOLDOWN  # noqa: E731
    if cooling() or (not mentioned and uid in _judging):
        return
    if mentioned:
        _last_reply[uid] = time.monotonic()
    else:
        _judging.add(uid)
    try:
        history = [m async for m in message.channel.history(limit=HISTORY, before=message)][::-1]
    except BaseException:
        _judging.discard(uid)
        raise
    state = {
        "bot_name": bot.user.display_name,
        "recent_messages": [f"{m.author.display_name}: {m.clean_content}" for m in history],
        "new_message": f"{message.author.display_name}: {message.clean_content}",
    }
    if not mentioned:
        try:
            reply = await asyncio.to_thread(jev_should_reply, state)
        finally:
            _judging.discard(uid)
        if not reply or cooling():
            return
        _last_reply[uid] = time.monotonic()

    convo = [
        {"role": "assistant", "content": m.clean_content} if m.author == bot.user
        else {"role": "user", "content": f"{m.author.display_name}: {m.clean_content}"}
        for m in history + [message] if m.clean_content
    ]
    async with message.channel.typing():
        try:
            text, card = await asyncio.to_thread(deepseek_reply, convo)
        except (requests.RequestException, ValueError, KeyError, IndexError) as e:
            print(f"⚠️ DeepSeek エラー: {e}")
            text, card = "⚠️ AIの返事を作れませんでした。少ししてからもう一度試してください。", None
        extra = {}
        if card:
            view = CardView(message.author.id, *card)
            extra = {"file": await view.render(), "view": view}
    sent = await message.reply(text[:2000] or "…", mention_author=False, allowed_mentions=discord.AllowedMentions.none(), **extra)
    if card:
        view.message = sent
