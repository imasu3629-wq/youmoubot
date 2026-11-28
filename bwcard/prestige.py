"""ベッドウォーズのレベル計算・プレステージ配色・Hypixelランク表示。

配色データは 0〜10,000☆ のプレステージ（100☆ごと）に対応しています。
各スキームは次のどれか:
  ("u", 括弧, 数字, 星)   … 括弧/数字/星がそれぞれ単色
  ("c", "色の並び")        … 「[」「数字1」「数字2」…「星」「]」に順番に色を当てる
  ("p", "先頭の色", 残り)  … 先頭から順に色、残りは同じ色
  ("s", 残り, "末尾の色")  … 末尾から順に色、残りは同じ色
"""
from __future__ import annotations

from .text import MC_COLORS

SCHEMES: dict[str, tuple] = {
    "none": ("u", "7"), "iron": ("u", "f"), "gold": ("u", "6"), "diamond": ("u", "b"),
    "emerald": ("u", "2"), "sapphire": ("u", "3"), "ruby": ("u", "4"), "crystal": ("u", "d"),
    "opal": ("u", "9"), "amethyst": ("u", "5"),
    "rainbow": ("c", "c6eabd5"),
    "iron_prime": ("u", "7", "f", "7"), "gold_prime": ("u", "7", "e", "6"),
    "diamond_prime": ("u", "7", "b", "3"), "emerald_prime": ("u", "7", "a", "2"),
    "sapphire_prime": ("u", "7", "3", "9"), "ruby_prime": ("u", "7", "c", "4"),
    "crystal_prime": ("u", "7", "d", "5"), "opal_prime": ("u", "7", "9", "1"),
    "amethyst_prime": ("u", "7", "5", "8"),
    "mirror": ("c", "87ff77"), "light": ("c", "ffee666"), "dawn": ("c", "66ffb33"),
    "dusk": ("c", "55dd6ee"), "air": ("c", "bbff7788"), "wind": ("c", "ffaa2222"),
    "nebula": ("c", "44ccdd55"), "thunder": ("c", "eeff888"), "earth": ("c", "aa2266ee"),
    "water": ("c", "bb339911"), "fire": ("c", "ee66cc44"), "sunrise": ("c", "993366e"),
    "eclipse": ("c", "c4774cc"), "gamma": ("c", "999dcc4"), "majestic": ("c", "2add55"),
    "andesine": ("c", "cc442aa"), "marine": ("c", "aaab991"), "element": ("c", "44ccb33"),
    "galaxy": ("c", "11955d1"), "atomic": ("c", "ccaa399"), "sunset": ("c", "55cc66e"),
    "time": ("c", "ee6cdd5"), "winter": ("c", "193bf77"), "obsidian": ("c", "058855"),
    "spring": ("c", "22ae65d"), "ice": ("c", "ffbb333"), "summer": ("c", "3bee6d5"),
    "spinel": ("c", "f4cc919"), "autumn": ("c", "55c6eb3"), "mystic": ("c", "2affaa"),
    "eternal": ("c", "4459910"), "burnout": ("c", "4cc6ef4"), "cooldown": ("c", "193bfe1"),
    "obliteration": ("c", "5defed"), "ender": ("c", "3a282a3"), "brust": ("c", "2aefbd5"),
    "comical": ("c", "4cefec"), "lusterlost": ("c", "4623958"), "maelstrom": ("c", "5c6fb39"),
    "time_undone": ("c", "7087ff"), "umbrella": ("c", "cffff"), "luminous": ("c", "6efffb3"),
    "tortilla": ("c", "efe66fe"), "corn": ("u", "a", "e", "a"), "bittersweet": ("c", "bbcccaa"),
    "sweetsour": ("c", "33aafa3"), "pop": ("u", "9", "d", "b"), "bubblegum": ("u", "5", "d", "f"),
    "contrast": ("c", "066eeff"), "blended": ("c", "aaaa228"), "allay": ("u", "3", "b", "f"),
    "blaze": ("c", "4c6ec6e4"), "creeper": ("c", "2af2af8"), "drowned": ("c", "233bba2"),
    "enderman": ("u", "8", "8", "d"), "frog": ("c", "6622fff"), "ghast": ("c", "fff77c8"),
    "hoglin": ("u", "d", "c", "6"), "iron_golem": ("c", "87fffe8"), "jerry": ("c", "6f262f"),
    "kringle": ("c", "2aaac42"), "liquid": ("c", "87fb391"), "mint": ("u", "f", "f", "a"),
    "neglected": ("c", "8844cc8"), "onion": ("c", "fdddaaf"), "poser": ("u", "3", "6", "e"),
    "quartz": ("u", "d", "f", "e"), "rich": ("u", "8", "6"), "sanguine": ("c", "444ccff"),
    "titanic": ("c", "9bbb339"), "unorthodox": ("s", "d", "58"), "volcanic": ("c", "0c66cc4"),
    "weeping_cherry": ("u", "2", "d", "a"), "x_ray": ("u", "f", "8", "f"),
    "yearn": ("p", "e64", "8"), "zebra": ("c", "008877ff"), "caution": ("c", "eee00e0"),
    "indescribable": ("c", "dddeebe"), "forgotten": ("u", "0", "8"), "fuse": ("c", "87fffef"),
    "prestigious": ("c", "9bfffc4"),
}

# 100☆ごとのデフォルト配色（0☆, 100☆, …, 10000☆）
PRESTIGE_ORDER = (
    "none iron gold diamond emerald sapphire ruby crystal opal amethyst "
    "rainbow iron_prime gold_prime diamond_prime emerald_prime sapphire_prime ruby_prime "
    "crystal_prime opal_prime amethyst_prime "
    "mirror light dawn dusk air wind nebula thunder earth water "
    "fire sunrise eclipse gamma majestic andesine marine element galaxy atomic "
    "sunset time winter obsidian spring ice summer spinel autumn mystic "
    "eternal burnout cooldown obliteration ender brust comical lusterlost maelstrom time_undone "
    "umbrella luminous tortilla corn bittersweet sweetsour pop bubblegum contrast blended "
    "allay blaze creeper drowned enderman frog ghast hoglin iron_golem jerry "
    "kringle liquid mint neglected onion poser quartz rich sanguine titanic "
    "unorthodox volcanic weeping_cherry x_ray yearn zebra caution indescribable forgotten fuse "
    "prestigious"
).split()

STARS = {
    "black_open": "✫", "white_circled": "✪", "white_outlined": "⚝", "four_clubs": "✥",
    "black_outlined": "✭", "four_pointed": "✦", "pinwheel": "✵", "hollow": "✰", "nautical": "✯",
}
DEFAULT_STARS = ((4000, "✭"), (3000, "✥"), (2000, "⚝"), (1000, "✪"), (0, "✫"))
BRACKETS = {
    "none": ("[", "]"), "curly_brace": ("{", "}"), "parenthesis": ("(", ")"),
    "angled": ("<", ">"), "double_angle_quotation_mark": ("«", "»"),
}

# Hypixel のランク名 → 色コード
COLOR_NAMES = {
    "BLACK": "0", "DARK_BLUE": "1", "DARK_GREEN": "2", "DARK_AQUA": "3", "DARK_RED": "4",
    "DARK_PURPLE": "5", "GOLD": "6", "GRAY": "7", "DARK_GRAY": "8", "BLUE": "9", "GREEN": "a",
    "AQUA": "b", "RED": "c", "LIGHT_PURPLE": "d", "YELLOW": "e", "WHITE": "f",
}


# ---------------------------------------------------------------- レベル計算
def exp_required(level: int) -> int:
    """level → level+1 に必要な経験値。"""
    return {0: 500, 1: 1000, 2: 2000, 3: 3500}.get(level % 100, 5000)


def level_info(exp: int) -> tuple[int, int, int]:
    """経験値 → (レベル, そのレベル内の経験値, 次のレベルまでに必要な経験値)。"""
    exp = max(0, int(exp or 0))
    prestige, rest = divmod(exp, 487_000)  # 1プレステージ(100☆) = 487,000 EXP
    for i in range(100):
        req = exp_required(i)
        if rest < req:
            return prestige * 100 + i, rest, req
        rest -= req
    return prestige * 100 + 100, 0, 500  # 到達しない


def level_float(exp: int) -> float:
    lvl, cur, req = level_info(exp)
    return lvl + cur / req


# ---------------------------------------------------------------- プレステージ表示
def default_scheme(level: int) -> str:
    return PRESTIGE_ORDER[max(0, min(level // 100, len(PRESTIGE_ORDER) - 1))]


def default_star(level: int) -> str:
    return next(star for req, star in DEFAULT_STARS if level >= req)


def _part_colors(scheme: tuple, n_parts: int) -> list[str]:
    kind = scheme[0]
    if kind == "u":
        bracket = scheme[1]
        digit = scheme[2] if len(scheme) > 2 else bracket
        star = scheme[3] if len(scheme) > 3 else digit
        return [bracket] + [digit] * (n_parts - 3) + [star, bracket]
    if kind == "c":
        cyc = scheme[1]
        return [cyc[i % len(cyc)] for i in range(n_parts)]
    if kind == "p":
        pre, rest = scheme[1], scheme[2]
        return [pre[i] if i < len(pre) else rest for i in range(n_parts)]
    if kind == "s":
        rest, suf = scheme[1], scheme[2]
        start = n_parts - len(suf)
        return [suf[i - start] if i >= start else rest for i in range(n_parts)]
    raise ValueError(kind)


def format_level(level: int, scheme: str | None = None, star: str | None = None,
                 brackets: tuple[str, str] | None = None, bold: bool = False) -> str:
    """例: format_level(2347) → '§5[2§d3§d4§67§e⚝§e]' のような§付き文字列。"""
    scheme = SCHEMES.get(scheme or "", None) or SCHEMES[default_scheme(level)]
    star = star or default_star(level)
    left, right = brackets or ("[", "]")
    parts = [left, *str(level), star, right]
    colors = _part_colors(scheme, len(parts))
    out = []
    for i, (part, col) in enumerate(zip(parts, colors)):
        is_digit = 0 < i < len(parts) - 2
        out.append(f"§{col}{'§l' if bold and is_digit else ''}{part}")
    return "".join(out)


def level_colors(level: int, scheme: str | None = None) -> list[tuple[int, int, int]]:
    """そのプレステージで使われている色（重複なし）。グラデーションの色決めに使う。"""
    sch = SCHEMES.get(scheme or "", None) or SCHEMES[default_scheme(level)]
    seen: list[str] = []
    for c in _part_colors(sch, len(str(level)) + 3):
        if c not in seen:
            seen.append(c)
    return [MC_COLORS[c] for c in seen]


def cosmetics_from_api(bw: dict) -> dict:
    """APIの active_prestige_scheme / active_star / active_prestige_bracket を読む。"""
    def pick(value, prefix, table):
        if not value or value.startswith("random"):  # ランダム設定は標準の見た目で表示
            return None
        key = value[len(prefix) + 1:] if value.startswith(prefix + "_") else value
        return table.get(key)

    scheme_key = bw.get("active_prestige_scheme") or ""
    scheme_key = scheme_key[len("prestige_scheme_"):] if scheme_key.startswith("prestige_scheme_") else None
    df = bw.get("dreamfeast") or {}
    return {
        "scheme": scheme_key if scheme_key in SCHEMES else None,
        "star": pick(bw.get("active_star"), "star", STARS),
        "brackets": pick(bw.get("active_prestige_bracket"), "prestige_bracket", BRACKETS),
        "bold": bool(df.get("toggle_bold_numbers")),
    }


# ---------------------------------------------------------------- ランク
_STAFF = {
    "ADMIN": "§c[ADMIN]", "OWNER": "§c[OWNER]", "GAME_MASTER": "§2[GM]", "MODERATOR": "§2[MOD]",
    "HELPER": "§9[HELPER]", "YOUTUBER": "§c[§fYOUTUBE§c]", "STAFF": "§c[§6ዞ§c]",
    "MOJANG": "§6[MOJANG]", "EVENTS": "§6[EVENTS]",
}


def rank_prefix(player: dict) -> str:
    """Hypixel の player オブジェクト → '§6[MVP§c++§6]' のようなランク表示（無ランクは空文字）。"""
    if player.get("prefix"):
        return player["prefix"]
    rank = player.get("rank")
    if rank and rank != "NORMAL":
        return _STAFF.get(rank, f"§c[{rank}]")
    plus = COLOR_NAMES.get(player.get("rankPlusColor") or "RED", "c")
    if player.get("monthlyPackageRank") == "SUPERSTAR":
        base = "b" if player.get("monthlyRankColor") == "AQUA" else "6"
        return f"§{base}[MVP§{plus}++§{base}]"
    pkg = player.get("newPackageRank") or player.get("packageRank")
    return {
        "VIP": "§a[VIP]", "VIP_PLUS": "§a[VIP§6+§a]", "MVP": "§b[MVP]", "MVP_PLUS": f"§b[MVP§{plus}+§b]",
    }.get(pkg or "", "")


def last_color(text: str, default: str = "7") -> str:
    color = default
    for i in range(len(text) - 1):
        if text[i] == "§" and text[i + 1].lower() in MC_COLORS:
            color = text[i + 1].lower()
    return color
