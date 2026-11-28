MODES = {
    "overall": ("総合", ""),
    "solo": ("ソロ", "eight_one_"),
    "doubles": ("ダブルス", "eight_two_"),
    "threes": ("3v3v3v3", "four_three_"),
    "fours": ("4v4v4v4", "four_four_"),
    "4v4": ("4v4", "two_four_"),
}

LEADERBOARDS = {
    "wins": ("Wins (総合)", "wins", "overall"),
    "wins_weekly": ("Wins (週間)", "wins", "weekly"),
    "final_kills": ("Final Kills (総合)", "final kills", "overall"),
    "final_kills_weekly": ("Final Kills (週間)", "final kills", "weekly"),
}


def ratio(a: int, b: int) -> float:
    return round(a / max(b, 1), 2)


def extract_bw_stats(player: dict, mode: str = "overall") -> dict:
    bw = (player.get("stats") or {}).get("Bedwars") or {}
    p = MODES[mode][1]

    def g(key):
        return bw.get(f"{p}{key}") or 0

    s = {
        "star": (player.get("achievements") or {}).get("bedwars_level") or 0,
        "wins": g("wins_bedwars"),
        "losses": g("losses_bedwars"),
        "final_kills": g("final_kills_bedwars"),
        "final_deaths": g("final_deaths_bedwars"),
        "winstreak": bw.get(f"{p}winstreak"),
    }
    s["fkdr"] = ratio(s["final_kills"], s["final_deaths"])
    s["wlr"] = ratio(s["wins"], s["losses"])
    # Auroraのキャッシュにはモード別のキル・ベッド数がない
    for key in ("kills", "deaths", "beds_broken", "beds_lost", "kdr", "bblr"):
        s[key] = None
    if mode == "overall":
        for key in ("kills", "deaths", "beds_broken", "beds_lost"):
            s[key] = bw.get(f"{key}_bedwars") or 0
        s["kdr"] = ratio(s["kills"], s["deaths"])
        s["bblr"] = ratio(s["beds_broken"], s["beds_lost"])
    return s


COMPARE_FIELDS = [
    ("⭐ Star", "star"),
    ("⚔️ FKDR", "fkdr"),
    ("🏆 WLR", "wlr"),
    ("🛏️ BBLR", "bblr"),
    ("🗡️ KDR", "kdr"),
    ("✅ Wins", "wins"),
    ("💀 Final Kills", "final_kills"),
    ("🛏️ Beds Broken", "beds_broken"),
]


def compare_line(label: str, a, b) -> str:
    mark_a = "🟢" if a > b else ("🟡" if a == b else "🔴")
    mark_b = "🟢" if b > a else ("🟡" if a == b else "🔴")
    return f"{label}: {mark_a} `{a:,}` vs `{b:,}` {mark_b}"


def find_leaderboard(leaderboards: dict, key: str):
    _, title_kw, prefix_kw = LEADERBOARDS[key]
    for lb in leaderboards.get("BEDWARS", []):
        title = (lb.get("title") or "").lower()
        prefix = (lb.get("prefix") or "").lower()
        if title_kw not in title:
            continue
        if prefix_kw and prefix_kw not in prefix:
            continue
        return lb
    return None
