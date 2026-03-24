import os
import sqlite3
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_data.db")


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS uuid_cache (
                mcid TEXT PRIMARY KEY,
                uuid TEXT NOT NULL,
                cached_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS players (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                minecraft_uuid TEXT NOT NULL UNIQUE,
                minecraft_username TEXT NOT NULL,
                discord_user_id TEXT UNIQUE,
                linked_discord_text TEXT,
                verification_status TEXT NOT NULL,
                registered_by_admin INTEGER NOT NULL DEFAULT 0,
                registered_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        _migrate_players_table_if_needed(conn)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_players_status ON players (verification_status)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_players_discord_nonnull ON players(discord_user_id) WHERE discord_user_id IS NOT NULL"
        )
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS player_stats (
                minecraft_uuid TEXT PRIMARY KEY,
                minecraft_name TEXT NOT NULL,
                bedwars_star INTEGER NOT NULL DEFAULT 0,
                wins INTEGER NOT NULL DEFAULT 0,
                losses INTEGER NOT NULL DEFAULT 0,
                final_kills INTEGER NOT NULL DEFAULT 0,
                final_deaths INTEGER NOT NULL DEFAULT 0,
                beds_broken INTEGER NOT NULL DEFAULT 0,
                beds_lost INTEGER NOT NULL DEFAULT 0,
                kills INTEGER NOT NULL DEFAULT 0,
                deaths INTEGER NOT NULL DEFAULT 0,
                games_played INTEGER NOT NULL DEFAULT 0,
                winstreak INTEGER NOT NULL DEFAULT 0,
                fkdr REAL NOT NULL DEFAULT 0,
                wlr REAL NOT NULL DEFAULT 0,
                kdr REAL NOT NULL DEFAULT 0,
                last_updated TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_player_stats_name ON player_stats (minecraft_name)"
        )


def _migrate_players_table_if_needed(conn: sqlite3.Connection):
    columns = conn.execute("PRAGMA table_info(players)").fetchall()
    if not columns:
        return

    has_registered_by_admin = any(col["name"] == "registered_by_admin" for col in columns)
    discord_notnull = any(col["name"] == "discord_user_id" and col["notnull"] == 1 for col in columns)
    if has_registered_by_admin and not discord_notnull:
        return

    conn.execute("""
        CREATE TABLE IF NOT EXISTS players_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            minecraft_uuid TEXT NOT NULL UNIQUE,
            minecraft_username TEXT NOT NULL,
            discord_user_id TEXT UNIQUE,
            linked_discord_text TEXT,
            verification_status TEXT NOT NULL,
            registered_by_admin INTEGER NOT NULL DEFAULT 0,
            registered_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        INSERT INTO players_new (
            id,
            minecraft_uuid,
            minecraft_username,
            discord_user_id,
            linked_discord_text,
            verification_status,
            registered_by_admin,
            registered_at,
            updated_at
        )
        SELECT
            id,
            minecraft_uuid,
            minecraft_username,
            discord_user_id,
            linked_discord_text,
            verification_status,
            0,
            registered_at,
            updated_at
        FROM players
    """)
    conn.execute("DROP TABLE players")
    conn.execute("ALTER TABLE players_new RENAME TO players")


def get_cached_uuid(mcid: str) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT uuid FROM uuid_cache WHERE mcid = ? AND cached_at > datetime('now', '-1 days')",
            (mcid.strip().lower(),),
        ).fetchone()
        return row["uuid"] if row else None


def save_uuid_cache(mcid: str, uuid: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO uuid_cache (mcid, uuid, cached_at) VALUES (?, ?, datetime('now'))",
            (mcid.strip().lower(), uuid),
        )


def get_player_by_discord(discord_user_id: str):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM players WHERE discord_user_id = ?",
            (str(discord_user_id),),
        ).fetchone()



def get_player_by_uuid(minecraft_uuid: str):
    with get_conn() as conn:
        return conn.execute(
            "SELECT * FROM players WHERE minecraft_uuid = ?",
            (minecraft_uuid,),
        ).fetchone()



def register_verified_player(
    minecraft_uuid: str,
    minecraft_username: str,
    discord_user_id: Optional[str],
    linked_discord_text: Optional[str],
    verification_status: str = "verified",
    registered_by_admin: bool = False,
):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO players (
                minecraft_uuid,
                minecraft_username,
                discord_user_id,
                linked_discord_text,
                verification_status,
                registered_by_admin,
                registered_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            (
                minecraft_uuid,
                minecraft_username,
                str(discord_user_id) if discord_user_id is not None else None,
                linked_discord_text,
                verification_status,
                1 if registered_by_admin else 0,
            ),
        )



def delete_player_registration_by_discord(discord_user_id: str) -> bool:
    with get_conn() as conn:
        cursor = conn.execute(
            "DELETE FROM players WHERE discord_user_id = ?",
            (str(discord_user_id),),
        )
        return cursor.rowcount > 0



def delete_player_registration_by_uuid(minecraft_uuid: str) -> bool:
    with get_conn() as conn:
        cursor = conn.execute(
            "DELETE FROM players WHERE minecraft_uuid = ?",
            (minecraft_uuid,),
        )
        return cursor.rowcount > 0


def set_config_value(key: str, value: str):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO bot_config (key, value, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=datetime('now')
            """,
            (key, value),
        )


def get_config_value(key: str) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT value FROM bot_config WHERE key = ?",
            (key,),
        ).fetchone()
        return row["value"] if row else None


def get_all_registered_players():
    with get_conn() as conn:
        return conn.execute(
            "SELECT minecraft_uuid, minecraft_username FROM players"
        ).fetchall()


def upsert_player_stats(
    minecraft_uuid: str,
    minecraft_name: str,
    bedwars_star: int,
    wins: int,
    losses: int,
    final_kills: int,
    final_deaths: int,
    beds_broken: int,
    beds_lost: int,
    kills: int,
    deaths: int,
    games_played: int,
    winstreak: int,
    fkdr: float,
    wlr: float,
    kdr: float,
):
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO player_stats (
                minecraft_uuid,
                minecraft_name,
                bedwars_star,
                wins,
                losses,
                final_kills,
                final_deaths,
                beds_broken,
                beds_lost,
                kills,
                deaths,
                games_played,
                winstreak,
                fkdr,
                wlr,
                kdr,
                last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(minecraft_uuid) DO UPDATE SET
                minecraft_name=excluded.minecraft_name,
                bedwars_star=excluded.bedwars_star,
                wins=excluded.wins,
                losses=excluded.losses,
                final_kills=excluded.final_kills,
                final_deaths=excluded.final_deaths,
                beds_broken=excluded.beds_broken,
                beds_lost=excluded.beds_lost,
                kills=excluded.kills,
                deaths=excluded.deaths,
                games_played=excluded.games_played,
                winstreak=excluded.winstreak,
                fkdr=excluded.fkdr,
                wlr=excluded.wlr,
                kdr=excluded.kdr,
                last_updated=datetime('now')
            """,
            (
                minecraft_uuid,
                minecraft_name,
                bedwars_star,
                wins,
                losses,
                final_kills,
                final_deaths,
                beds_broken,
                beds_lost,
                kills,
                deaths,
                games_played,
                winstreak,
                fkdr,
                wlr,
                kdr,
            ),
        )
