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
                discord_user_id TEXT NOT NULL UNIQUE,
                linked_discord_text TEXT,
                verification_status TEXT NOT NULL,
                registered_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_players_status ON players (verification_status)"
        )


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
    discord_user_id: str,
    linked_discord_text: Optional[str],
    verification_status: str = "verified",
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
                registered_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), datetime('now'))
            """,
            (
                minecraft_uuid,
                minecraft_username,
                str(discord_user_id),
                linked_discord_text,
                verification_status,
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
