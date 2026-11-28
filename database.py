import sqlite3

import os
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_data.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_conn() as conn:
        # UUIDキャッシュ（Mojang API節約）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS uuid_cache (
                mcid TEXT PRIMARY KEY,
                uuid TEXT NOT NULL,
                cached_at TEXT DEFAULT (datetime('now'))
            )
        """)
        # ユーザー登録テーブル（UUID主キー）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS registrations (
                uuid TEXT PRIMARY KEY,
                mcid TEXT NOT NULL,
                discord_id TEXT NOT NULL,
                star INTEGER DEFAULT 0,
                fkdr REAL DEFAULT 0.0,
                registered_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            )
        """)
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(registrations)")}
        for col in ("wins", "final_kills"):
            if col not in cols:
                conn.execute(f"ALTER TABLE registrations ADD COLUMN {col} INTEGER DEFAULT 0")

# --- UUID キャッシュ ---
def get_cached_uuid(mcid: str):
    with get_conn() as conn:
        row = conn.execute(
            "SELECT uuid FROM uuid_cache WHERE mcid = ? AND cached_at > datetime('now', '-1 days')",
            (mcid.lower(),)
        ).fetchone()
        return row["uuid"] if row else None

def save_uuid_cache(mcid: str, uuid: str):
    with get_conn() as conn:
        conn.execute(
            "INSERT OR REPLACE INTO uuid_cache (mcid, uuid, cached_at) VALUES (?, ?, datetime('now'))",
            (mcid.lower(), uuid)
        )

# --- 登録 ---
def register_player(uuid: str, mcid: str, discord_id: str, star: int, fkdr: float, wins: int = 0, final_kills: int = 0) -> bool:
    """登録できたら True。すでに誰かが登録済みなら False。"""
    with get_conn() as conn:
        return conn.execute("""
            INSERT OR IGNORE INTO registrations (uuid, mcid, discord_id, star, fkdr, wins, final_kills, registered_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
        """, (uuid, mcid, str(discord_id), star, fkdr, wins, final_kills)).rowcount == 1

def update_stats(uuid: str, mcid: str, star: int, fkdr: float, wins: int = None, final_kills: int = None):
    with get_conn() as conn:
        conn.execute("""
            UPDATE registrations
            SET star = ?, fkdr = ?, mcid = ?, wins = COALESCE(?, wins), final_kills = COALESCE(?, final_kills),
                updated_at = datetime('now')
            WHERE uuid = ?
        """, (star, fkdr, mcid, wins, final_kills, uuid))

def get_all_registrations():
    with get_conn() as conn:
        return conn.execute("SELECT uuid, mcid FROM registrations").fetchall()

def is_registered(uuid: str) -> bool:
    with get_conn() as conn:
        row = conn.execute("SELECT 1 FROM registrations WHERE uuid = ?", (uuid,)).fetchone()
        return row is not None

def is_registered_by_discord(discord_id: str, uuid: str) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT 1 FROM registrations WHERE discord_id = ? AND uuid = ?",
            (str(discord_id), uuid)
        ).fetchone()
        return row is not None

# --- 一覧 ---
def get_registered_by_discord(discord_id: str):
    with get_conn() as conn:
        return conn.execute("""
            SELECT mcid, star, fkdr, updated_at
            FROM registrations
            WHERE discord_id = ?
            ORDER BY fkdr DESC
        """, (str(discord_id),)).fetchall()

# --- ランキング ---
def get_ranking_by_fkdr(limit=10):
    with get_conn() as conn:
        return conn.execute("""
            SELECT mcid, star, fkdr, updated_at
            FROM registrations
            ORDER BY fkdr DESC
            LIMIT ?
        """, (limit,)).fetchall()

def get_ranking_by_star(limit=10):
    with get_conn() as conn:
        return conn.execute("""
            SELECT mcid, star, fkdr, updated_at
            FROM registrations
            ORDER BY star DESC
            LIMIT ?
        """, (limit,)).fetchall()

RANKING_COLUMNS = ("fkdr", "star", "wins", "final_kills")

def get_ranking(column: str, limit=10):
    if column not in RANKING_COLUMNS:
        raise ValueError(column)
    with get_conn() as conn:
        return conn.execute(f"""
            SELECT uuid, mcid, star, fkdr, wins, final_kills
            FROM registrations
            ORDER BY {column} DESC
            LIMIT ?
        """, (limit,)).fetchall()

# --- 削除 ---
def delete_player(discord_id: str, uuid: str) -> bool:
    with get_conn() as conn:
        cursor = conn.execute(
            "DELETE FROM registrations WHERE discord_id = ? AND uuid = ?",
            (str(discord_id), uuid)
        )
        return cursor.rowcount > 0
