import os
import hashlib
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import psycopg2
from psycopg2 import IntegrityError
from psycopg2.extras import Json, RealDictCursor

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")


def _connect_kwargs() -> dict[str, Any]:
    kwargs: dict[str, Any] = {"dsn": DATABASE_URL}
    if "sslmode=" not in DATABASE_URL:
        kwargs["sslmode"] = os.environ.get("PGSSLMODE", "disable" if ".railway.internal" in DATABASE_URL else "require")
    return kwargs


@contextmanager
def get_conn() -> Iterator[psycopg2.extensions.connection]:
    conn = psycopg2.connect(**_connect_kwargs())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _fetchone_dict(cursor) -> Optional[dict[str, Any]]:
    row = cursor.fetchone()
    return dict(row) if row else None


def _fetchall_dict(cursor) -> list[dict[str, Any]]:
    return [dict(row) for row in cursor.fetchall()]


def init_db():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS uuid_cache (
                mcid TEXT PRIMARY KEY,
                uuid TEXT NOT NULL,
                cached_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS verified_users (
                id SERIAL PRIMARY KEY,
                discord_user_id BIGINT UNIQUE NOT NULL,
                mcid VARCHAR(32) UNIQUE NOT NULL,
                uuid VARCHAR(36),
                hypixel_discord_tag VARCHAR(64),
                head_base64 TEXT,
                fkdr DOUBLE PRECISION,
                wins INTEGER,
                losses INTEGER,
                final_kills INTEGER,
                final_deaths INTEGER,
                bedwars_star INTEGER,
                verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS id SERIAL")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS hypixel_discord_tag VARCHAR(64)")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS head_base64 TEXT")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS fkdr DOUBLE PRECISION")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS wins INTEGER")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS losses INTEGER")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS final_kills INTEGER")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS final_deaths INTEGER")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS bedwars_star INTEGER")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS verified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        cur.execute("ALTER TABLE verified_users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        cur.execute("ALTER TABLE verified_users DROP CONSTRAINT IF EXISTS verified_users_tag_allowed_values")
        cur.execute("ALTER TABLE verified_users DROP COLUMN IF EXISTS tag")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                id SERIAL PRIMARY KEY,
                minecraft_uuid TEXT NOT NULL UNIQUE,
                minecraft_username TEXT NOT NULL,
                discord_user_id TEXT UNIQUE,
                linked_discord_text TEXT,
                verification_status TEXT NOT NULL,
                registered_by_admin BOOLEAN NOT NULL DEFAULT FALSE,
                registered_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_players_status ON players (verification_status)")
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_players_discord_nonnull ON players(discord_user_id) WHERE discord_user_id IS NOT NULL"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
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
                fkdr DOUBLE PRECISION NOT NULL DEFAULT 0,
                wlr DOUBLE PRECISION NOT NULL DEFAULT 0,
                kdr DOUBLE PRECISION NOT NULL DEFAULT 0,
                head_image_base64 TEXT,
                raw_flashlight_json JSONB,
                last_updated TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute("ALTER TABLE player_stats ADD COLUMN IF NOT EXISTS raw_flashlight_json JSONB")
        cur.execute(
            """
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name = 'player_stats'
                      AND column_name = 'raw_flashlight_json'
                      AND data_type = 'json'
                ) THEN
                    ALTER TABLE player_stats
                    ALTER COLUMN raw_flashlight_json
                    TYPE JSONB
                    USING raw_flashlight_json::jsonb;
                END IF;
            END$$;
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_player_stats_name ON player_stats (minecraft_name)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ranking_message_state (
                guild_id BIGINT NOT NULL,
                channel_id BIGINT NOT NULL,
                ranking_type TEXT NOT NULL,
                message_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
                last_updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (guild_id, channel_id, ranking_type)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS hidden_urchin_tags (
                mcid TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT NOW(),
                added_by BIGINT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS manual_tags (
                mcid TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                added_by BIGINT NOT NULL,
                PRIMARY KEY (mcid, tag_name)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_manual_tags_tag_name ON manual_tags (tag_name)")


def _admin_shadow_id(minecraft_uuid: str) -> int:
    digest = hashlib.sha1(minecraft_uuid.encode("utf-8")).hexdigest()[:15]
    return -int(digest, 16)


def _normalize_row_with_aliases(row: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    if not row:
        return None
    normalized = dict(row)
    normalized.setdefault("minecraft_username", normalized.get("mcid"))
    normalized.setdefault("minecraft_uuid", normalized.get("uuid"))
    normalized.setdefault("linked_discord_text", None)
    normalized.setdefault("verification_status", "verified")
    normalized.setdefault("registered_by_admin", False)
    normalized.setdefault("registered_at", normalized.get("verified_at"))
    normalized.setdefault("updated_at", normalized.get("updated_at", normalized.get("verified_at")))
    return normalized


def get_cached_uuid(mcid: str) -> Optional[str]:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT uuid FROM uuid_cache WHERE mcid = %s AND cached_at > NOW() - INTERVAL '1 day'",
            (mcid.strip().lower(),),
        )
        row = _fetchone_dict(cur)
        return row["uuid"] if row else None


def save_uuid_cache(mcid: str, uuid: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO uuid_cache (mcid, uuid, cached_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (mcid) DO UPDATE SET
                uuid = EXCLUDED.uuid,
                cached_at = NOW()
            """,
            (mcid.strip().lower(), uuid),
        )


def get_player_by_discord(discord_user_id: str):
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT discord_user_id, mcid, uuid, verified_at, updated_at FROM verified_users WHERE discord_user_id = %s",
            (int(discord_user_id),),
        )
        return _normalize_row_with_aliases(_fetchone_dict(cur))


def get_player_by_uuid(minecraft_uuid: str):
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT discord_user_id, mcid, uuid, verified_at, updated_at FROM verified_users WHERE uuid = %s",
            (minecraft_uuid,),
        )
        row = _fetchone_dict(cur)
        if not row:
            return None
        normalized = _normalize_row_with_aliases(row)
        if normalized and int(normalized.get("discord_user_id") or 0) < 0:
            normalized["discord_user_id"] = None
            normalized["registered_by_admin"] = True
            normalized["verification_status"] = "forced_verified"
        return normalized


def get_player_by_username(minecraft_username: str):
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            "SELECT discord_user_id, mcid, uuid, verified_at, updated_at FROM verified_users WHERE lower(mcid) = lower(%s)",
            (minecraft_username,),
        )
        return _normalize_row_with_aliases(_fetchone_dict(cur))


def register_verified_player(
    minecraft_uuid: str,
    minecraft_username: str,
    discord_user_id: Optional[str],
    linked_discord_text: Optional[str],
    verification_status: str = "verified",
    registered_by_admin: bool = False,
):
    normalized_discord_user_id = _admin_shadow_id(minecraft_uuid) if discord_user_id is None else int(discord_user_id)
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO verified_users (
                discord_user_id,
                mcid,
                uuid,
                hypixel_discord_tag,
                verified_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, NOW(), NOW())
            """,
            (normalized_discord_user_id, minecraft_username, minecraft_uuid, linked_discord_text),
        )
        cur.execute(
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
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (minecraft_uuid) DO UPDATE SET
                minecraft_username = EXCLUDED.minecraft_username,
                discord_user_id = EXCLUDED.discord_user_id,
                linked_discord_text = EXCLUDED.linked_discord_text,
                verification_status = EXCLUDED.verification_status,
                registered_by_admin = EXCLUDED.registered_by_admin,
                updated_at = NOW()
            """,
            (
                minecraft_uuid,
                minecraft_username,
                str(normalized_discord_user_id) if normalized_discord_user_id > 0 else None,
                linked_discord_text,
                verification_status,
                registered_by_admin,
            ),
        )


def delete_player_registration_by_discord(discord_user_id: str) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM verified_users WHERE discord_user_id = %s", (int(discord_user_id),))
        return cur.rowcount > 0


def delete_player_registration_by_uuid(minecraft_uuid: str) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT mcid FROM verified_users WHERE uuid = %s", (minecraft_uuid,))
        row = cur.fetchone()
        mcid = str(row[0]).strip().lower() if row and row[0] else None
        cur.execute("DELETE FROM verified_users WHERE uuid = %s", (minecraft_uuid,))
        deleted = cur.rowcount > 0
        cur.execute("DELETE FROM players WHERE minecraft_uuid = %s", (minecraft_uuid,))
        if mcid:
            cur.execute("DELETE FROM hidden_urchin_tags WHERE mcid = %s", (mcid,))
            cur.execute("DELETE FROM manual_tags WHERE mcid = %s", (mcid,))
        return deleted


def delete_registered_player_data_by_uuid(minecraft_uuid: str) -> bool:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT mcid FROM verified_users WHERE uuid = %s", (minecraft_uuid,))
        row = cur.fetchone()
        mcid = str(row[0]).strip().lower() if row and row[0] else None
        cur.execute("DELETE FROM verified_users WHERE uuid = %s", (minecraft_uuid,))
        deleted = cur.rowcount > 0
        cur.execute("DELETE FROM players WHERE minecraft_uuid = %s", (minecraft_uuid,))
        cur.execute("DELETE FROM player_stats WHERE minecraft_uuid = %s", (minecraft_uuid,))
        if mcid:
            cur.execute("DELETE FROM hidden_urchin_tags WHERE mcid = %s", (mcid,))
            cur.execute("DELETE FROM manual_tags WHERE mcid = %s", (mcid,))
        return deleted


def update_player_uuid(old_uuid: str, new_uuid: str, minecraft_username: Optional[str] = None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM player_stats WHERE minecraft_uuid = %s", (new_uuid,))
        cur.execute(
            """
            UPDATE verified_users
            SET uuid = %s,
                mcid = COALESCE(%s, mcid)
            WHERE uuid = %s
            """,
            (new_uuid, minecraft_username, old_uuid),
        )
        cur.execute(
            """
            UPDATE players
            SET minecraft_uuid = %s,
                minecraft_username = COALESCE(%s, minecraft_username),
                updated_at = NOW()
            WHERE minecraft_uuid = %s
            """,
            (new_uuid, minecraft_username, old_uuid),
        )
        cur.execute(
            """
            UPDATE player_stats
            SET minecraft_uuid = %s,
                minecraft_name = COALESCE(%s, minecraft_name),
                last_updated = NOW()
            WHERE minecraft_uuid = %s
            """,
            (new_uuid, minecraft_username, old_uuid),
        )


def set_config_value(key: str, value: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO bot_config (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT(key) DO UPDATE SET
                value = EXCLUDED.value,
                updated_at = NOW()
            """,
            (key, value),
        )


def get_config_value(key: str) -> Optional[str]:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT value FROM bot_config WHERE key = %s", (key,))
        row = _fetchone_dict(cur)
        return row["value"] if row else None


def get_all_registered_players():
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT uuid AS minecraft_uuid, mcid AS minecraft_username FROM verified_users")
        return _fetchall_dict(cur)


def get_registered_mcids_for_autocomplete(prefix: str = "", limit: int = 25) -> list[str]:
    normalized_prefix = (prefix or "").strip().lower()
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        if normalized_prefix:
            cur.execute(
                """
                SELECT mcid
                FROM verified_users
                WHERE lower(mcid) LIKE %s
                ORDER BY lower(mcid) ASC
                LIMIT %s
                """,
                (f"{normalized_prefix}%", limit),
            )
        else:
            cur.execute(
                """
                SELECT mcid
                FROM verified_users
                ORDER BY lower(mcid) ASC
                LIMIT %s
                """,
                (limit,),
            )
        rows = _fetchall_dict(cur)
        return [str(row["mcid"]) for row in rows if row.get("mcid")]


def get_registered_player_by_mcid(mcid: str) -> Optional[dict[str, Any]]:
    normalized_mcid = (mcid or "").strip().lower()
    if not normalized_mcid:
        return None
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT mcid, uuid
            FROM verified_users
            WHERE lower(mcid) = %s
            LIMIT 1
            """,
            (normalized_mcid,),
        )
        return _fetchone_dict(cur)


def _normalize_mcid_key(mcid: str) -> str:
    return (mcid or "").strip().lower()


def _normalize_tag_key(tag_name: str) -> str:
    return (tag_name or "").strip().lower()


def add_hidden_urchin_mcid(mcid: str, added_by: int) -> bool:
    normalized_mcid = _normalize_mcid_key(mcid)
    if not normalized_mcid:
        return False
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO hidden_urchin_tags (mcid, added_by)
            VALUES (%s, %s)
            ON CONFLICT (mcid) DO NOTHING
            """,
            (normalized_mcid, int(added_by)),
        )
        return cur.rowcount > 0


def remove_hidden_urchin_mcid(mcid: str) -> bool:
    normalized_mcid = _normalize_mcid_key(mcid)
    if not normalized_mcid:
        return False
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM hidden_urchin_tags WHERE mcid = %s", (normalized_mcid,))
        return cur.rowcount > 0


def is_hidden_urchin_mcid(mcid: str) -> bool:
    normalized_mcid = _normalize_mcid_key(mcid)
    if not normalized_mcid:
        return False
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM hidden_urchin_tags WHERE mcid = %s", (normalized_mcid,))
        return cur.fetchone() is not None


def list_hidden_urchin_mcids() -> list[str]:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT mcid FROM hidden_urchin_tags ORDER BY mcid ASC")
        rows = _fetchall_dict(cur)
        return [str(row["mcid"]) for row in rows if row.get("mcid")]


def add_manual_tag(mcid: str, tag_name: str, added_by: int) -> bool:
    normalized_mcid = _normalize_mcid_key(mcid)
    normalized_tag = _normalize_tag_key(tag_name)
    if not normalized_mcid or not normalized_tag:
        return False
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO manual_tags (mcid, tag_name, added_by)
            VALUES (%s, %s, %s)
            ON CONFLICT (mcid, tag_name) DO NOTHING
            """,
            (normalized_mcid, normalized_tag, int(added_by)),
        )
        return cur.rowcount > 0


def remove_manual_tag(mcid: str, tag_name: str) -> bool:
    normalized_mcid = _normalize_mcid_key(mcid)
    normalized_tag = _normalize_tag_key(tag_name)
    if not normalized_mcid or not normalized_tag:
        return False
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM manual_tags WHERE mcid = %s AND tag_name = %s", (normalized_mcid, normalized_tag))
        return cur.rowcount > 0


def has_manual_tag(mcid: str, tag_name: str) -> bool:
    normalized_mcid = _normalize_mcid_key(mcid)
    normalized_tag = _normalize_tag_key(tag_name)
    if not normalized_mcid or not normalized_tag:
        return False
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM manual_tags WHERE mcid = %s AND tag_name = %s",
            (normalized_mcid, normalized_tag),
        )
        return cur.fetchone() is not None


def list_manual_tags_for_mcid(mcid: str) -> list[str]:
    normalized_mcid = _normalize_mcid_key(mcid)
    if not normalized_mcid:
        return []
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT tag_name
            FROM manual_tags
            WHERE mcid = %s
            ORDER BY tag_name ASC
            """,
            (normalized_mcid,),
        )
        rows = _fetchall_dict(cur)
        return [str(row["tag_name"]) for row in rows if row.get("tag_name")]


def list_mcids_by_manual_tag(tag_name: str, prefix: str = "", limit: int = 25) -> list[str]:
    normalized_tag = _normalize_tag_key(tag_name)
    normalized_prefix = _normalize_mcid_key(prefix)
    if not normalized_tag:
        return []
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        if normalized_prefix:
            cur.execute(
                """
                SELECT mcid
                FROM manual_tags
                WHERE tag_name = %s
                  AND mcid LIKE %s
                ORDER BY mcid ASC
                LIMIT %s
                """,
                (normalized_tag, f"{normalized_prefix}%", limit),
            )
        else:
            cur.execute(
                """
                SELECT mcid
                FROM manual_tags
                WHERE tag_name = %s
                ORDER BY mcid ASC
                LIMIT %s
                """,
                (normalized_tag, limit),
            )
        rows = _fetchall_dict(cur)
        return [str(row["mcid"]) for row in rows if row.get("mcid")]


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
    head_image_base64: Optional[str] = None,
    raw_flashlight_json: Optional[Any] = None,
):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
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
                head_image_base64,
                raw_flashlight_json,
                last_updated
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT(minecraft_uuid) DO UPDATE SET
                minecraft_name = EXCLUDED.minecraft_name,
                bedwars_star = EXCLUDED.bedwars_star,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                final_kills = EXCLUDED.final_kills,
                final_deaths = EXCLUDED.final_deaths,
                beds_broken = EXCLUDED.beds_broken,
                beds_lost = EXCLUDED.beds_lost,
                kills = EXCLUDED.kills,
                deaths = EXCLUDED.deaths,
                games_played = EXCLUDED.games_played,
                winstreak = EXCLUDED.winstreak,
                fkdr = EXCLUDED.fkdr,
                wlr = EXCLUDED.wlr,
                kdr = EXCLUDED.kdr,
                head_image_base64 = COALESCE(EXCLUDED.head_image_base64, player_stats.head_image_base64),
                raw_flashlight_json = EXCLUDED.raw_flashlight_json,
                last_updated = NOW()
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
                head_image_base64,
                Json(raw_flashlight_json) if raw_flashlight_json is not None else None,
            ),
        )


def get_player_stats_by_uuid(minecraft_uuid: str):
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                ps.minecraft_uuid,
                ps.minecraft_name,
                ps.bedwars_star,
                ps.wins,
                ps.losses,
                ps.final_kills,
                ps.final_deaths,
                ps.beds_broken,
                ps.beds_lost,
                ps.kills,
                ps.deaths,
                ps.winstreak,
                ps.fkdr,
                ps.wlr,
                ps.kdr,
                ps.head_image_base64,
                ps.raw_flashlight_json,
                ps.last_updated
            FROM player_stats ps
            WHERE ps.minecraft_uuid = %s
            """,
            (minecraft_uuid,),
        )
        return _fetchone_dict(cur)


def get_top_player_stats(metric: str, limit: int = 10):
    metric_column_map = {
        "fkdr": "fkdr",
        "wins": "wins",
        "star": "bedwars_star",
        "wlr": "wlr",
        "kdr": "kdr",
        "final_kills": "final_kills",
        "beds_broken": "beds_broken",
        "winstreak": "winstreak",
        "bblr": "bblr",
    }
    order_column = metric_column_map.get(metric)
    if not order_column:
        raise ValueError("Invalid ranking metric")

    if metric == "bblr":
        order_expression = """
            CASE
                WHEN COALESCE(ps.beds_lost, 0) > 0 THEN ps.beds_broken::double precision / ps.beds_lost
                WHEN COALESCE(ps.beds_broken, 0) > 0 THEN ps.beds_broken::double precision
                ELSE 0
            END
        """
    else:
        order_expression = f"COALESCE(ps.{order_column}, 0)"

    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                ps.minecraft_uuid,
                ps.minecraft_name,
                ps.bedwars_star,
                ps.wins,
                ps.losses,
                ps.final_kills,
                ps.final_deaths,
                ps.beds_broken,
                ps.beds_lost,
                ps.kills,
                ps.deaths,
                ps.games_played,
                ps.winstreak,
                ps.fkdr,
                ps.wlr,
                ps.kdr,
                ps.head_image_base64,
                ps.last_updated
            FROM player_stats ps
            INNER JOIN verified_users vu
                ON vu.uuid = ps.minecraft_uuid
            ORDER BY {order_expression} DESC, ps.minecraft_name ASC
            LIMIT %s
            """,
            (limit,),
        )
        return _fetchall_dict(cur)


def get_ranking_message_state(guild_id: int, channel_id: int, ranking_type: str) -> Optional[dict[str, Any]]:
    with get_conn() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT guild_id, channel_id, ranking_type, message_ids, last_updated_at
            FROM ranking_message_state
            WHERE guild_id = %s AND channel_id = %s AND ranking_type = %s
            """,
            (int(guild_id), int(channel_id), ranking_type),
        )
        row = _fetchone_dict(cur)
        if not row:
            return None
        message_ids = row.get("message_ids") or []
        row["message_ids"] = [int(mid) for mid in message_ids if str(mid).isdigit()]
        return row


def upsert_ranking_message_state(
    guild_id: int,
    channel_id: int,
    ranking_type: str,
    message_ids: list[int],
):
    normalized_ids = [int(mid) for mid in message_ids]
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ranking_message_state (
                guild_id, channel_id, ranking_type, message_ids, last_updated_at
            )
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (guild_id, channel_id, ranking_type) DO UPDATE SET
                message_ids = EXCLUDED.message_ids,
                last_updated_at = NOW()
            """,
            (int(guild_id), int(channel_id), ranking_type, Json(normalized_ids)),
        )


def update_player_head_image(minecraft_uuid: str, head_image_base64: str):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE player_stats
            SET head_image_base64 = %s,
                last_updated = NOW()
            WHERE minecraft_uuid = %s
            """,
            (head_image_base64, minecraft_uuid),
        )
