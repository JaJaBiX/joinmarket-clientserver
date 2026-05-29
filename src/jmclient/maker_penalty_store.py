import os
import sqlite3
import time
from configparser import NoOptionError, NoSectionError
from typing import Iterable, List, Optional, Set

from jmbase import get_log
from jmclient.configure import jm_single


jlog = get_log()


DEFAULT_DB_RELATIVE_PATH = os.path.join("cmtdata", "taker-maker-policy.sqlite3")


def _now_int(now: Optional[float] = None) -> int:
    return int(time.time() if now is None else now)


def _split_nicks(value: str) -> Set[str]:
    nicks = set()
    for raw in value.replace(",", "\n").splitlines():
        nick = raw.strip()
        if not nick or nick.startswith("#"):
            continue
        nicks.add(nick)
    return nicks


class MakerPenaltyStore(object):
    """SQLite-backed taker maker penalty state.

    This store intentionally records only operational maker nicks and coarse
    failure metadata. Do not add tx hex, addresses, wallet identifiers, RPC
    credentials or other sensitive data here.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or self._get_db_path_from_config()
        self.conn = None
        self._connect()

    def _connect(self) -> None:
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _init_schema(self) -> None:
        assert self.conn is not None
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS maker_penalties (
                    nick TEXT PRIMARY KEY,
                    cooldown_until INTEGER NOT NULL DEFAULT 0,
                    cooldown_count INTEGER NOT NULL DEFAULT 0,
                    cooldown_window_start INTEGER NOT NULL DEFAULT 0,
                    banned INTEGER NOT NULL DEFAULT 0,
                    banned_until INTEGER NOT NULL DEFAULT 0,
                    ban_reason TEXT NOT NULL DEFAULT '',
                    last_stage TEXT NOT NULL DEFAULT '',
                    last_attempt_id TEXT NOT NULL DEFAULT '',
                    last_event_at INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_maker_penalties_cooldown_until
                ON maker_penalties(cooldown_until)
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_maker_penalties_ban
                ON maker_penalties(banned, banned_until)
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS maker_penalty_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nick TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    attempt_id TEXT NOT NULL DEFAULT '',
                    created_at INTEGER NOT NULL,
                    details TEXT NOT NULL DEFAULT ''
                )
                """
            )
            self.conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_maker_penalty_events_nick_created
                ON maker_penalty_events(nick, created_at)
                """
            )
            self.conn.execute("PRAGMA user_version = 1")

    @staticmethod
    def _config_get(section: str, key: str, default: str = "") -> str:
        try:
            return jm_single().config.get(section, key)
        except (NoOptionError, NoSectionError):
            return default

    @staticmethod
    def _config_getint(section: str, key: str, default: int) -> int:
        try:
            return jm_single().config.getint(section, key)
        except (ValueError, NoOptionError, NoSectionError):
            return default

    def _get_db_path_from_config(self) -> str:
        configured = self._config_get("POLICY", "taker_maker_penalty_db", "").strip()
        if configured:
            if os.path.isabs(configured):
                return configured
            return os.path.join(jm_single().datadir or ".", configured)
        return os.path.join(jm_single().datadir or ".", DEFAULT_DB_RELATIVE_PATH)

    def _get_configured_ban_file_path(self) -> str:
        configured = self._config_get("POLICY", "taker_persistent_maker_ban_file", "").strip()
        if not configured:
            return ""
        if os.path.isabs(configured):
            return configured
        return os.path.join(jm_single().datadir or ".", configured)

    def configured_bans(self) -> List[str]:
        nicks = _split_nicks(
            self._config_get("POLICY", "taker_persistent_maker_ban_list", "")
        )
        ban_file = self._get_configured_ban_file_path()
        if ban_file:
            try:
                with open(ban_file, "r", encoding="utf-8") as f:
                    nicks.update(_split_nicks(f.read()))
            except FileNotFoundError:
                jlog.warning("Configured taker maker ban file not found: {}".format(ban_file))
            except OSError as exc:
                jlog.warning(
                    "Could not read configured taker maker ban file {}: {}".format(
                        ban_file, exc
                    )
                )
        return sorted(nicks)

    def active_cooldowns(self, now: Optional[float] = None) -> List[str]:
        assert self.conn is not None
        now_i = _now_int(now)
        rows = self.conn.execute(
            """
            SELECT nick FROM maker_penalties
            WHERE cooldown_until > ?
            ORDER BY nick
            """,
            (now_i,),
        ).fetchall()
        return [row[0] for row in rows]

    def active_db_bans(self, now: Optional[float] = None) -> List[str]:
        assert self.conn is not None
        now_i = _now_int(now)
        rows = self.conn.execute(
            """
            SELECT nick FROM maker_penalties
            WHERE banned = 1 AND (banned_until = 0 OR banned_until > ?)
            ORDER BY nick
            """,
            (now_i,),
        ).fetchall()
        return [row[0] for row in rows]

    def active_hard_bans(self, now: Optional[float] = None) -> List[str]:
        return sorted(set(self.configured_bans()) | set(self.active_db_bans(now)))

    def prune(self, now: Optional[float] = None) -> None:
        """Clear expired cooldown and temporary ban flags.

        Keeping historical rows and events helps diagnostics while ensuring
        active lookups stay simple and deterministic.
        """
        assert self.conn is not None
        now_i = _now_int(now)
        with self.conn:
            self.conn.execute(
                """
                UPDATE maker_penalties
                SET cooldown_until = 0, updated_at = ?
                WHERE cooldown_until > 0 AND cooldown_until <= ?
                """,
                (now_i, now_i),
            )
            self.conn.execute(
                """
                UPDATE maker_penalties
                SET banned = 0, banned_until = 0, updated_at = ?
                WHERE banned = 1 AND banned_until > 0 AND banned_until <= ?
                """,
                (now_i, now_i),
            )

    def record_stage2_cooldown(
            self, makers: Iterable[str], attempt_id: str = "",
            now: Optional[float] = None) -> None:
        cooldown_seconds = self._config_getint(
            "POLICY", "taker_stage2_maker_cooldown_seconds", 3600)
        if cooldown_seconds <= 0:
            return

        now_i = _now_int(now)
        cooldown_until = now_i + cooldown_seconds
        threshold = self._config_getint(
            "POLICY", "taker_maker_cooldown_ban_threshold", 3)
        window_seconds = self._config_getint(
            "POLICY", "taker_maker_cooldown_ban_window_seconds", 604800)
        auto_ban_seconds = self._config_getint(
            "POLICY", "taker_maker_auto_ban_seconds", 0)

        for nick in sorted(set(m.strip() for m in makers if m and m.strip())):
            self._record_single_stage2_cooldown(
                nick, attempt_id, now_i, cooldown_until, threshold,
                window_seconds, auto_ban_seconds)

    def _record_single_stage2_cooldown(
            self, nick: str, attempt_id: str, now_i: int, cooldown_until: int,
            threshold: int, window_seconds: int, auto_ban_seconds: int) -> None:
        assert self.conn is not None
        with self.conn:
            row = self.conn.execute(
                """
                SELECT cooldown_count, cooldown_window_start
                FROM maker_penalties
                WHERE nick = ?
                """,
                (nick,),
            ).fetchone()
            if row is None:
                cooldown_count = 0
                window_start = now_i
            else:
                cooldown_count = int(row[0])
                window_start = int(row[1])
                if window_seconds > 0 and (
                        window_start <= 0 or now_i - window_start > window_seconds):
                    cooldown_count = 0
                    window_start = now_i

            cooldown_count += 1
            banned = 0
            banned_until = 0
            ban_reason = ""
            if threshold > 0 and cooldown_count >= threshold:
                banned = 1
                banned_until = 0 if auto_ban_seconds <= 0 else now_i + auto_ban_seconds
                ban_reason = "stage2_cooldown_threshold"

            self.conn.execute(
                """
                INSERT INTO maker_penalties (
                    nick, cooldown_until, cooldown_count,
                    cooldown_window_start, banned, banned_until, ban_reason,
                    last_stage, last_attempt_id, last_event_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(nick) DO UPDATE SET
                    cooldown_until = excluded.cooldown_until,
                    cooldown_count = excluded.cooldown_count,
                    cooldown_window_start = excluded.cooldown_window_start,
                    banned = CASE
                        WHEN excluded.banned = 1 THEN 1 ELSE maker_penalties.banned
                    END,
                    banned_until = CASE
                        WHEN excluded.banned = 1 THEN excluded.banned_until
                        ELSE maker_penalties.banned_until
                    END,
                    ban_reason = CASE
                        WHEN excluded.banned = 1 THEN excluded.ban_reason
                        ELSE maker_penalties.ban_reason
                    END,
                    last_stage = excluded.last_stage,
                    last_attempt_id = excluded.last_attempt_id,
                    last_event_at = excluded.last_event_at,
                    updated_at = excluded.updated_at
                """,
                (
                    nick, cooldown_until, cooldown_count, window_start, banned,
                    banned_until, ban_reason, "stage2", attempt_id, now_i, now_i,
                ),
            )
            self.conn.execute(
                """
                INSERT INTO maker_penalty_events (
                    nick, event_type, stage, attempt_id, created_at, details
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (nick, "cooldown", "stage2", attempt_id, now_i, "stage2_timeout"),
            )

        if banned:
            jlog.info(
                "taker_maker_auto_ban nick={} reason={} cooldown_count={} "
                "threshold={}".format(
                    nick, ban_reason, cooldown_count, threshold))

    def record_soft_failure(
            self, makers: Iterable[str], stage: str, attempt_id: str = "",
            now: Optional[float] = None) -> None:
        assert self.conn is not None
        now_i = _now_int(now)
        stage = (stage or "unknown").strip() or "unknown"
        for nick in sorted(set(m.strip() for m in makers if m and m.strip())):
            with self.conn:
                self.conn.execute(
                    """
                    INSERT INTO maker_penalties (
                        nick, last_stage, last_attempt_id, last_event_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(nick) DO UPDATE SET
                        last_stage = excluded.last_stage,
                        last_attempt_id = excluded.last_attempt_id,
                        last_event_at = excluded.last_event_at,
                        updated_at = excluded.updated_at
                    """,
                    (nick, stage, attempt_id, now_i, now_i),
                )
                self.conn.execute(
                    """
                    INSERT INTO maker_penalty_events (
                        nick, event_type, stage, attempt_id, created_at, details
                    ) VALUES (?, ?, ?, ?, ?, '')
                    """,
                    (nick, "soft_failure", stage, attempt_id, now_i),
                )

    def ban(
            self, nick: str, reason: str = "manual", until: int = 0,
            now: Optional[float] = None) -> None:
        assert self.conn is not None
        nick = nick.strip()
        if not nick:
            return
        now_i = _now_int(now)
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO maker_penalties (
                    nick, banned, banned_until, ban_reason, last_stage,
                    last_attempt_id, last_event_at, updated_at
                ) VALUES (?, 1, ?, ?, '', '', ?, ?)
                ON CONFLICT(nick) DO UPDATE SET
                    banned = 1,
                    banned_until = excluded.banned_until,
                    ban_reason = excluded.ban_reason,
                    last_event_at = excluded.last_event_at,
                    updated_at = excluded.updated_at
                """,
                (nick, int(until), reason, now_i, now_i),
            )
            self.conn.execute(
                """
                INSERT INTO maker_penalty_events (
                    nick, event_type, stage, attempt_id, created_at, details
                ) VALUES (?, ?, '', '', ?, ?)
                """,
                (nick, "ban", now_i, reason),
            )

    def unban(self, nick: str, now: Optional[float] = None) -> None:
        assert self.conn is not None
        nick = nick.strip()
        if not nick:
            return
        now_i = _now_int(now)
        with self.conn:
            self.conn.execute(
                """
                UPDATE maker_penalties
                SET banned = 0, banned_until = 0, updated_at = ?
                WHERE nick = ?
                """,
                (now_i, nick),
            )
            self.conn.execute(
                """
                INSERT INTO maker_penalty_events (
                    nick, event_type, stage, attempt_id, created_at, details
                ) VALUES (?, ?, '', '', ?, '')
                """,
                (nick, "unban", now_i),
            )
