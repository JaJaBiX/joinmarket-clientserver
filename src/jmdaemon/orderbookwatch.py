#! /usr/bin/env python

import sqlite3
import sys
import threading
import time
import uuid
from decimal import InvalidOperation, Decimal
from numbers import Integral

from jmdaemon.protocol import COMMAND_PREFIX, JM_VERSION
from jmdaemon import fidelity_bond_sanity_check
from jmbase.support import dict_factory, get_log, joinmarket_alert
log = get_log()


class JMTakerError(Exception):
    pass


class RefreshStateStore(object):
    """File-backed state for paced directory-node orderbook refreshes."""

    def __init__(self, db_path, busy_timeout_ms=5000):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.con = sqlite3.connect(db_path, check_same_thread=False,
                                   isolation_level=None)
        self.con.row_factory = dict_factory
        self.con.execute("PRAGMA busy_timeout={};".format(
            int(busy_timeout_ms)))
        self.con.execute("PRAGMA journal_mode=WAL;")
        self.con.execute("PRAGMA synchronous=NORMAL;")
        self._migrate()

    def close(self):
        with self.lock:
            self.con.close()

    def _migrate(self):
        now = self._now()
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                self.con.execute(
                    "CREATE TABLE IF NOT EXISTS directory_peers_refresh("
                    "directory TEXT PRIMARY KEY, "
                    "need_refresh INTEGER NOT NULL DEFAULT 1, "
                    "cooldown_time INTEGER NOT NULL DEFAULT 0, "
                    "current_refresh_id TEXT, "
                    "current_generation_offer_count INTEGER NOT NULL "
                    "DEFAULT 0, "
                    "current_generation_fidelitybond_count INTEGER NOT NULL "
                    "DEFAULT 0, "
                    "last_request_at INTEGER, "
                    "last_response_at INTEGER, "
                    "last_successful_refresh_at INTEGER, "
                    "last_connected_at INTEGER, "
                    "last_disconnected_at INTEGER, "
                    "request_count INTEGER NOT NULL DEFAULT 0, "
                    "response_count INTEGER NOT NULL DEFAULT 0, "
                    "consecutive_missing_count INTEGER NOT NULL DEFAULT 0, "
                    "last_missing_accounted_request_at INTEGER, "
                    "updated_at INTEGER);")
                self.con.execute(
                    "CREATE INDEX IF NOT EXISTS "
                    "directory_peers_refresh_due "
                    "ON directory_peers_refresh(need_refresh, "
                    "cooldown_time);")
                self.con.execute(
                    "UPDATE directory_peers_refresh SET "
                    "need_refresh=1, "
                    "current_refresh_id=NULL, "
                    "current_generation_offer_count=0, "
                    "current_generation_fidelitybond_count=0, "
                    "last_successful_refresh_at=NULL, "
                    "last_missing_accounted_request_at=NULL, "
                    "updated_at=? "
                    "WHERE last_request_at IS NULL AND ("
                    "need_refresh=0 OR current_refresh_id IS NOT NULL OR "
                    "current_generation_offer_count != 0 OR "
                    "current_generation_fidelitybond_count != 0 OR "
                    "last_successful_refresh_at IS NOT NULL);",
                    (now,))
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    @staticmethod
    def _now():
        return int(time.time())

    def _ensure_directory_locked(self, directory, now):
        self.con.execute(
            "INSERT OR IGNORE INTO directory_peers_refresh"
            "(directory, updated_at) VALUES(?, ?);",
            (directory, now))

    def mark_connected(self, directory, mark_need_refresh=False, now=None):
        if not directory:
            return
        now = self._now() if now is None else now
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                self._ensure_directory_locked(directory, now)
                assignments = ["last_connected_at=?", "updated_at=?"]
                values = [now, now]
                if mark_need_refresh:
                    assignments.append("need_refresh=1")
                self.con.execute(
                    "UPDATE directory_peers_refresh SET " +
                    ", ".join(assignments) + " WHERE directory=?;",
                    values + [directory])
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    def mark_disconnected(self, directory, now=None):
        if not directory:
            return
        now = self._now() if now is None else now
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                self._ensure_directory_locked(directory, now)
                self.con.execute(
                    "UPDATE directory_peers_refresh SET "
                    "last_disconnected_at=?, updated_at=? "
                    "WHERE directory=?;",
                    (now, now, directory))
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    def mark_due_connected_directories(self, directories, target_seconds,
                                       now=None, reset_fresh=False):
        now = self._now() if now is None else now
        directories = [d for d in directories if d]
        if not directories:
            return
        fresh_value = "0" if reset_fresh else "need_refresh"
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                for directory in directories:
                    self._ensure_directory_locked(directory, now)
                    self.con.execute(
                        "UPDATE directory_peers_refresh SET "
                        "last_connected_at=COALESCE(last_connected_at, ?), "
                        "need_refresh=CASE "
                        "WHEN last_successful_refresh_at IS NULL THEN 1 "
                        "WHEN ? - last_successful_refresh_at > ? THEN 1 "
                        "ELSE " + fresh_value + " END, "
                        "updated_at=? WHERE directory=?;",
                        (now, now, int(target_seconds), now, directory))
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    def mark_need_refresh(self, directories, now=None):
        now = self._now() if now is None else now
        directories = [d for d in directories if d]
        if not directories:
            return
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                for directory in directories:
                    self._ensure_directory_locked(directory, now)
                    self.con.execute(
                        "UPDATE directory_peers_refresh SET need_refresh=1, "
                        "updated_at=? WHERE directory=?;",
                        (now, directory))
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    def _account_missing_generation_locked(self, directory, now):
        cur = self.con.execute(
            "SELECT last_request_at, current_generation_offer_count, "
            "current_generation_fidelitybond_count, "
            "last_missing_accounted_request_at "
            "FROM directory_peers_refresh WHERE directory=?;",
            (directory,))
        row = cur.fetchone()
        if not row or row["last_request_at"] is None:
            return
        if row["last_missing_accounted_request_at"] == row["last_request_at"]:
            return
        total = int(row["current_generation_offer_count"] or 0) + \
            int(row["current_generation_fidelitybond_count"] or 0)
        if total != 0:
            return
        self.con.execute(
            "UPDATE directory_peers_refresh SET "
            "consecutive_missing_count=consecutive_missing_count + 1, "
            "last_missing_accounted_request_at=last_request_at, "
            "updated_at=? WHERE directory=?;",
            (now, directory))

    def record_request(self, directory, refresh_id, cooldown_seconds, now=None):
        now = self._now() if now is None else now
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                self._ensure_directory_locked(directory, now)
                self._account_missing_generation_locked(directory, now)
                self.con.execute(
                    "UPDATE directory_peers_refresh SET "
                    "need_refresh=0, cooldown_time=?, "
                    "current_refresh_id=?, "
                    "current_generation_offer_count=0, "
                    "current_generation_fidelitybond_count=0, "
                    "last_request_at=?, "
                    "request_count=request_count + 1, "
                    "updated_at=? WHERE directory=?;",
                    (now + int(cooldown_seconds), refresh_id, now, now,
                     directory))
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    def record_request_send_failure(self, directory, now=None):
        now = self._now() if now is None else now
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                self._ensure_directory_locked(directory, now)
                self._account_missing_generation_locked(directory, now)
                self.con.execute(
                    "UPDATE directory_peers_refresh SET need_refresh=1, "
                    "current_refresh_id=NULL, "
                    "current_generation_offer_count=0, "
                    "current_generation_fidelitybond_count=0, "
                    "updated_at=? WHERE directory=?;",
                    (now, directory))
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    def record_response(self, directory, response_type, now=None):
        if not directory:
            return
        now = self._now() if now is None else now
        offer_increment = 1 if response_type == "offer" else 0
        fidelitybond_increment = 1 if response_type == "fidelitybond" else 0
        with self.lock:
            self.con.execute("BEGIN IMMEDIATE;")
            try:
                self._ensure_directory_locked(directory, now)
                cur = self.con.execute(
                    "SELECT current_refresh_id, last_request_at, "
                    "last_successful_refresh_at "
                    "FROM directory_peers_refresh WHERE directory=?;",
                    (directory,))
                row = cur.fetchone()
                has_current_request = bool(
                    row and row["current_refresh_id"] and
                    row["last_request_at"] is not None)
                first_response_for_request = bool(
                    has_current_request and
                    (row["last_successful_refresh_at"] is None or
                     row["last_successful_refresh_at"] <
                     row["last_request_at"]))
                if first_response_for_request:
                    self.con.execute(
                        "UPDATE directory_peers_refresh SET "
                        "last_response_at=?, "
                        "last_successful_refresh_at=?, "
                        "need_refresh=0, "
                        "current_generation_offer_count="
                        "current_generation_offer_count + ?, "
                        "current_generation_fidelitybond_count="
                        "current_generation_fidelitybond_count + ?, "
                        "response_count=response_count + 1, "
                        "consecutive_missing_count=0, "
                        "last_missing_accounted_request_at=NULL, "
                        "updated_at=? WHERE directory=?;",
                        (now, now, offer_increment, fidelitybond_increment,
                         now, directory))
                elif has_current_request:
                    self.con.execute(
                        "UPDATE directory_peers_refresh SET "
                        "last_response_at=?, "
                        "current_generation_offer_count="
                        "current_generation_offer_count + ?, "
                        "current_generation_fidelitybond_count="
                        "current_generation_fidelitybond_count + ?, "
                        "response_count=response_count + 1, "
                        "updated_at=? WHERE directory=?;",
                        (now, offer_increment, fidelitybond_increment, now,
                         directory))
                else:
                    self.con.execute(
                        "UPDATE directory_peers_refresh SET "
                        "last_response_at=?, "
                        "response_count=response_count + 1, "
                        "updated_at=? WHERE directory=?;",
                        (now, now, directory))
                self.con.execute("COMMIT;")
            except Exception:
                self.con.execute("ROLLBACK;")
                raise

    def select_next_refresh(self, runtime_liquidity, now=None):
        now = self._now() if now is None else now
        runtime_liquidity = [row for row in runtime_liquidity
                             if row.get("directory")]
        if not runtime_liquidity:
            return None
        values_sql = ",".join(["(?, ?, ?)"] * len(runtime_liquidity))
        params = []
        for row in runtime_liquidity:
            params.extend([
                row["directory"],
                int(row.get("orderbook_size", 0) or 0),
                int(row.get("fidelitybond_size", 0) or 0),
            ])
        params.append(now)
        sql = (
            "WITH runtime(directory, orderbook_size, fidelitybond_size) "
            "AS (VALUES " + values_sql + ") "
            "SELECT dpr.*, runtime.orderbook_size, "
            "runtime.fidelitybond_size "
            "FROM directory_peers_refresh AS dpr "
            "JOIN runtime ON runtime.directory = dpr.directory "
            "WHERE dpr.need_refresh = 1 "
            "AND COALESCE(dpr.cooldown_time, 0) < ? "
            "ORDER BY COALESCE(dpr.consecutive_missing_count, 0) ASC, "
            "runtime.orderbook_size DESC, "
            "runtime.fidelitybond_size DESC, "
            "COALESCE(dpr.last_request_at, 0) ASC, "
            "dpr.directory ASC LIMIT 1;")
        with self.lock:
            cur = self.con.execute(sql, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def get_rows(self):
        with self.lock:
            cur = self.con.execute(
                "SELECT * FROM directory_peers_refresh "
                "ORDER BY directory;")
            return cur.fetchall()


class OrderbookWatch(object):

    def set_msgchan(self, msgchan):
        self.msgchan = msgchan
        self.current_refresh_id = None
        if not hasattr(self, "source_inactive_grace_seconds"):
            self.source_inactive_grace_seconds = -1
        if not hasattr(self, "orphan_source_retention_seconds"):
            self.orphan_source_retention_seconds = 0
        self.msgchan.register_orderbookwatch_callbacks(self.on_order_seen,
                               self.on_order_cancel, self.on_fidelity_bond_seen)
        self.msgchan.register_channel_callbacks(
            self.on_welcome, self.on_set_topic, None, self.on_disconnect,
            self.on_nick_leave, None, None, self.on_message_seen)

        self.dblock = threading.Lock()
        con = sqlite3.connect(":memory:", check_same_thread=False)
        con.row_factory = dict_factory
        self.db = con.cursor()
        self._visible_orderbook_cache = []
        self._visible_fidelitybond_cache = []
        self._visibility_cache_dirty = True
        self._visibility_cache_updated_at = None
        try:
            self.dblock.acquire(True)
            self.db.execute("CREATE TABLE orderbook(counterparty TEXT, "
                            "oid INTEGER, ordertype TEXT, minsize INTEGER, "
                            "maxsize INTEGER, txfee INTEGER, cjfee TEXT, "
                            "PRIMARY KEY(counterparty, oid));")
            self.db.execute("CREATE TABLE fidelitybonds(counterparty TEXT, "
                "takernick TEXT, proof TEXT, PRIMARY KEY(counterparty));");
            self.db.execute("CREATE TABLE orderbook_sources("
                            "counterparty TEXT, oid INTEGER, directory TEXT, "
                            "first_seen_at INTEGER, last_seen_at INTEGER, "
                            "last_refresh_id TEXT NOT NULL, "
                            "active INTEGER DEFAULT 1, "
                            "inactive_at INTEGER, "
                            "PRIMARY KEY(counterparty, oid, directory));")
            self.db.execute("CREATE TABLE fidelitybond_sources("
                            "counterparty TEXT, directory TEXT, "
                            "first_seen_at INTEGER, last_seen_at INTEGER, "
                            "last_refresh_id TEXT NOT NULL, "
                            "active INTEGER DEFAULT 1, "
                            "inactive_at INTEGER, "
                            "PRIMARY KEY(counterparty, directory));")
            self.db.execute("CREATE TABLE directory_peers("
                            "directory TEXT PRIMARY KEY, "
                            "first_connected_at INTEGER, "
                            "last_connected_at INTEGER, "
                            "last_disconnected_at INTEGER, "
                            "last_seen_at INTEGER, "
                            "last_message_at INTEGER, "
                            "last_pubmsg_at INTEGER, "
                            "last_orderbook_request_seen_at INTEGER, "
                            "last_orderbook_request_at INTEGER, "
                            "last_orderbook_response_at INTEGER, "
                            "last_successful_refresh_at INTEGER, "
                            "last_refresh_id TEXT, rx_message_count INTEGER, "
                            "orderbook_request_rx_count INTEGER, "
                            "last_non_orderbook_message_at INTEGER, "
                            "status TEXT);")
            self.db.execute("CREATE INDEX orderbook_sources_directory_active_key "
                            "ON orderbook_sources(directory, active, "
                            "counterparty, oid);")
            self.db.execute("CREATE INDEX orderbook_sources_directory_refresh "
                            "ON orderbook_sources(directory, active, "
                            "last_refresh_id);")
            self.db.execute("CREATE INDEX orderbook_sources_active_last_seen "
                            "ON orderbook_sources(active, last_seen_at);")
            self.db.execute("CREATE INDEX "
                            "fidelitybond_sources_directory_active_key "
                            "ON fidelitybond_sources(directory, active, "
                            "counterparty);")
            self.db.execute("CREATE INDEX fidelitybond_sources_directory_refresh "
                            "ON fidelitybond_sources(directory, active, "
                            "last_refresh_id);")
            self.db.execute("CREATE INDEX "
                            "fidelitybond_sources_active_last_seen "
                            "ON fidelitybond_sources(active, last_seen_at);")
            self.db.execute("CREATE INDEX directory_peers_status_directory "
                            "ON directory_peers(status, directory);")
            self.db.execute("CREATE VIEW visible_orderbook AS "
                            "SELECT o.counterparty, o.oid, o.ordertype, "
                            "o.minsize, o.maxsize, o.txfee, o.cjfee "
                            "FROM (SELECT os.counterparty, os.oid "
                            "FROM directory_peers AS dp "
                            "JOIN orderbook_sources AS os "
                            "ON os.directory = dp.directory "
                            "WHERE dp.status = 'connected' "
                            "AND os.active = 1 "
                            "GROUP BY os.counterparty, os.oid) "
                            "AS visible "
                            "JOIN orderbook AS o "
                            "ON o.counterparty = visible.counterparty "
                            "AND o.oid = visible.oid;")
            self.db.execute("CREATE VIEW visible_fidelitybonds AS "
                            "SELECT f.counterparty, f.takernick, f.proof "
                            "FROM (SELECT fs.counterparty "
                            "FROM directory_peers AS dp "
                            "JOIN fidelitybond_sources AS fs "
                            "ON fs.directory = dp.directory "
                            "WHERE dp.status = 'connected' "
                            "AND fs.active = 1 "
                            "GROUP BY fs.counterparty) AS visible "
                            "JOIN fidelitybonds AS f "
                            "ON f.counterparty = visible.counterparty;")
        finally:
            self.dblock.release()

    @staticmethod
    def _now():
        return int(time.time())

    def set_current_refresh_id(self, refresh_id):
        self.current_refresh_id = refresh_id

    def on_valid_orderbook_response(self, source_directory, response_type):
        pass

    @staticmethod
    def _make_initial_refresh_id():
        return "initial-" + uuid.uuid4().hex

    def _mark_visibility_cache_dirty_locked(self):
        self._visibility_cache_dirty = True

    @staticmethod
    def _copy_rows(rows):
        return [row.copy() if isinstance(row, dict) else dict(row)
                for row in rows]

    def _refresh_visibility_cache_locked(self):
        if not self._visibility_cache_dirty:
            return
        started = time.monotonic()
        self.db.execute("SELECT * FROM visible_orderbook;")
        self._visible_orderbook_cache = self._copy_rows(self.db.fetchall())
        self.db.execute("SELECT * FROM visible_fidelitybonds;")
        self._visible_fidelitybond_cache = self._copy_rows(
            self.db.fetchall())
        self._visibility_cache_dirty = False
        self._visibility_cache_updated_at = self._now()
        elapsed = time.monotonic() - started
        if elapsed >= 0.25:
            log.info("Refreshed visible orderbook cache in {:.3f}s "
                     "(offers={}, fidelitybonds={})".format(
                         elapsed, len(self._visible_orderbook_cache),
                         len(self._visible_fidelitybond_cache)))

    def _ensure_directory_peer_locked(self, directory):
        self.db.execute("SELECT directory FROM directory_peers WHERE "
                        "directory=?;", (directory,))
        if self.db.fetchone():
            return
        self.db.execute(
            ("INSERT INTO directory_peers(directory, rx_message_count, "
             "orderbook_request_rx_count) VALUES(?, 0, 0);"),
            (directory,))

    def _get_directory_peer_status_locked(self, directory):
        self.db.execute("SELECT status FROM directory_peers WHERE "
                        "directory=?;", (directory,))
        row = self.db.fetchone()
        return row["status"] if row else None

    def _set_directory_peer_fields_locked(self, directory, **fields):
        if not directory or not fields:
            return
        self._ensure_directory_peer_locked(directory)
        old_status = None
        if "status" in fields:
            old_status = self._get_directory_peer_status_locked(directory)
        keys = sorted(fields.keys())
        assignments = ", ".join([key + "=?" for key in keys])
        values = [fields[key] for key in keys] + [directory]
        self.db.execute("UPDATE directory_peers SET " + assignments +
                        " WHERE directory=?;", values)
        if "status" in fields and fields["status"] != old_status:
            self._mark_visibility_cache_dirty_locked()

    def _get_or_create_directory_refresh_id_locked(self, directory):
        self._ensure_directory_peer_locked(directory)
        self.db.execute("SELECT last_refresh_id FROM directory_peers WHERE "
                        "directory=?;", (directory,))
        row = self.db.fetchone()
        refresh_id = row["last_refresh_id"] if row else None
        if refresh_id is None:
            refresh_id = self._make_initial_refresh_id()
            self.db.execute(
                "UPDATE directory_peers SET last_refresh_id=? "
                "WHERE directory=?;",
                (refresh_id, directory))
        return refresh_id

    def record_directory_connected(self, directory):
        now = self._now()
        try:
            self.dblock.acquire(True)
            self._ensure_directory_peer_locked(directory)
            self.db.execute("SELECT first_connected_at FROM directory_peers "
                            "WHERE directory=?;", (directory,))
            row = self.db.fetchone()
            first_connected_at = row["first_connected_at"] if row else None
            fields = {"last_connected_at": now, "status": "connected"}
            if first_connected_at is None:
                fields["first_connected_at"] = now
            self._set_directory_peer_fields_locked(directory, **fields)
        finally:
            self.dblock.release()

    def record_directory_disconnected(self, directory):
        try:
            self.dblock.acquire(True)
            self._set_directory_peer_fields_locked(
                directory, last_disconnected_at=self._now(),
                status="disconnected")
        finally:
            self.dblock.release()

    def record_orderbook_request(self, directory, refresh_id=None):
        try:
            self.dblock.acquire(True)
            fields = {"last_orderbook_request_at": self._now(),
                      "status": "connected"}
            if refresh_id is not None:
                fields["last_refresh_id"] = refresh_id
            self._set_directory_peer_fields_locked(directory, **fields)
        finally:
            self.dblock.release()

    @staticmethod
    def _pubmsg_has_orderbook_request(message):
        if not message or message[0] != COMMAND_PREFIX:
            return False
        commands = message[1:].split(COMMAND_PREFIX)
        return any(command.split(" ")[0] == "orderbook"
                   for command in commands)

    def on_message_seen(self, mc, msgtype, nick, message, source_directory=None):
        if not source_directory:
            return
        self.record_directory_message(source_directory, msgtype, message)

    def record_directory_message(self, directory, msgtype, message):
        now = self._now()
        is_pubmsg = msgtype == "pubmsg"
        try:
            self.dblock.acquire(True)
            fields = {"last_message_at": now, "status": "connected"}
            has_orderbook_request = False
            if is_pubmsg:
                fields["last_pubmsg_at"] = now
                if self._pubmsg_has_orderbook_request(message):
                    has_orderbook_request = True
                    fields["last_orderbook_request_seen_at"] = now
            if not has_orderbook_request:
                fields["last_non_orderbook_message_at"] = now
            self._set_directory_peer_fields_locked(directory, **fields)
            self.db.execute(
                ("UPDATE directory_peers SET "
                 "rx_message_count=COALESCE(rx_message_count, 0) + 1 "
                 "WHERE directory=?;"), (directory,))
            if has_orderbook_request:
                self.db.execute(
                    ("UPDATE directory_peers SET "
                     "orderbook_request_rx_count="
                     "COALESCE(orderbook_request_rx_count, 0) + 1 "
                     "WHERE directory=?;"), (directory,))
        finally:
            self.dblock.release()

    def record_successful_refresh(self, directory, refresh_id=None):
        try:
            self.dblock.acquire(True)
            fields = {"last_successful_refresh_at": self._now(),
                      "status": "connected"}
            if refresh_id is not None:
                fields["last_refresh_id"] = refresh_id
            self._set_directory_peer_fields_locked(directory, **fields)
        finally:
            self.dblock.release()

    def _record_directory_response_locked(self, directory, now):
        fields = {"last_seen_at": now, "last_orderbook_response_at": now,
                  "status": "connected"}
        self._set_directory_peer_fields_locked(directory, **fields)

    def _upsert_order_source_locked(self, counterparty, oid, directory, now):
        refresh_id = self._get_or_create_directory_refresh_id_locked(directory)
        self.db.execute(
            ("SELECT first_seen_at FROM orderbook_sources WHERE "
             "counterparty=? AND oid=? AND directory=?;"),
            (counterparty, oid, directory))
        row = self.db.fetchone()
        first_seen_at = row["first_seen_at"] if row else now
        self.db.execute(
            ("INSERT OR REPLACE INTO orderbook_sources VALUES"
             "(?, ?, ?, ?, ?, ?, ?, ?);"),
            (counterparty, oid, directory, first_seen_at, now,
             refresh_id, 1, None))
        self._refresh_active_order_sources_locked(counterparty, oid, now)
        self._mark_visibility_cache_dirty_locked()

    def _refresh_active_order_sources_locked(self, counterparty, oid, now):
        self.db.execute(
            ("SELECT directory FROM orderbook_sources WHERE counterparty=? "
             "AND oid=? AND active=1;"),
            (counterparty, oid))
        directories = [row["directory"] for row in self.db.fetchall()]
        for directory in directories:
            refresh_id = self._get_or_create_directory_refresh_id_locked(
                directory)
            self.db.execute(
                ("UPDATE orderbook_sources SET last_seen_at=?, "
                 "last_refresh_id=?, inactive_at=NULL "
                 "WHERE counterparty=? AND oid=? AND directory=? "
                 "AND active=1;"),
                (now, refresh_id, counterparty, oid, directory))

    def _deactivate_order_source_locked(self, counterparty, oid, directory,
                                        now):
        self.db.execute(
            ("UPDATE orderbook_sources SET active=0, "
             "inactive_at=COALESCE(inactive_at, ?) "
             "WHERE counterparty=? AND oid=? AND directory=?;"),
            (now, counterparty, oid, directory))
        self._mark_visibility_cache_dirty_locked()

    def _deactivate_fidelitybond_source_locked(self, counterparty, directory,
                                               now):
        self.db.execute(
            ("UPDATE fidelitybond_sources SET active=0, "
             "inactive_at=COALESCE(inactive_at, ?) "
             "WHERE counterparty=? AND directory=?;"),
            (now, counterparty, directory))
        self._mark_visibility_cache_dirty_locked()

    def _has_active_order_source_locked(self, counterparty, oid):
        self.db.execute(
            ("SELECT 1 FROM orderbook_sources WHERE counterparty=? AND "
             "oid=? AND active=1 LIMIT 1;"),
            (counterparty, oid))
        return self.db.fetchone() is not None

    def _has_any_order_source_locked(self, counterparty, oid):
        self.db.execute(
            ("SELECT 1 FROM orderbook_sources WHERE counterparty=? AND "
             "oid=? LIMIT 1;"),
            (counterparty, oid))
        return self.db.fetchone() is not None

    def _has_active_fidelitybond_source_locked(self, counterparty):
        self.db.execute(
            ("SELECT 1 FROM fidelitybond_sources WHERE counterparty=? AND "
             "active=1 LIMIT 1;"),
            (counterparty,))
        return self.db.fetchone() is not None

    def _has_any_fidelitybond_source_locked(self, counterparty):
        self.db.execute(
            ("SELECT 1 FROM fidelitybond_sources WHERE counterparty=? "
             "LIMIT 1;"),
            (counterparty,))
        return self.db.fetchone() is not None

    def _latest_order_source_inactive_at_locked(self, counterparty, oid):
        self.db.execute(
            ("SELECT MAX(COALESCE(inactive_at, last_seen_at, first_seen_at, 0)) "
             "AS inactive_at FROM orderbook_sources WHERE counterparty=? "
             "AND oid=?;"),
            (counterparty, oid))
        row = self.db.fetchone()
        return row["inactive_at"] if row else None

    def _latest_fidelitybond_source_inactive_at_locked(self, counterparty):
        self.db.execute(
            ("SELECT MAX(COALESCE(inactive_at, last_seen_at, first_seen_at, 0)) "
             "AS inactive_at FROM fidelitybond_sources WHERE counterparty=?;"),
            (counterparty,))
        row = self.db.fetchone()
        return row["inactive_at"] if row else None

    @staticmethod
    def _should_prune_inactive(now, inactive_at, grace_seconds):
        if grace_seconds < 0:
            return True
        if inactive_at is None:
            return grace_seconds == 0
        return now - int(inactive_at) >= grace_seconds

    def _prune_order_if_no_active_source_locked(self, counterparty, oid, now,
                                                grace_seconds):
        if not self._has_any_order_source_locked(counterparty, oid):
            return
        if self._has_active_order_source_locked(counterparty, oid):
            return
        inactive_at = self._latest_order_source_inactive_at_locked(
            counterparty, oid)
        if self._should_prune_inactive(now, inactive_at, grace_seconds):
            self.db.execute(
                ("DELETE FROM orderbook WHERE counterparty=? AND oid=?;"),
                (counterparty, oid))
            self._mark_visibility_cache_dirty_locked()

    def _prune_fidelitybond_if_no_active_source_locked(self, counterparty, now,
                                                       grace_seconds):
        if not self._has_any_fidelitybond_source_locked(counterparty):
            return
        if self._has_active_fidelitybond_source_locked(counterparty):
            return
        inactive_at = self._latest_fidelitybond_source_inactive_at_locked(
            counterparty)
        if self._should_prune_inactive(now, inactive_at, grace_seconds):
            self.db.execute("DELETE FROM fidelitybonds WHERE counterparty=?;",
                            (counterparty,))
            self._mark_visibility_cache_dirty_locked()

    def _delete_order_if_no_sources_locked(self, counterparty, oid):
        self.db.execute(
            ("SELECT 1 FROM orderbook_sources WHERE counterparty=? AND oid=? "
             "LIMIT 1;"),
            (counterparty, oid))
        if self.db.fetchone() is None:
            self.db.execute(
                ("DELETE FROM orderbook WHERE counterparty=? AND oid=?;"),
                (counterparty, oid))
            self._mark_visibility_cache_dirty_locked()

    def _delete_fidelitybond_if_no_sources_locked(self, counterparty):
        self.db.execute(
            ("SELECT 1 FROM fidelitybond_sources WHERE counterparty=? "
             "LIMIT 1;"),
            (counterparty,))
        if self.db.fetchone() is None:
            self.db.execute("DELETE FROM fidelitybonds WHERE counterparty=?;",
                            (counterparty,))
            self._mark_visibility_cache_dirty_locked()

    def _prune_orphan_order_sources_locked(self, now, retention_seconds):
        if retention_seconds < 0:
            return
        self.db.execute(
            ("DELETE FROM orderbook_sources WHERE NOT EXISTS ("
             "SELECT 1 FROM orderbook WHERE "
             "orderbook.counterparty=orderbook_sources.counterparty AND "
             "orderbook.oid=orderbook_sources.oid) AND "
             "(?=0 OR ? - COALESCE(inactive_at, last_seen_at, first_seen_at, 0)"
             " >= ?);"),
            (retention_seconds, now, retention_seconds))

    def _prune_orphan_fidelitybond_sources_locked(self, now,
                                                  retention_seconds):
        if retention_seconds < 0:
            return
        self.db.execute(
            ("DELETE FROM fidelitybond_sources WHERE NOT EXISTS ("
             "SELECT 1 FROM fidelitybonds WHERE "
             "fidelitybonds.counterparty="
             "fidelitybond_sources.counterparty) AND "
             "(?=0 OR ? - COALESCE(inactive_at, last_seen_at, first_seen_at, 0)"
             " >= ?);"),
            (retention_seconds, now, retention_seconds))

    def _prune_sources_locked(self, now=None, inactive_grace_seconds=None,
                              orphan_retention_seconds=None):
        now = self._now() if now is None else now
        inactive_grace_seconds = (
            self.source_inactive_grace_seconds
            if inactive_grace_seconds is None else inactive_grace_seconds)
        orphan_retention_seconds = (
            self.orphan_source_retention_seconds
            if orphan_retention_seconds is None else orphan_retention_seconds)

        self.db.execute("SELECT counterparty, oid FROM orderbook;")
        for row in self.db.fetchall():
            self._prune_order_if_no_active_source_locked(
                row["counterparty"], row["oid"], now,
                inactive_grace_seconds)

        self.db.execute("SELECT counterparty FROM fidelitybonds;")
        for row in self.db.fetchall():
            self._prune_fidelitybond_if_no_active_source_locked(
                row["counterparty"], now, inactive_grace_seconds)

        self._prune_orphan_order_sources_locked(now, orphan_retention_seconds)
        self._prune_orphan_fidelitybond_sources_locked(
            now, orphan_retention_seconds)

    def prune_sources(self, now=None, inactive_grace_seconds=None,
                      orphan_retention_seconds=None):
        try:
            self.dblock.acquire(True)
            self._prune_sources_locked(now, inactive_grace_seconds,
                                       orphan_retention_seconds)
        finally:
            self.dblock.release()

    def has_active_order_source(self, counterparty, oid):
        try:
            self.dblock.acquire(True)
            return self._has_active_order_source_locked(counterparty, oid)
        finally:
            self.dblock.release()

    def has_active_fidelitybond_source(self, counterparty):
        try:
            self.dblock.acquire(True)
            return self._has_active_fidelitybond_source_locked(counterparty)
        finally:
            self.dblock.release()

    def _restore_order_from_sources_locked(self, counterparty, oid):
        # Compatibility no-op for older tests/call paths: source rows no longer
        # carry payload, so the global table remains the payload owner.
        self._prune_order_if_no_active_source_locked(
            counterparty, oid, self._now(), self.source_inactive_grace_seconds)

    def _restore_fidelitybond_from_sources_locked(self, counterparty):
        # Compatibility no-op for older tests/call paths; see
        # _restore_order_from_sources_locked.
        self._prune_fidelitybond_if_no_active_source_locked(
            counterparty, self._now(), self.source_inactive_grace_seconds)

    def _upsert_fidelitybond_source_locked(self, counterparty, directory,
                                           now):
        refresh_id = self._get_or_create_directory_refresh_id_locked(directory)
        self.db.execute(
            ("SELECT first_seen_at FROM fidelitybond_sources WHERE "
             "counterparty=? AND directory=?;"),
            (counterparty, directory))
        row = self.db.fetchone()
        first_seen_at = row["first_seen_at"] if row else now
        self.db.execute(
            ("INSERT OR REPLACE INTO fidelitybond_sources VALUES"
             "(?, ?, ?, ?, ?, ?, ?);"),
            (counterparty, directory, first_seen_at, now,
             refresh_id, 1, None))
        self._refresh_active_fidelitybond_sources_locked(counterparty, now)
        self._mark_visibility_cache_dirty_locked()

    def _refresh_active_fidelitybond_sources_locked(self, counterparty, now):
        self.db.execute(
            ("SELECT directory FROM fidelitybond_sources WHERE "
             "counterparty=? AND active=1;"),
            (counterparty,))
        directories = [row["directory"] for row in self.db.fetchall()]
        for directory in directories:
            refresh_id = self._get_or_create_directory_refresh_id_locked(
                directory)
            self.db.execute(
                ("UPDATE fidelitybond_sources SET last_seen_at=?, "
                 "last_refresh_id=?, inactive_at=NULL "
                 "WHERE counterparty=? AND directory=? AND active=1;"),
                (now, refresh_id, counterparty, directory))

    def prune_unseen_sources_for_directories(self, directories, refresh_id):
        directories = [d for d in directories if d]
        if not directories:
            return
        log.debug("Ignoring refresh-id source prune for directories {} "
                  "and refresh id {}; TTL prune is authoritative.".format(
                      ",".join(directories), refresh_id))
        self.prune_sources()

    def prune_stale_sources(self, offer_ttl_seconds,
                            fidelitybond_ttl_seconds, now=None):
        now = self._now() if now is None else now
        try:
            self.dblock.acquire(True)
            dirty = False
            if offer_ttl_seconds > 0:
                self.db.execute(
                    ("UPDATE orderbook_sources SET active=0, "
                     "inactive_at=COALESCE(inactive_at, ?) "
                     "WHERE active=1 AND last_seen_at < ?;"),
                    (now, now - int(offer_ttl_seconds)))
                dirty = dirty or self.db.rowcount > 0
            if fidelitybond_ttl_seconds > 0:
                self.db.execute(
                    ("UPDATE fidelitybond_sources SET active=0, "
                     "inactive_at=COALESCE(inactive_at, ?) "
                     "WHERE active=1 AND last_seen_at < ?;"),
                    (now, now - int(fidelitybond_ttl_seconds)))
                dirty = dirty or self.db.rowcount > 0
            self._prune_sources_locked(now)
            if dirty:
                self._mark_visibility_cache_dirty_locked()
        finally:
            self.dblock.release()

    def get_directory_peer_rows(self):
        try:
            self.dblock.acquire(True)
            self.db.execute("SELECT * FROM directory_peers ORDER BY directory;")
            return self.db.fetchall()
        finally:
            self.dblock.release()

    def get_orderbook_source_rows(self):
        try:
            self.dblock.acquire(True)
            self.db.execute("SELECT * FROM orderbook_sources ORDER BY "
                            "directory, counterparty, oid;")
            return self.db.fetchall()
        finally:
            self.dblock.release()

    def get_fidelitybond_source_rows(self):
        try:
            self.dblock.acquire(True)
            self.db.execute("SELECT * FROM fidelitybond_sources ORDER BY "
                            "directory, counterparty;")
            return self.db.fetchall()
        finally:
            self.dblock.release()

    def get_visible_orderbook_rows(self):
        try:
            self.dblock.acquire(True)
            self._refresh_visibility_cache_locked()
            return self._copy_rows(self._visible_orderbook_cache)
        finally:
            self.dblock.release()

    def get_visible_fidelitybond_rows(self):
        try:
            self.dblock.acquire(True)
            self._refresh_visibility_cache_locked()
            return self._copy_rows(self._visible_fidelitybond_cache)
        finally:
            self.dblock.release()

    def get_raw_orderbook_rows(self):
        try:
            self.dblock.acquire(True)
            self.db.execute("SELECT * FROM orderbook;")
            return self.db.fetchall()
        finally:
            self.dblock.release()

    def get_raw_fidelitybond_rows(self):
        try:
            self.dblock.acquire(True)
            self.db.execute("SELECT * FROM fidelitybonds;")
            return self.db.fetchall()
        finally:
            self.dblock.release()

    def get_directory_runtime_liquidity(self, connected_directories=None):
        connected_directories = set(connected_directories or [])
        result = {
            directory: {
                "directory": directory,
                "orderbook_size": 0,
                "fidelitybond_size": 0,
            }
            for directory in connected_directories
        }
        try:
            self.dblock.acquire(True)
            self.db.execute(
                "SELECT directory, COUNT(*) AS size FROM orderbook_sources "
                "WHERE active=1 GROUP BY directory;")
            for row in self.db.fetchall():
                directory = row["directory"]
                if connected_directories and directory not in result:
                    continue
                result.setdefault(directory, {
                    "directory": directory,
                    "orderbook_size": 0,
                    "fidelitybond_size": 0,
                })["orderbook_size"] = row["size"]
            self.db.execute(
                "SELECT directory, COUNT(*) AS size "
                "FROM fidelitybond_sources WHERE active=1 "
                "GROUP BY directory;")
            for row in self.db.fetchall():
                directory = row["directory"]
                if connected_directories and directory not in result:
                    continue
                result.setdefault(directory, {
                    "directory": directory,
                    "orderbook_size": 0,
                    "fidelitybond_size": 0,
                })["fidelitybond_size"] = row["size"]
            return list(result.values())
        finally:
            self.dblock.release()

    @staticmethod
    def on_set_topic(newtopic):
        chunks = newtopic.split('|')
        for msg in chunks[1:]:
            try:
                msg = msg.strip()
                params = msg.split(' ')
                min_version = int(params[0])
                max_version = int(params[1])
                alert = msg[msg.index(params[1]) + len(params[1]):].strip()
            except (ValueError, IndexError):
                continue
            if min_version < JM_VERSION < max_version:
                print('=' * 60)
                print('JOINMARKET ALERT')
                print(alert)
                print('=' * 60)
                joinmarket_alert[0] = alert

    def on_order_seen(self, counterparty, oid, ordertype, minsize, maxsize,
                      txfee, cjfee, source_directory=None):
        valid_source_directory = None
        try:
            self.dblock.acquire(True)
            if int(oid) < 0 or int(oid) > sys.maxsize:
                log.debug("Got invalid order ID: " + oid + " from " +
                          counterparty)
                return
            now = self._now()
            # delete orders eagerly, so in case a buggy maker sends an
            # invalid offer, we won't accidentally !fill based on the ghost
            # of its previous message.
            if source_directory:
                self._deactivate_order_source_locked(
                    counterparty, oid, source_directory, now)
                self._prune_order_if_no_active_source_locked(
                    counterparty, oid, now, self.source_inactive_grace_seconds)
                self._prune_orphan_order_sources_locked(
                    now, self.orphan_source_retention_seconds)
            else:
                self.db.execute(
                    ("DELETE FROM orderbook WHERE counterparty=? "
                     "AND oid=?;"), (counterparty, oid))
            # now validate the remaining fields
            if int(minsize) < 0 or int(minsize) > 21 * 10**14:
                log.debug("Got invalid minsize: {} from {}".format(
                    minsize, counterparty))
                return
            if int(minsize) < self.dust_threshold:
                minsize = self.dust_threshold
                log.debug("{} has dusty minsize, capping at {}".format(
                    counterparty, minsize))
                # do not pass return, go not drop this otherwise fine offer
            if int(maxsize) < 0 or int(maxsize) > 21 * 10**14:
                log.debug("Got invalid maxsize: " + maxsize + " from " +
                          counterparty)
                return
            if int(txfee) < 0:
                log.debug("Got invalid txfee: {} from {}".format(txfee,
                                                                 counterparty))
                return
            if int(minsize) > int(maxsize):

                fmt = ("Got minsize bigger than maxsize: {} - {} "
                       "from {}").format
                log.debug(fmt(minsize, maxsize, counterparty))
                return
            if ordertype in ['sw0absoffer', 'swabsoffer', 'absoffer']\
                    and not isinstance(cjfee, Integral):
                try:
                    cjfee = int(cjfee)
                except ValueError:
                    log.debug("Got non integer coinjoin fee: " + str(cjfee) +
                              " for an absoffer from " + counterparty)
                    return
            cjfee = str(Decimal(cjfee))
            if source_directory:
                self._record_directory_response_locked(source_directory, now)
                self._upsert_order_source_locked(
                    counterparty, oid, source_directory, now)
            self.db.execute(
                'INSERT OR REPLACE INTO orderbook VALUES(?, ?, ?, ?, ?, ?, ?);',
                (counterparty, oid, ordertype, minsize, maxsize, txfee,
                 cjfee))  # any parseable Decimal is a valid cjfee
            self._mark_visibility_cache_dirty_locked()
            valid_source_directory = source_directory
        except InvalidOperation:
            log.debug("Got invalid cjfee: " + str(cjfee) + " from " + counterparty)
        except Exception as e:
            log.debug("Error parsing order " + str(oid) + " from " + counterparty)
            log.debug("Exception was: " + repr(e))
        finally:
            self.dblock.release()
        if valid_source_directory:
            try:
                self.on_valid_orderbook_response(
                    valid_source_directory, "offer")
            except Exception as e:
                log.warning("Orderbook response hook failed for {}: {}".format(
                    valid_source_directory, repr(e)))

    def on_order_cancel(self, counterparty, oid, source_directory=None):
        try:
            self.dblock.acquire(True)
            if source_directory:
                now = self._now()
                self._deactivate_order_source_locked(
                    counterparty, oid, source_directory, now)
                self._prune_sources_locked(now)
                return
            self.db.execute(
                ("DELETE FROM orderbook_sources WHERE counterparty=? "
                 "AND oid=?;"), (counterparty, oid))
            self.db.execute(
                ("DELETE FROM orderbook WHERE "
                 "counterparty=? AND oid=?;"), (counterparty, oid))
            self._mark_visibility_cache_dirty_locked()
        finally:
            self.dblock.release()

    def on_fidelity_bond_seen(self, nick, bond_type, fidelity_bond_proof_msg,
                              source_directory=None):
        taker_nick = self.msgchan.nick
        maker_nick = nick
        if not fidelity_bond_sanity_check.fidelity_bond_sanity_check(fidelity_bond_proof_msg):
            log.debug("Failed to verify fidelity bond for {}, skipping."
                      .format(maker_nick))
            return
        valid_source_directory = None
        try:
            self.dblock.acquire(True)
            if source_directory:
                now = self._now()
                self._record_directory_response_locked(source_directory, now)
                self._upsert_fidelitybond_source_locked(
                    nick, source_directory, now)
            self.db.execute("INSERT OR REPLACE INTO fidelitybonds VALUES(?, ?, ?);",
                (nick, taker_nick, fidelity_bond_proof_msg))
            self._mark_visibility_cache_dirty_locked()
            valid_source_directory = source_directory
        finally:
            self.dblock.release()
        if valid_source_directory:
            try:
                self.on_valid_orderbook_response(
                    valid_source_directory, "fidelitybond")
            except Exception as e:
                log.warning("Fidelity-bond response hook failed for {}: {}"
                            .format(valid_source_directory, repr(e)))

    def on_nick_leave(self, nick, source_directory=None):
        try:
            self.dblock.acquire(True)
            if source_directory:
                now = self._now()
                self.db.execute(
                    ("UPDATE orderbook_sources SET active=0, "
                     "inactive_at=COALESCE(inactive_at, ?) "
                     "WHERE counterparty=? AND directory=? AND active=1;"),
                    (now, nick, source_directory))
                self.db.execute(
                    ("UPDATE fidelitybond_sources SET active=0, "
                     "inactive_at=COALESCE(inactive_at, ?) "
                     "WHERE counterparty=? AND directory=? AND active=1;"),
                    (now, nick, source_directory))
                self._prune_sources_locked(now)
                self._mark_visibility_cache_dirty_locked()
                return
            self.db.execute('DELETE FROM orderbook_sources WHERE '
                            'counterparty=?;', (nick,))
            self.db.execute('DELETE FROM fidelitybond_sources WHERE '
                            'counterparty=?;', (nick,))
            self.db.execute('DELETE FROM orderbook WHERE counterparty=?;',
                            (nick,))
            self.db.execute('DELETE FROM fidelitybonds WHERE counterparty=?;',
                            (nick,))
            self._mark_visibility_cache_dirty_locked()
        finally:
            self.dblock.release()

    def on_disconnect(self):
        try:
            self.dblock.acquire(True)
            self.db.execute('DELETE FROM orderbook;')
            self.db.execute('DELETE FROM fidelitybonds;')
            self.db.execute('DELETE FROM orderbook_sources;')
            self.db.execute('DELETE FROM fidelitybond_sources;')
            self._mark_visibility_cache_dirty_locked()
        finally:
            self.dblock.release()
