#! /usr/bin/env python

import sqlite3
import sys
import threading
import time
from decimal import InvalidOperation, Decimal
from numbers import Integral

from jmdaemon.protocol import COMMAND_PREFIX, JM_VERSION
from jmdaemon import fidelity_bond_sanity_check
from jmbase.support import dict_factory, get_log, joinmarket_alert
log = get_log()


class JMTakerError(Exception):
    pass

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
        try:
            self.dblock.acquire(True)
            self.db.execute("CREATE TABLE orderbook(counterparty TEXT, "
                            "oid INTEGER, ordertype TEXT, minsize INTEGER, "
                            "maxsize INTEGER, txfee INTEGER, cjfee TEXT);")
            self.db.execute("CREATE TABLE fidelitybonds(counterparty TEXT, "
                "takernick TEXT, proof TEXT);");
            self.db.execute("CREATE TABLE orderbook_sources("
                            "counterparty TEXT, oid INTEGER, directory TEXT, "
                            "first_seen_at INTEGER, last_seen_at INTEGER, "
                            "last_refresh_id TEXT, active INTEGER DEFAULT 1, "
                            "inactive_at INTEGER, "
                            "PRIMARY KEY(counterparty, oid, directory));")
            self.db.execute("CREATE TABLE fidelitybond_sources("
                            "counterparty TEXT, directory TEXT, "
                            "first_seen_at INTEGER, last_seen_at INTEGER, "
                            "last_refresh_id TEXT, active INTEGER DEFAULT 1, "
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
        finally:
            self.dblock.release()

    @staticmethod
    def _now():
        return int(time.time())

    def set_current_refresh_id(self, refresh_id):
        self.current_refresh_id = refresh_id

    def _ensure_directory_peer_locked(self, directory):
        self.db.execute("SELECT directory FROM directory_peers WHERE "
                        "directory=?;", (directory,))
        if self.db.fetchone():
            return
        self.db.execute(
            ("INSERT INTO directory_peers(directory, rx_message_count, "
             "orderbook_request_rx_count) VALUES(?, 0, 0);"),
            (directory,))

    def _set_directory_peer_fields_locked(self, directory, **fields):
        if not directory or not fields:
            return
        self._ensure_directory_peer_locked(directory)
        keys = sorted(fields.keys())
        assignments = ", ".join([key + "=?" for key in keys])
        values = [fields[key] for key in keys] + [directory]
        self.db.execute("UPDATE directory_peers SET " + assignments +
                        " WHERE directory=?;", values)

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
        if self.current_refresh_id is not None:
            fields["last_refresh_id"] = self.current_refresh_id
        self._set_directory_peer_fields_locked(directory, **fields)

    def _upsert_order_source_locked(self, counterparty, oid, directory, now):
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
             self.current_refresh_id, 1, None))

    def _deactivate_order_source_locked(self, counterparty, oid, directory,
                                        now):
        self.db.execute(
            ("UPDATE orderbook_sources SET active=0, "
             "inactive_at=COALESCE(inactive_at, ?) "
             "WHERE counterparty=? AND oid=? AND directory=?;"),
            (now, counterparty, oid, directory))

    def _deactivate_fidelitybond_source_locked(self, counterparty, directory,
                                               now):
        self.db.execute(
            ("UPDATE fidelitybond_sources SET active=0, "
             "inactive_at=COALESCE(inactive_at, ?) "
             "WHERE counterparty=? AND directory=?;"),
            (now, counterparty, directory))

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

    def _delete_order_if_no_sources_locked(self, counterparty, oid):
        self.db.execute(
            ("SELECT 1 FROM orderbook_sources WHERE counterparty=? AND oid=? "
             "LIMIT 1;"),
            (counterparty, oid))
        if self.db.fetchone() is None:
            self.db.execute(
                ("DELETE FROM orderbook WHERE counterparty=? AND oid=?;"),
                (counterparty, oid))

    def _delete_fidelitybond_if_no_sources_locked(self, counterparty):
        self.db.execute(
            ("SELECT 1 FROM fidelitybond_sources WHERE counterparty=? "
             "LIMIT 1;"),
            (counterparty,))
        if self.db.fetchone() is None:
            self.db.execute("DELETE FROM fidelitybonds WHERE counterparty=?;",
                            (counterparty,))

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
             self.current_refresh_id, 1, None))

    def prune_unseen_sources_for_directories(self, directories, refresh_id):
        directories = [d for d in directories if d]
        if not directories:
            return
        now = self._now()
        try:
            self.dblock.acquire(True)
            for directory in directories:
                self.db.execute(
                    ("UPDATE orderbook_sources SET active=0, "
                     "inactive_at=COALESCE(inactive_at, ?) "
                     "WHERE directory=? AND active=1 AND "
                     "(last_refresh_id IS NULL OR last_refresh_id != ?);"),
                    (now, directory, refresh_id))
                self.db.execute(
                    ("UPDATE fidelitybond_sources SET active=0, "
                     "inactive_at=COALESCE(inactive_at, ?) "
                     "WHERE directory=? AND active=1 AND "
                     "(last_refresh_id IS NULL OR last_refresh_id != ?);"),
                    (now, directory, refresh_id))
            self._prune_sources_locked(now)
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
                ("DELETE FROM orderbook WHERE counterparty=? "
                 "AND oid=?;"), (counterparty, oid))
            self.db.execute(
                'INSERT INTO orderbook VALUES(?, ?, ?, ?, ?, ?, ?);',
                (counterparty, oid, ordertype, minsize, maxsize, txfee,
                 cjfee))  # any parseable Decimal is a valid cjfee
        except InvalidOperation:
            log.debug("Got invalid cjfee: " + str(cjfee) + " from " + counterparty)
        except Exception as e:
            log.debug("Error parsing order " + str(oid) + " from " + counterparty)
            log.debug("Exception was: " + repr(e))
        finally:
            self.dblock.release()

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
        try:
            self.dblock.acquire(True)
            if source_directory:
                now = self._now()
                self._record_directory_response_locked(source_directory, now)
                self._upsert_fidelitybond_source_locked(
                    nick, source_directory, now)
            self.db.execute("DELETE FROM fidelitybonds WHERE counterparty=?;",
                            (nick, ))
            self.db.execute("INSERT INTO fidelitybonds VALUES(?, ?, ?);",
                (nick, taker_nick, fidelity_bond_proof_msg))
        finally:
            self.dblock.release()

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
                return
            self.db.execute('DELETE FROM orderbook_sources WHERE '
                            'counterparty=?;', (nick,))
            self.db.execute('DELETE FROM fidelitybond_sources WHERE '
                            'counterparty=?;', (nick,))
            self.db.execute('DELETE FROM orderbook WHERE counterparty=?;',
                            (nick,))
            self.db.execute('DELETE FROM fidelitybonds WHERE counterparty=?;',
                            (nick,))
        finally:
            self.dblock.release()

    def on_disconnect(self):
        try:
            self.dblock.acquire(True)
            self.db.execute('DELETE FROM orderbook;')
            self.db.execute('DELETE FROM fidelitybonds;')
            self.db.execute('DELETE FROM orderbook_sources;')
            self.db.execute('DELETE FROM fidelitybond_sources;')
        finally:
            self.dblock.release()
