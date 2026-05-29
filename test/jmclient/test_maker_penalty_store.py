from configparser import ConfigParser

from jmclient import jm_single
from jmclient.maker_penalty_store import MakerPenaltyStore


def configure_store(tmp_path, **overrides):
    old_config = jm_single().config
    old_datadir = jm_single().datadir
    config = ConfigParser(strict=False)
    config.add_section("POLICY")
    values = {
        "taker_stage2_maker_cooldown_seconds": "3600",
        "taker_persistent_maker_ban_list": "",
        "taker_persistent_maker_ban_file": "",
        "taker_maker_penalty_db": str(tmp_path / "penalties.sqlite3"),
        "taker_maker_cooldown_ban_threshold": "3",
        "taker_maker_cooldown_ban_window_seconds": "604800",
        "taker_maker_auto_ban_seconds": "0",
    }
    values.update({k: str(v) for k, v in overrides.items()})
    for key, value in values.items():
        config.set("POLICY", key, value)
    jm_single().config = config
    jm_single().datadir = str(tmp_path)
    return old_config, old_datadir


def restore_config(old_config, old_datadir):
    jm_single().config = old_config
    jm_single().datadir = old_datadir


def test_stage2_cooldown_persists_across_store_instances(tmp_path):
    old_config, old_datadir = configure_store(tmp_path)
    try:
        store = MakerPenaltyStore()
        store.record_stage2_cooldown(["maker-a"], "attempt-1", now=10)
        assert store.active_cooldowns(now=11) == ["maker-a"]
        store.close()

        reopened = MakerPenaltyStore()
        assert reopened.active_cooldowns(now=11) == ["maker-a"]
        reopened.close()
    finally:
        restore_config(old_config, old_datadir)


def test_expired_cooldown_is_pruned(tmp_path):
    old_config, old_datadir = configure_store(
        tmp_path, taker_stage2_maker_cooldown_seconds="10")
    try:
        store = MakerPenaltyStore()
        store.record_stage2_cooldown(["maker-a"], "attempt-1", now=10)
        assert store.active_cooldowns(now=19) == ["maker-a"]
        store.prune(now=20)
        assert store.active_cooldowns(now=20) == []
        store.close()
    finally:
        restore_config(old_config, old_datadir)


def test_repeated_cooldowns_promote_to_ban(tmp_path):
    old_config, old_datadir = configure_store(
        tmp_path, taker_maker_cooldown_ban_threshold="2")
    try:
        store = MakerPenaltyStore()
        store.record_stage2_cooldown(["maker-a"], "attempt-1", now=10)
        assert store.active_db_bans(now=11) == []

        store.record_stage2_cooldown(["maker-a"], "attempt-2", now=20)
        assert store.active_db_bans(now=21) == ["maker-a"]
        store.close()
    finally:
        restore_config(old_config, old_datadir)


def test_cooldown_ban_window_resets_count(tmp_path):
    old_config, old_datadir = configure_store(
        tmp_path,
        taker_maker_cooldown_ban_threshold="2",
        taker_maker_cooldown_ban_window_seconds="5",
    )
    try:
        store = MakerPenaltyStore()
        store.record_stage2_cooldown(["maker-a"], "attempt-1", now=10)
        store.record_stage2_cooldown(["maker-a"], "attempt-2", now=20)
        assert store.active_db_bans(now=21) == []
        store.close()
    finally:
        restore_config(old_config, old_datadir)


def test_configured_bans_combine_inline_file_and_db(tmp_path):
    ban_file = tmp_path / "banned-makers.txt"
    ban_file.write_text("maker-file\n# comment\n\n", encoding="utf-8")
    old_config, old_datadir = configure_store(
        tmp_path,
        taker_persistent_maker_ban_list="maker-inline, maker-inline-2",
        taker_persistent_maker_ban_file=str(ban_file),
    )
    try:
        store = MakerPenaltyStore()
        store.ban("maker-db", reason="test", now=10)

        assert store.active_hard_bans(now=11) == [
            "maker-db",
            "maker-file",
            "maker-inline",
            "maker-inline-2",
        ]
        store.close()
    finally:
        restore_config(old_config, old_datadir)
