"""Unit tests for reconciliation persistence and flag semantics."""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from web.app.recon_store import (
    CONSISTENT,
    CONCLUSION_CONFLICT,
    DEVICE_CONFLICT,
    MISSING,
    RUN_DONE_DIFF,
    RUN_DONE_OK,
    RUN_DONE_UNVERIFIED,
    UNVERIFIED,
    ReconciliationStore,
)
from web.app.store import Store


def deadline_in(seconds: float = 60) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


@pytest.fixture()
def stores(tmp_path):
    cmd = Store(tmp_path / "ops.db")
    recon = ReconciliationStore(tmp_path / "ops.db")
    cmd.init()
    recon.init()
    return cmd, recon


def _confirmed(store: Store, device: str, cid: str, state: str = "executed"):
    store.submit(device_id=device, command_id=cid, deadline=deadline_in())
    store.claim_batch(retry_seconds=0)
    receipt = "succeeded" if state == "executed" else "failed"
    assert store.finish(cid, state=state, receipt_status=receipt)


def test_snapshot_only_contains_confirmed_commands(stores):
    cmd, recon = stores
    _confirmed(cmd, "d1", "c1")
    _confirmed(cmd, "d1", "c2", state="failed")
    cmd.submit(device_id="d1", command_id="c3", deadline=deadline_in())  # pending
    cmd.claim_batch(retry_seconds=0)  # c3 -> inflight
    cmd.submit(device_id="d2", command_id="c4", deadline=deadline_in())

    snapshot = cmd.confirmed_for_device("d1")
    assert [r["command_id"] for r in snapshot] == ["c1", "c2"]
    run_id = recon.create_run("d1", snapshot)
    items = recon.list_items(run_id)
    assert [i["command_id"] for i in items] == ["c1", "c2"]
    assert all(i["result"] == "pending" for i in items)


def test_consistent_run_does_not_flag_device(stores):
    cmd, recon = stores
    _confirmed(cmd, "d1", "c1")
    run_id = recon.create_run("d1", cmd.confirmed_for_device("d1"))
    item = recon.next_pending_item()
    recon.record_item_result(run_id, item["command_id"], result=CONSISTENT, detail="ok")
    status = recon.finish_run_if_complete(run_id)
    assert status == RUN_DONE_OK
    assert recon.is_flagged("d1") is False


def test_missing_and_conflict_results_flag_device_persistently(stores):
    cmd, recon = stores
    _confirmed(cmd, "d1", "c1")
    _confirmed(cmd, "d1", "c2")
    _confirmed(cmd, "d1", "c3")
    run_id = recon.create_run("d1", cmd.confirmed_for_device("d1"))

    expected = {
        "c1": MISSING,
        "c2": DEVICE_CONFLICT,
        "c3": CONCLUSION_CONFLICT,
    }
    while (item := recon.next_pending_item()) is not None:
        recon.record_item_result(
            run_id, item["command_id"], result=expected[item["command_id"]],
            detail="diff",
        )
    assert recon.finish_run_if_complete(run_id) == RUN_DONE_DIFF
    assert recon.is_flagged("d1") is True
    flags = recon.flagged_devices()
    assert len(flags) == 1 and flags[0]["device_id"] == "d1"
    assert flags[0]["created_run"] == run_id

    # Flagging twice keeps the original flag.
    assert recon.flag_device("d1", "again") is False
    assert len(recon.flagged_devices()) == 1


def test_unverified_run_is_not_ok_and_does_not_flag(stores):
    cmd, recon = stores
    _confirmed(cmd, "d1", "c1")
    _confirmed(cmd, "d1", "c2")
    run_id = recon.create_run("d1", cmd.confirmed_for_device("d1"))

    first = recon.next_pending_item()
    recon.record_item_result(run_id, first["command_id"], result=CONSISTENT, detail="ok")
    second = recon.next_pending_item()
    recon.record_item_result(
        run_id, second["command_id"], result=UNVERIFIED, detail="gateway down"
    )
    assert recon.finish_run_if_complete(run_id) == RUN_DONE_UNVERIFIED
    assert recon.is_flagged("d1") is False
    run = recon.get_run(run_id)
    assert run["unverified_count"] == 1 and run["diff_count"] == 0


def test_empty_snapshot_run_closes_as_ok(stores):
    cmd, recon = stores
    run_id = recon.create_run("d-empty", [])
    assert recon.get_run(run_id)["snapshot_count"] == 0
    assert recon.finish_run_if_complete(run_id) == RUN_DONE_OK
    assert recon.is_flagged("d-empty") is False


def test_active_run_guard(stores):
    cmd, recon = stores
    _confirmed(cmd, "d1", "c1")
    from web.app.reconciler import Reconciler

    worker = Reconciler(cmd, recon, "http://gateway", tick_seconds=60)
    run_id, count = worker.request_run("d1")
    assert count == 1
    assert recon.has_active_run("d1") is True
    with pytest.raises(RuntimeError):
        worker.request_run("d1")
    # A different device can still start its own run.
    other_id, other_count = worker.request_run("d2")
    assert other_id != run_id and other_count == 0
    assert recon.get_run(other_id)["device_id"] == "d2"


def test_findings_and_flags_survive_reopen(tmp_path):
    db = tmp_path / "ops.db"
    cmd = Store(db)
    recon = ReconciliationStore(db)
    cmd.init()
    recon.init()
    _confirmed(cmd, "d1", "c1")
    run_id = recon.create_run("d1", cmd.confirmed_for_device("d1"))
    item = recon.next_pending_item()
    recon.record_item_result(run_id, item["command_id"], result=MISSING, detail="gone")
    assert recon.finish_run_if_complete(run_id) == RUN_DONE_DIFF

    # Reopen both stores against the same file, as a process restart would.
    cmd2 = Store(db)
    recon2 = ReconciliationStore(db)
    cmd2.init()
    recon2.init()
    assert recon2.is_flagged("d1") is True
    run = recon2.get_run(run_id)
    assert run["status"] == RUN_DONE_DIFF and run["diff_count"] == 1
    items = recon2.list_items(run_id)
    assert items[0]["result"] == MISSING
    assert cmd2.get("c1")["state"] == "executed"


def test_flagged_device_is_skipped_by_claim_but_other_devices_run(stores):
    cmd, recon = stores
    recon.flag_device("d1", "reconciliation found a missing record")
    cmd.submit(device_id="d1", command_id="a1", deadline=deadline_in())
    cmd.submit(device_id="d1", command_id="a2", deadline=deadline_in())
    cmd.submit(device_id="d2", command_id="b1", deadline=deadline_in())

    flagged = {r["device_id"] for r in recon.flagged_devices()}
    claimed = cmd.claim_batch(retry_seconds=0, skip_devices=flagged)
    assert [r["command_id"] for r in claimed] == ["b1"]
    # Second pass: d1 still blocked, nothing new.
    assert cmd.claim_batch(retry_seconds=0, skip_devices=flagged) == []
    assert cmd.get("a1")["state"] == "pending"
    assert cmd.get("a2")["state"] == "pending"

    # Without the skip set (flag cleared), the device drains normally again.
    assert recon.clear_flag("d1", "manual acknowledgement")
    claimed = cmd.claim_batch(retry_seconds=0, skip_devices=set())
    assert [r["command_id"] for r in claimed] == ["a1"]


def test_new_confirmations_after_snapshot_are_counted_not_checked(stores):
    cmd, recon = stores
    _confirmed(cmd, "d1", "c1")
    run_id = recon.create_run("d1", cmd.confirmed_for_device("d1"))
    run = recon.get_run(run_id)
    time.sleep(0.01)
    # A command confirmed after the snapshot was taken.
    _confirmed(cmd, "d1", "c2")
    assert recon.count_confirmed_since("d1", run["created_at"]) == 1
    # The run itself still only covers c1.
    assert [i["command_id"] for i in recon.list_items(run_id)] == ["c1"]
