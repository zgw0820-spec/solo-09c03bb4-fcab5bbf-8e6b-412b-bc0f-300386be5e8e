from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest

from web.app.store import CommandIdConflict, Store


@pytest.fixture()
def store(tmp_path):
    s = Store(tmp_path / "ops.db")
    s.init()
    return s


def deadline_in(seconds: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def test_duplicate_submit_returns_original(store):
    row1, created1 = store.submit(
        device_id="d1", command_id="c1", deadline=deadline_in(60)
    )
    assert created1 is True
    store.claim_batch(retry_seconds=0)
    row2, created2 = store.submit(
        device_id="d1", command_id="c1", deadline=deadline_in(60)
    )
    assert created2 is False
    assert row2["id"] == row1["id"]
    assert row2["state"] == "inflight"
    assert row2["attempts"] == 1


def test_same_command_id_other_device_conflicts(store):
    store.submit(device_id="d1", command_id="c1", deadline=deadline_in(60))
    with pytest.raises(CommandIdConflict):
        store.submit(device_id="d2", command_id="c1", deadline=deadline_in(60))


def test_same_device_is_fifo_and_other_devices_parallel(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    store.submit(device_id="d1", command_id="a2", deadline=deadline_in(60))
    store.submit(device_id="d2", command_id="b1", deadline=deadline_in(60))

    claimed = store.claim_batch(retry_seconds=0)
    assert [r["command_id"] for r in claimed] == ["a1", "b1"]

    # a1 is still inflight -> a2 must not be claimed for d1
    assert store.claim_batch(retry_seconds=0) == []
    store.finish("a1", state="executed", receipt_status="succeeded")
    claimed = store.claim_batch(retry_seconds=0)
    assert [r["command_id"] for r in claimed] == ["a2"]


def test_inflight_blocks_queue_even_after_deadline(store):
    store.submit(
        device_id="d1", command_id="a1", deadline=deadline_in(0.2)
    )
    store.submit(device_id="d1", command_id="a2", deadline=deadline_in(60))
    store.claim_batch(retry_seconds=0)
    time.sleep(0.25)

    # a1's deadline has passed but it was sent: must stay inflight, still block.
    assert store.expire_pending() == 0
    assert store.get("a1")["state"] == "inflight"
    assert store.claim_batch(retry_seconds=0) == []
    assert store.get("a2")["state"] == "pending"


def test_pending_past_deadline_is_unexecuted(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(0.1))
    time.sleep(0.15)
    store.expire_pending()
    row = store.get("a1")
    assert row["state"] == "unexecuted"
    assert row["attempts"] == 0


def test_pending_past_deadline_never_claimed(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(0.1))
    time.sleep(0.15)
    assert store.claim_batch(retry_seconds=0) == []
    assert store.get("a1")["state"] == "unexecuted"


def test_finish_is_idempotent(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    store.claim_batch(retry_seconds=0)
    assert store.finish("a1", state="executed", receipt_status="succeeded")
    assert not store.finish("a1", state="failed", receipt_status="failed")
    assert store.get("a1")["state"] == "executed"


def test_duplicate_of_terminal_command_stays_terminal(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    store.claim_batch(retry_seconds=0)
    store.finish("a1", state="executed", receipt_status="succeeded")
    row, created = store.submit(
        device_id="d1", command_id="a1", deadline=deadline_in(60)
    )
    assert created is False and row["state"] == "executed"
    assert store.claim_batch(retry_seconds=0) == []


def test_backoff_prevents_tight_retry(store):
    store.submit(device_id="d1", command_id="a1", deadline=deadline_in(60))
    assert len(store.claim_batch(retry_seconds=5)) == 1
    store.release_to_pending("a1", "not delivered")
    assert store.claim_batch(retry_seconds=5) == []
    assert store.claim_batch(retry_seconds=0)
