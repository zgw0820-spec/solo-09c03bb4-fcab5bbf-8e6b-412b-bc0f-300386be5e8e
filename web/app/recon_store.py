"""Persistence for post-maintenance reconciliation (对账).

After a gateway disk swap, an operator enters a device id on the web page and
asks the system to verify, *one command id at a time*, whether every command
the web already holds a confirmed outcome for (``executed`` / ``failed``) still
has a trustworthy execution record at the gateway.

Three tables live in the same SQLite file (and therefore the same named
volume) as the command table, so flags, runs and per-command findings all
survive restarts of both containers:

``device_flags``
    Devices currently "待排查".  A flag is set persistently the moment a
    reconciliation finds a real discrepancy; it is never auto-cleared.
    The dispatcher treats a flagged device as blocked for *new* claims, while
    already-inflight / pending-verification commands keep their old rules.

``recon_runs``
    One row per reconciliation request.  ``snapshot_count`` fixes the set of
    confirmed commands in scope at request time: a command whose outcome is
    confirmed *during* the scan is not retro-checked in this run, and an old
    consistent result is never relabelled as "currently consistent".

``recon_items``
    The finding for one command id:

        consistent           gateway record matches web (at snapshot time)
        missing              gateway has no record for the id (HTTP 404)
        device_conflict      gateway record belongs to another device id
        conclusion_conflict  executed/failed conclusion disagrees
        unverified           gateway unreachable / answer unusable -- this is
                             NOT consistent and NOT missing

Only ``missing`` / ``device_conflict`` / ``conclusion_conflict`` count as
discrepancies that raise the device flag.  ``unverified`` leaves the device
unflagged and the run explicitly non-OK so the operator must re-check later.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

# Per-command findings -------------------------------------------------------
CONSISTENT = "consistent"
MISSING = "missing"
DEVICE_CONFLICT = "device_conflict"
CONCLUSION_CONFLICT = "conclusion_conflict"
UNVERIFIED = "unverified"

DISCREPANCY_RESULTS = (MISSING, DEVICE_CONFLICT, CONCLUSION_CONFLICT)
ALL_RESULTS = (CONSISTENT, *DISCREPANCY_RESULTS, UNVERIFIED)

RESULT_DISPLAY = {
    CONSISTENT: "一致（快照时点）",
    MISSING: "网关记录缺失",
    DEVICE_CONFLICT: "设备号冲突",
    CONCLUSION_CONFLICT: "执行结论冲突",
    UNVERIFIED: "未核实（网关不可达）",
}

# Run lifecycle --------------------------------------------------------------
RUN_PENDING = "pending"        # created, worker has not processed it (yet)
RUN_RUNNING = "running"        # at least one item processed, more to do
RUN_DONE_OK = "done_ok"        # every in-scope item checked, all consistent
RUN_DONE_DIFF = "done_diff"    # finished with at least one discrepancy
RUN_DONE_UNVERIFIED = "done_unverified"  # finished, but some items unverified

RUN_DISPLAY = {
    RUN_PENDING: "待核对",
    RUN_RUNNING: "核对中",
    RUN_DONE_OK: "已完成：全部一致",
    RUN_DONE_DIFF: "已完成：发现差异，设备已标记待排查",
    RUN_DONE_UNVERIFIED: "已完成：存在未核实项，需重新对账",
}

# Commands whose web-side outcome is confirmed and therefore in scope.
CONFIRMED_STATES = ("executed", "failed")

SCHEMA = """
CREATE TABLE IF NOT EXISTS device_flags (
    device_id    TEXT PRIMARY KEY,
    reason       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    created_run  INTEGER,
    cleared_at   TEXT,
    clear_reason TEXT
);

CREATE TABLE IF NOT EXISTS recon_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id       TEXT NOT NULL,
    status          TEXT NOT NULL,
    snapshot_count  INTEGER NOT NULL DEFAULT 0,
    checked_count   INTEGER NOT NULL DEFAULT 0,
    diff_count      INTEGER NOT NULL DEFAULT 0,
    unverified_count INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    finished_at     TEXT
);

CREATE INDEX IF NOT EXISTS recon_runs_device ON recon_runs(device_id, id);
CREATE INDEX IF NOT EXISTS recon_runs_status ON recon_runs(status);

CREATE TABLE IF NOT EXISTS recon_items (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id           INTEGER NOT NULL REFERENCES recon_runs(id),
    command_id       TEXT NOT NULL,
    device_id        TEXT NOT NULL,
    web_state        TEXT NOT NULL,
    web_receipt      TEXT,
    result           TEXT NOT NULL,
    detail           TEXT,
    gateway_device   TEXT,
    gateway_state    TEXT,
    gateway_status   TEXT,
    checked_at       TEXT,
    created_at       TEXT NOT NULL,
    UNIQUE(run_id, command_id)
);

CREATE INDEX IF NOT EXISTS recon_items_run ON recon_items(run_id, id);
CREATE INDEX IF NOT EXISTS recon_items_result ON recon_items(result);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


class ReconciliationStore:
    """Reconciliation tables, sharing one database file with the command store."""

    def __init__(self, path: str | Path):
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # Reconciliation writes must serialise against command-table writes in
        # the same file; reuse one process-wide lock per path.
        self._write_lock = _locks.setdefault(self.path, threading.RLock())

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        with self._write_lock, self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    # ----------------------------------------------------------- device flags
    def is_flagged(self, device_id: str) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM device_flags WHERE device_id=? AND cleared_at IS NULL",
                (device_id,),
            ).fetchone()
            return row is not None

    def flagged_devices(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM device_flags WHERE cleared_at IS NULL "
                "ORDER BY created_at"
            ).fetchall()

    def flag_device(
        self, device_id: str, reason: str, run_id: Optional[int] = None
    ) -> bool:
        """Persistently mark a device as needing investigation.

        Returns True if a new active flag was created, False if one already
        existed (the original reason is preserved).
        """
        now = utcnow()
        with self._tx() as conn:
            existing = conn.execute(
                "SELECT * FROM device_flags WHERE device_id=? AND cleared_at IS NULL",
                (device_id,),
            ).fetchone()
            if existing is not None:
                return False
            conn.execute(
                "INSERT INTO device_flags (device_id, reason, created_at, created_run) "
                "VALUES (?, ?, ?, ?)",
                (device_id, reason, now, run_id),
            )
            return True

    def clear_flag(self, device_id: str, reason: str) -> bool:
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE device_flags SET cleared_at=?, clear_reason=? "
                "WHERE device_id=? AND cleared_at IS NULL",
                (utcnow(), reason, device_id),
            )
            return cur.rowcount == 1

    # ------------------------------------------------------------------- runs
    def create_run(self, device_id: str, snapshot: list[sqlite3.Row]) -> int:
        """Create a run and fix its in-scope command set.

        ``snapshot`` is the list of confirmed (executed/failed) command rows
        for the device at request time; later confirmations are out of scope.
        """
        now = utcnow()
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT INTO recon_runs (device_id, status, snapshot_count, "
                "created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (device_id, RUN_PENDING, len(snapshot), now, now),
            )
            run_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO recon_items (run_id, command_id, device_id, "
                "web_state, web_receipt, result, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    (
                        run_id,
                        r["command_id"],
                        r["device_id"],
                        r["state"],
                        r["receipt_status"],
                        RUN_PENDING,
                        now,
                    )
                    for r in snapshot
                ],
            )
            return run_id

    def has_active_run(self, device_id: str) -> bool:
        """True while a run for this device is still being processed."""
        with self.connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM recon_runs WHERE device_id=? "
                "AND status IN (?, ?)",
                (device_id, RUN_PENDING, RUN_RUNNING),
            ).fetchone()
            return row is not None

    def open_run_ids(self) -> list[int]:
        with self.connect() as conn:
            return [
                r["id"]
                for r in conn.execute(
                    "SELECT id FROM recon_runs WHERE status IN (?, ?) ORDER BY id",
                    (RUN_PENDING, RUN_RUNNING),
                ).fetchall()
            ]

    def next_pending_item(self) -> Optional[sqlite3.Row]:
        """Claim the oldest not-yet-checked reconciliation item.

        Marks the parent run as ``running``.  Only one item per run is handed
        out at a time so per-device checks stay in command order.
        """
        now = utcnow()
        with self._tx() as conn:
            row = conn.execute(
                "SELECT ri.* FROM recon_items ri JOIN recon_runs rr "
                "ON ri.run_id = rr.id WHERE ri.result = ? "
                "ORDER BY rr.id, ri.id LIMIT 1",
                (RUN_PENDING,),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE recon_runs SET status=?, updated_at=? WHERE id=? "
                "AND status IN (?, ?)",
                (RUN_RUNNING, now, row["run_id"], RUN_PENDING, RUN_RUNNING),
            )
            return row

    def record_item_result(
        self,
        run_id: int,
        command_id: str,
        *,
        result: str,
        detail: str,
        gateway_device: Optional[str] = None,
        gateway_state: Optional[str] = None,
        gateway_status: Optional[str] = None,
    ) -> None:
        if result not in ALL_RESULTS:
            raise ValueError(f"unknown reconciliation result {result!r}")
        now = utcnow()
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE recon_items SET result=?, detail=?, gateway_device=?, "
                "gateway_state=?, gateway_status=?, checked_at=? "
                "WHERE run_id=? AND command_id=? AND result=?",
                (
                    result,
                    detail,
                    gateway_device,
                    gateway_state,
                    gateway_status,
                    now,
                    run_id,
                    command_id,
                    RUN_PENDING,
                ),
            )
            if cur.rowcount != 1:
                return  # already recorded (e.g. worker restart race); keep first
            conn.execute(
                "UPDATE recon_runs SET checked_count=checked_count+1, "
                "diff_count=diff_count+?, unverified_count=unverified_count+?, "
                "updated_at=? WHERE id=?",
                (
                    1 if result in DISCREPANCY_RESULTS else 0,
                    1 if result == UNVERIFIED else 0,
                    now,
                    run_id,
                ),
            )

    def finish_run_if_complete(self, run_id: int) -> Optional[str]:
        """Close the run when every item has a final result.

        Raises the device flag when at least one discrepancy is present.
        Returns the final status, or None if items remain.
        """
        now = utcnow()
        with self._tx() as conn:
            row = conn.execute(
                "SELECT * FROM recon_runs WHERE id=?", (run_id,)
            ).fetchone()
            if row is None:
                return None
            remaining = conn.execute(
                "SELECT COUNT(*) AS n FROM recon_items WHERE run_id=? AND result=?",
                (run_id, RUN_PENDING),
            ).fetchone()["n"]
            if remaining:
                return None
            if row["diff_count"] > 0:
                status = RUN_DONE_DIFF
            elif row["unverified_count"] > 0:
                status = RUN_DONE_UNVERIFIED
            else:
                status = RUN_DONE_OK
            conn.execute(
                "UPDATE recon_runs SET status=?, updated_at=?, finished_at=? "
                "WHERE id=?",
                (status, now, now, run_id),
            )
            if status == RUN_DONE_DIFF:
                flagged = conn.execute(
                    "SELECT 1 FROM device_flags WHERE device_id=? "
                    "AND cleared_at IS NULL",
                    (row["device_id"],),
                ).fetchone()
                if flagged is None:
                    conn.execute(
                        "INSERT INTO device_flags (device_id, reason, created_at, "
                        "created_run) VALUES (?, ?, ?, ?)",
                        (
                            row["device_id"],
                            f"对账批次 #{run_id} 发现 {row['diff_count']} 条差异",
                            now,
                            run_id,
                        ),
                    )
            return status

    # ----------------------------------------------------------------- reads
    def get_run(self, run_id: int) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM recon_runs WHERE id=?", (run_id,)
            ).fetchone()

    def list_runs(self, device_id: Optional[str] = None, limit: int = 100) -> list[sqlite3.Row]:
        with self.connect() as conn:
            if device_id is None:
                return conn.execute(
                    "SELECT * FROM recon_runs ORDER BY id DESC LIMIT ?", (limit,)
                ).fetchall()
            return conn.execute(
                "SELECT * FROM recon_runs WHERE device_id=? ORDER BY id DESC LIMIT ?",
                (device_id, limit),
            ).fetchall()

    def list_items(self, run_id: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM recon_items WHERE run_id=? ORDER BY id", (run_id,)
            ).fetchall()

    def count_confirmed_since(self, device_id: str, created_at: str) -> int:
        """Confirmed commands for the device that appeared after the snapshot.

        These are deliberately *not* checked by the run; the UI shows the
        count so an old "consistent" finding is never read as current truth.
        """
        with self.connect() as conn:
            return conn.execute(
                "SELECT COUNT(*) AS n FROM commands WHERE device_id=? "
                "AND state IN ('executed','failed') AND created_at > ?",
                (device_id, created_at),
            ).fetchone()["n"]


_locks: dict[str, threading.RLock] = {}
