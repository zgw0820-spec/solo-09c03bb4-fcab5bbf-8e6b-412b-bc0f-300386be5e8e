"""Background reconciliation worker.

A run is created when an operator submits a device id on the web page.  The
worker then walks the run's in-scope commands strictly one id at a time and
queries the gateway for its authoritative execution record.

Hard rules encoded here:

* Only commands already confirmed on the web side (``executed`` / ``failed``)
  are checked; inflight / pending commands keep the dispatcher's own
  verification and ordering rules untouched.
* Each command is classified exactly once into consistent / missing /
  device_conflict / conclusion_conflict / unverified.
* A gateway that cannot be reached (connection error, timeout, 5xx) yields
  ``unverified`` -- never "consistent" and never "missing".
* Findings are written to SQLite per command, so progress and results survive
  restarts of either service; the worker simply resumes pending items.
* A run with one or more real discrepancies persistently flags the device as
  待排查; the dispatcher then stops claiming new commands for it.
"""
from __future__ import annotations

import asyncio
import logging

import httpx

from .recon_store import (
    CONSISTENT,
    CONCLUSION_CONFLICT,
    DEVICE_CONFLICT,
    MISSING,
    RUN_PENDING,
    UNVERIFIED,
    ReconciliationStore,
)
from .store import Store

log = logging.getLogger("ops.reconciler")


class Reconciler:
    def __init__(
        self,
        store: Store,
        recon: ReconciliationStore,
        gateway_url: str,
        *,
        tick_seconds: float = 0.25,
        http_timeout: float = 3.0,
    ):
        self.store = store
        self.recon = recon
        self.gateway_url = gateway_url.rstrip("/")
        self.tick_seconds = tick_seconds
        self.http_timeout = http_timeout
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    # ------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="reconciler")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    # ------------------------------------------------------------ run intake
    def request_run(self, device_id: str) -> tuple[int, int]:
        """Open a reconciliation run for a device.

        Returns ``(run_id, snapshot_count)``.  Raises ``RuntimeError`` if the
        device already has a run in progress.
        """
        device_id = device_id.strip()
        if not device_id:
            raise ValueError("设备号不能为空")
        if self.recon.has_active_run(device_id):
            raise RuntimeError("该设备已有对账任务在进行中，请等待完成后再试")
        snapshot = self.store.confirmed_for_device(device_id)
        run_id = self.recon.create_run(device_id, snapshot)
        return run_id, len(snapshot)

    # -------------------------------------------------------------- main loop
    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                # Close runs whose items are all settled (also covers runs
                # created with an empty snapshot and runs interrupted by a
                # process restart).
                for run_id in self.recon.open_run_ids():
                    self.recon.finish_run_if_complete(run_id)
                item = self.recon.next_pending_item()
                if item is not None:
                    await self._check_item(item)
                    self.recon.finish_run_if_complete(item["run_id"])
            except Exception:  # keep the worker alive across transient errors
                log.exception("reconciliation tick failed")
            await asyncio.sleep(self.tick_seconds)

    # --------------------------------------------------------------- checking
    async def _check_item(self, item) -> None:
        command_id = item["command_id"]
        run_id = item["run_id"]
        try:
            async with httpx.AsyncClient(timeout=self.http_timeout) as client:
                resp = await client.get(
                    f"{self.gateway_url}/api/commands/{command_id}"
                )
        except httpx.HTTPError as exc:
            # Unreachable / timed out: we must not guess either way.
            self.recon.record_item_result(
                run_id,
                command_id,
                result=UNVERIFIED,
                detail=f"网关不可达（{type(exc).__name__}），未核实，不得判为一致或丢失",
            )
            return

        if resp.status_code == 404:
            self.recon.record_item_result(
                run_id,
                command_id,
                result=MISSING,
                detail="网关已无该指令的执行记录（404）",
            )
            return

        if resp.status_code != 200:
            self.recon.record_item_result(
                run_id,
                command_id,
                result=UNVERIFIED,
                detail=f"网关返回 HTTP {resp.status_code}，记录不可用，未核实",
            )
            return

        body = resp.json()
        self._compare(run_id, item, body)

    def _compare(self, run_id: int, item, gw: dict) -> None:
        """Classify one gateway answer against the web-side confirmed row."""
        command_id = item["command_id"]
        gw_device = gw.get("device_id")
        gw_state = gw.get("state")
        gw_status = gw.get("status")
        common = dict(
            gateway_device=gw_device,
            gateway_state=gw_state,
            gateway_status=gw_status,
        )

        if gw_device != item["device_id"]:
            self.recon.record_item_result(
                run_id,
                command_id,
                result=DEVICE_CONFLICT,
                detail=(
                    f"设备号冲突：网页记录属于 {item['device_id']}，"
                    f"网关记录属于 {gw_device}"
                ),
                **common,
            )
            return

        # The web side holds a confirmed success/failure; a gateway row that is
        # still running or frozen unknown is not a trustworthy execution record.
        if gw_state != "executed" or gw_status not in ("succeeded", "failed"):
            self.recon.record_item_result(
                run_id,
                command_id,
                result=CONCLUSION_CONFLICT,
                detail=(
                    f"执行结论冲突：网页已确认 {item['web_state']}/"
                    f"{item['web_receipt'] or '—'}，网关记录为 "
                    f"{gw_state}/{gw_status or '—'}，无可信执行结论"
                ),
                **common,
            )
            return

        expected_status = (
            "succeeded" if item["web_state"] == "executed" else "failed"
        )
        if gw_status != expected_status:
            self.recon.record_item_result(
                run_id,
                command_id,
                result=CONCLUSION_CONFLICT,
                detail=(
                    f"执行结论冲突：网页记录 {item['web_state']}/"
                    f"{item['web_receipt'] or '—'}，网关记录 "
                    f"{gw_state}/{gw_status}"
                ),
                **common,
            )
            return

        self.recon.record_item_result(
            run_id,
            command_id,
            result=CONSISTENT,
            detail=(
                f"快照时点核对一致：双方均为 {item['web_state']}/"
                f"{item['web_receipt'] or '—'}（仅代表对账时点，不追溯之后变化）"
            ),
            **common,
        )
