"""Integration tests for post-maintenance reconciliation.

The reconciler talks to the real gateway ASGI app over httpx ASGITransport,
exactly like the dispatcher tests.  Gateway-side tampering (deleted row,
rewritten device id / conclusion) stands in for a swapped gateway disk whose
history no longer matches the operations web.
"""
from __future__ import annotations

import asyncio
import importlib
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest


# --------------------------------------------------------------------- setup
@pytest.fixture()
def services(tmp_path, monkeypatch):
    monkeypatch.setenv("GATEWAY_DB", str(tmp_path / "gateway.db"))
    monkeypatch.setenv("GATEWAY_EXECUTION_DELAY", "0.05")
    monkeypatch.setenv("OPS_DB", str(tmp_path / "ops.db"))
    _reload()
    _gw().store.init()
    _web().store.init()
    _web().recon_store.init()
    return _gw(), _web()


def _reload():
    for name in [m for m in list(sys.modules) if m.startswith(("web.app", "gateway.app"))]:
        del sys.modules[name]
    importlib.import_module("gateway.app.main")
    importlib.import_module("web.app.main")


def _gw():
    return sys.modules["gateway.app.main"]


def _web():
    return sys.modules["web.app.main"]


def deadline_secs(seconds: float) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=seconds)


def _patched_httpx(gw_app):
    """A private httpx module whose AsyncClient always hits the gateway ASGI app.

    Rebinding ``module.httpx`` (rather than patching the shared httpx package)
    keeps the double entirely local to the module under test.
    """
    import types

    private = types.ModuleType("httpx_patched")

    class PatchedAsyncClient(httpx.AsyncClient):
        def __init__(self, *args, **kwargs):
            kwargs.pop("timeout", None)
            kwargs.pop("transport", None)
            kwargs.pop("base_url", None)
            super().__init__(*args, transport=httpx.ASGITransport(app=gw_app),
                             base_url="http://gateway", timeout=2.0, **kwargs)

    private.AsyncClient = PatchedAsyncClient
    # Re-export the exception classes the production code references through
    # its own ``httpx`` binding.
    private.HTTPError = httpx.HTTPError
    private.ConnectError = httpx.ConnectError
    private.ConnectTimeout = httpx.ConnectTimeout
    private.ReadTimeout = httpx.ReadTimeout
    return private


def _patch_gateway(monkeypatch, gw_app, module_name="web.app.reconciler"):
    monkeypatch.setattr(sys.modules[module_name], "httpx", _patched_httpx(gw_app))


def make_reconciler(web_main, tick: float = 0.02):
    return web_main.Reconciler(
        web_main.store, web_main.recon_store, "http://gateway", tick_seconds=tick
    )


def make_dispatcher(monkeypatch, web_main, gw_app):
    _patch_gateway(monkeypatch, gw_app, module_name="web.app.dispatcher")
    return web_main.Dispatcher(
        web_main.store,
        "http://gateway",
        tick_seconds=0.02,
        verify_interval=0.05,
        verify_delay_after_loss=0.1,
        retry_seconds=0,
        reconcile_grace=0.3,
        recon_store=web_main.recon_store,
    )


async def _wait_until(predicate, timeout=6.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return predicate()


def drive_worker(worker, predicate, timeout=8.0) -> bool:
    async def runner():
        worker.start()
        ok = await _wait_until(predicate, timeout)
        await worker.stop()
        return ok

    return asyncio.run(runner())


def settle_command(web_main, gw_app, monkeypatch, device, cid, *, drop=False):
    """Run one command through the dispatcher to its confirmed terminal state."""
    web_main.store.submit(
        device_id=device, command_id=cid, deadline=deadline_secs(30),
        drop_receipt=drop,
    )
    d = make_dispatcher(monkeypatch, web_main, gw_app)
    ok = drive_worker(d, lambda: web_main.store.get(cid)["state"] in ("executed", "failed"))
    assert ok, f"{cid} never reached a terminal state"


def _run_items(web_main, run_id):
    return {i["command_id"]: i for i in web_main.recon_store.list_items(run_id)}


# --------------------------------------------------------------------- tests
def test_consistent_records_close_ok_without_flag(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")
    settle_command(web, gw.app, monkeypatch, "d1", "c2")

    worker = make_reconciler(web)
    run_id, count = worker.request_run("d1")
    assert count == 2
    ok = drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"].startswith("done")
    )
    assert ok
    run = web.recon_store.get_run(run_id)
    assert run["status"] == "done_ok"
    assert run["checked_count"] == 2 and run["diff_count"] == 0
    assert web.recon_store.is_flagged("d1") is False
    for item in _run_items(web, run_id).values():
        assert item["result"] == "consistent"
        assert "快照时点" in item["detail"]


def test_missing_gateway_record_is_detected_and_flags_device(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")

    # Simulate the swapped disk: the gateway no longer holds this execution.
    with gw.store.connect() as conn:
        conn.execute("DELETE FROM executions WHERE command_id='c1'")
    assert gw.store.get("c1") is None

    worker = make_reconciler(web)
    run_id, _ = worker.request_run("d1")
    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"] == "done_diff"
    )
    item = _run_items(web, run_id)["c1"]
    assert item["result"] == "missing"
    assert web.recon_store.is_flagged("d1") is True
    flag = web.recon_store.flagged_devices()[0]
    assert flag["created_run"] == run_id


def test_device_id_conflict_is_detected(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")

    with gw.store.connect() as conn:
        conn.execute(
            "UPDATE executions SET device_id='d-OTHER' WHERE command_id='c1'"
        )

    worker = make_reconciler(web)
    run_id, _ = worker.request_run("d1")
    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"] == "done_diff"
    )
    item = _run_items(web, run_id)["c1"]
    assert item["result"] == "device_conflict"
    assert item["gateway_device"] == "d-OTHER"
    assert web.recon_store.is_flagged("d1") is True


def test_conclusion_conflict_success_vs_failure(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")
    assert web.store.get("c1")["state"] == "executed"

    # Gateway history now claims the reboot failed while the web saw success.
    with gw.store.connect() as conn:
        conn.execute(
            "UPDATE executions SET receipt_status='failed' WHERE command_id='c1'"
        )

    worker = make_reconciler(web)
    run_id, _ = worker.request_run("d1")
    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"] == "done_diff"
    )
    item = _run_items(web, run_id)["c1"]
    assert item["result"] == "conclusion_conflict"
    assert item["gateway_status"] == "failed"
    assert web.recon_store.is_flagged("d1") is True


def test_gateway_unknown_row_is_conclusion_conflict_not_consistent(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")
    with gw.store.connect() as conn:
        conn.execute(
            "UPDATE executions SET state='unknown', receipt_status=NULL "
            "WHERE command_id='c1'"
        )

    worker = make_reconciler(web)
    run_id, _ = worker.request_run("d1")
    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"] == "done_diff"
    )
    assert _run_items(web, run_id)["c1"]["result"] == "conclusion_conflict"
    assert web.recon_store.is_flagged("d1") is True


def test_unreachable_gateway_is_unverified_never_consistent_or_missing(
    services, monkeypatch
):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")

    class DownTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request):
            raise httpx.ConnectError("gateway down", request=request)

    import types
    down_httpx = types.ModuleType("httpx_down")

    class DownClient(httpx.AsyncClient):
        def __init__(self, *args, **kwargs):
            kwargs.pop("timeout", None)
            kwargs.pop("transport", None)
            kwargs.pop("base_url", None)
            super().__init__(*args, transport=DownTransport(),
                             base_url="http://gateway", timeout=2.0, **kwargs)

    down_httpx.AsyncClient = DownClient
    down_httpx.HTTPError = httpx.HTTPError
    monkeypatch.setattr(sys.modules["web.app.reconciler"], "httpx", down_httpx)

    worker = make_reconciler(web)
    run_id, _ = worker.request_run("d1")
    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"].startswith("done")
    )
    run = web.recon_store.get_run(run_id)
    assert run["status"] == "done_unverified"
    item = _run_items(web, run_id)["c1"]
    assert item["result"] == "unverified"
    assert "未核实" in item["detail"]
    # Crucially: no flag from an unreachable gateway, and no false "consistent".
    assert web.recon_store.is_flagged("d1") is False


def test_flag_blocks_new_claims_but_inflight_and_other_devices_continue(
    services, monkeypatch
):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    # Keep commands observably inflight long enough to land the flag mid-flight.
    gw.EXECUTION_DELAY = 0.8

    # c0 is an older, already-confirmed command whose missing gateway record
    # will raise the persistent flag during reconciliation.
    settle_command(web, gw.app, monkeypatch, "d1", "c0")
    with gw.store.connect() as conn:
        conn.execute("DELETE FROM executions WHERE command_id='c0'")

    # c1 is delivered and becomes inflight *before* the flag is persisted.
    web.store.submit(device_id="d1", command_id="c1", deadline=deadline_secs(30))
    # c2 will be submitted while d1 is flagged; d3 belongs to another device.
    web.store.submit(device_id="d3", command_id="c3", deadline=deadline_secs(30))

    d = make_dispatcher(monkeypatch, web, gw.app)

    async def body():
        d.start()
        try:
            assert await _wait_until(lambda: web.store.get("c1")["state"] == "inflight")
            assert await _wait_until(lambda: gw.store.get("c1") is not None)

            # Run reconciliation while c1 is still on the wire: the flag lands
            # mid-flight.  Reconciliation only reads gateway data, so it does
            # not touch c1's delivery path.
            run_id, _ = worker.request_run("d1")
            assert await _wait_until(lambda: web.recon_store.is_flagged("d1"))
            assert web.recon_store.get_run(run_id)["status"] == "done_diff"

            # New work for the flagged device is now rejected at claim time.
            web.store.submit(device_id="d1", command_id="c2", deadline=deadline_secs(30))
            await asyncio.sleep(0.4)
            assert web.store.get("c2")["state"] == "pending"
            assert gw.store.get("c2") is None

            # The already-inflight command keeps its old verification rule and
            # still settles normally; the other device is unaffected.
            assert await _wait_until(
                lambda: web.store.get("c1")["state"] == "executed"
            )
            assert await _wait_until(
                lambda: web.store.get("c3")["state"] == "executed"
            )
            await asyncio.sleep(0.3)
            assert web.store.get("c2")["state"] == "pending"
            assert gw.store.get("c2") is None
        finally:
            await d.stop()

    # A reconciler whose HTTP client also targets the gateway ASGI app.
    _patch_gateway(monkeypatch, gw.app, module_name="web.app.reconciler")
    worker = make_reconciler(web)

    async def orchestrated():
        worker.start()
        try:
            await body()
        finally:
            await worker.stop()

    asyncio.run(orchestrated())
    assert gw.store.get("c1")["executions"] == 1
    assert gw.store.get("c3")["executions"] == 1
    assert web.recon_store.is_flagged("d1") is True


def test_snapshot_excludes_confirmations_that_arrive_during_scan(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")

    # Open the run while the worker is not running yet.
    worker = make_reconciler(web)
    run_id, count = worker.request_run("d1")
    assert count == 1

    # While the scan is pending, another command gets confirmed.  It must not
    # be retro-checked inside this run, and the old finding must not be relabelled.
    settle_command(web, gw.app, monkeypatch, "d1", "c2")

    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"].startswith("done")
    )
    items = _run_items(web, run_id)
    assert list(items) == ["c1"]
    assert items["c1"]["result"] == "consistent"
    run = web.recon_store.get_run(run_id)
    assert web.recon_store.count_confirmed_since("d1", run["created_at"]) == 1


def test_pending_and_inflight_commands_are_out_of_scope(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    web.store.submit(device_id="d1", command_id="p1", deadline=deadline_secs(30))

    worker = make_reconciler(web)
    run_id, count = worker.request_run("d1")
    assert count == 0
    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"] == "done_ok"
    )
    assert web.recon_store.list_items(run_id) == []
    assert web.recon_store.is_flagged("d1") is False


def test_run_resumes_and_flag_persists_across_restart(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")
    settle_command(web, gw.app, monkeypatch, "d1", "c2")
    with gw.store.connect() as conn:
        conn.execute("DELETE FROM executions WHERE command_id='c2'")

    # First worker processes exactly one item, then the process "dies".
    worker = make_reconciler(web)
    run_id, _ = worker.request_run("d1")

    async def partial():
        worker.start()
        await _wait_until(lambda: web.recon_store.get_run(run_id)["checked_count"] >= 1)
        await worker.stop()

    asyncio.run(partial())
    mid = web.recon_store.get_run(run_id)
    assert mid["checked_count"] == 1 and mid["status"] == "running"

    # Simulate a full restart of the web process: modules reload, SQLite files
    # are reopened from the same paths.
    _reload()
    gw2, web2 = _gw(), _web()
    gw2.store.init()
    web2.store.init()
    web2.recon_store.init()
    _patch_gateway(monkeypatch, gw2.app)

    assert web2.recon_store.get_run(run_id)["status"] == "running"
    worker2 = make_reconciler(web2)
    assert drive_worker(
        worker2, lambda: web2.recon_store.get_run(run_id)["status"] == "done_diff"
    )
    items = _run_items(web2, run_id)
    assert items["c2"]["result"] == "missing"
    assert web2.recon_store.is_flagged("d1") is True
    # The original command table is untouched by reconciliation.
    assert web2.store.get("c1")["state"] == "executed"
    assert web2.store.get("c2")["state"] == "executed"


def test_api_endpoints_round_trip(services, monkeypatch):
    gw, web = services
    _patch_gateway(monkeypatch, gw.app)
    settle_command(web, gw.app, monkeypatch, "d1", "c1")
    worker = make_reconciler(web)
    # In production lifespan() installs this global; do the same for the
    # route handlers under test.
    web.reconciler = worker
    run_id, _ = worker.request_run("d1")
    assert drive_worker(
        worker, lambda: web.recon_store.get_run(run_id)["status"].startswith("done")
    )

    # Call the route handlers directly (they are plain sync functions).
    payload = web.api_recon_run(run_id)
    assert payload["status"] == "done_ok"
    assert payload["items"][0]["result_label"].startswith("一致")
    assert payload["device_flagged"] is False

    # Starting a second run for the same device is allowed after completion.
    started = web.api_start_recon(web.ReconRequest(device_id="d1"))
    assert started["snapshot_count"] == 1
    assert web.recon_store.has_active_run("d1") is True

    # Flags endpoint reflects an empty set, then a manually raised flag.
    assert web.api_recon_flags() == []
    web.recon_store.flag_device("d9", "manual")
    flags = web.api_recon_flags()
    assert [f["device_id"] for f in flags] == ["d9"]
