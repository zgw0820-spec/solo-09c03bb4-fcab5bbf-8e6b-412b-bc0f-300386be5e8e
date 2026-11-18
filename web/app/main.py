"""Operations web: submit reboot commands, watch progress, simulate loss."""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import httpx
from pydantic import BaseModel

from .dispatcher import Dispatcher
from .recon_store import (
    RESULT_DISPLAY,
    RUN_DISPLAY,
    ReconciliationStore,
)
from .reconciler import Reconciler
from .store import DISPLAY, CommandIdConflict, Store

DB_PATH = os.environ.get("OPS_DB", "/data/ops.db")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://gateway:8001")

store = Store(DB_PATH)
recon_store = ReconciliationStore(DB_PATH)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
dispatcher: Dispatcher | None = None
reconciler: Reconciler | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global dispatcher, reconciler
    store.init()
    recon_store.init()
    dispatcher = Dispatcher(store, GATEWAY_URL, recon_store=recon_store)
    reconciler = Reconciler(store, recon_store, GATEWAY_URL)
    dispatcher.start()
    reconciler.start()
    try:
        yield
    finally:
        await reconciler.stop()
        await dispatcher.stop()


app = FastAPI(title="Reboot Control", lifespan=lifespan)


def row_to_dict(row) -> dict:
    return {
        "command_id": row["command_id"],
        "device_id": row["device_id"],
        "deadline": row["deadline"],
        "state": row["state"],
        "state_label": DISPLAY.get(row["state"], row["state"]),
        "receipt_status": row["receipt_status"],
        "detail": row["detail"],
        "attempts": row["attempts"],
        "receipt_lost": bool(row["receipt_lost"]),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    rows = [row_to_dict(r) for r in store.list()]
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "rows": rows,
            "counts": store.counts(),
            "now": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "gateway_url": GATEWAY_URL,
            "flags": recon_store.flagged_devices(),
            "runs": [_run_to_dict(r) for r in recon_store.list_runs(limit=20)],
            "run_display": RUN_DISPLAY,
            "result_display": RESULT_DISPLAY,
        },
    )


@app.post("/commands")
def submit_form(
    device_id: str = Form(...),
    command_id: str = Form(...),
    deadline_minutes: int = Form(30),
    drop_receipt: bool = Form(False),
):
    device_id = device_id.strip()
    command_id = command_id.strip()
    if not device_id or not command_id:
        raise HTTPException(status_code=400, detail="设备号和指令号不能为空")
    if deadline_minutes < 1 or deadline_minutes > 1440:
        raise HTTPException(status_code=400, detail="截止时间需在 1..1440 分钟之间")
    deadline = datetime.now(timezone.utc) + timedelta(minutes=deadline_minutes)
    try:
        row, created = store.submit(
            device_id=device_id,
            command_id=command_id,
            deadline=deadline,
            drop_receipt=drop_receipt,
        )
    except CommandIdConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    # Duplicates deliberately redirect to the original record; nothing is
    # re-queued and `created` is False.
    suffix = "" if created else "?duplicate=1"
    return RedirectResponse(url=f"/commands/{row['command_id']}{suffix}", status_code=303)


@app.get("/commands/{command_id}", response_class=HTMLResponse)
def detail(request: Request, command_id: str, duplicate: int = 0):
    row = store.get(command_id)
    if row is None:
        raise HTTPException(status_code=404, detail="指令不存在")
    return templates.TemplateResponse(
        request,
        "detail.html",
        {"cmd": row_to_dict(row), "duplicate": bool(duplicate)},
    )


@app.get("/api/commands/{command_id}")
def api_detail(command_id: str):
    row = store.get(command_id)
    if row is None:
        raise HTTPException(status_code=404, detail="指令不存在")
    return row_to_dict(row)


@app.get("/api/commands")
def api_list():
    return [row_to_dict(r) for r in store.list()]


@app.post("/commands/{command_id}/drop-receipt")
def drop_receipt(command_id: str):
    """Simulate losing the receipt for this command.

    * Not yet delivered: arm the flag; the next delivery asks the gateway to
      execute but swallow its receipt.
    * Already delivered: ask the gateway to drop the response (works while it
      is still inside the simulated device delay), keeping the execution.
    """
    row = store.get(command_id)
    if row is None:
        raise HTTPException(status_code=404, detail="指令不存在")
    if row["state"] not in ("pending", "inflight"):
        raise HTTPException(status_code=409, detail="指令已结案，无法触发丢回执")

    if row["state"] == "inflight":
        try:
            resp = httpx.post(
                f"{GATEWAY_URL.rstrip('/')}/api/commands/{command_id}/drop-receipt",
                timeout=2.0,
            )
            delivered = resp.status_code == 200
        except httpx.HTTPError:
            delivered = False
        if not delivered:
            # The gateway has not seen it yet; next delivery will drop.
            store.arm_drop_receipt(command_id)
    else:
        store.arm_drop_receipt(command_id)
    return RedirectResponse(url=f"/commands/{command_id}", status_code=303)


@app.get("/healthz")
def healthz():
    return {"ok": True}


# ==========================================================================
# Reconciliation (更换网关磁盘后的历史执行记录对账)
# ==========================================================================
class ReconRequest(BaseModel):
    device_id: str


def _run_to_dict(row) -> dict:
    return {
        "id": row["id"],
        "device_id": row["device_id"],
        "status": row["status"],
        "status_label": RUN_DISPLAY.get(row["status"], row["status"]),
        "snapshot_count": row["snapshot_count"],
        "checked_count": row["checked_count"],
        "diff_count": row["diff_count"],
        "unverified_count": row["unverified_count"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "finished_at": row["finished_at"],
    }


def _item_to_dict(row) -> dict:
    return {
        "command_id": row["command_id"],
        "device_id": row["device_id"],
        "web_state": row["web_state"],
        "web_receipt": row["web_receipt"],
        "result": row["result"],
        "result_label": RESULT_DISPLAY.get(row["result"], row["result"]),
        "detail": row["detail"],
        "gateway_device": row["gateway_device"],
        "gateway_state": row["gateway_state"],
        "gateway_status": row["gateway_status"],
        "checked_at": row["checked_at"],
    }


def _start_recon(device_id: str) -> int:
    device_id = (device_id or "").strip()
    if not device_id:
        raise HTTPException(status_code=400, detail="设备号不能为空")
    try:
        run_id, _count = reconciler.request_run(device_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return run_id


@app.post("/reconcile")
def reconcile_form(device_id: str = Form(...)):
    run_id = _start_recon(device_id)
    return RedirectResponse(url=f"/reconcile/runs/{run_id}", status_code=303)


@app.post("/api/reconcile")
def api_start_recon(payload: ReconRequest):
    run_id = _start_recon(payload.device_id)
    return {"run_id": run_id, **_run_to_dict(recon_store.get_run(run_id))}


@app.get("/reconcile/runs/{run_id}", response_class=HTMLResponse)
def recon_run_page(request: Request, run_id: int):
    run = recon_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="对账批次不存在")
    items = [_item_to_dict(r) for r in recon_store.list_items(run_id)]
    # Commands confirmed after this run's snapshot are explicitly out of scope:
    # an old "consistent" finding must never be presented as current truth.
    new_after_snapshot = recon_store.count_confirmed_since(
        run["device_id"], run["created_at"]
    )
    return templates.TemplateResponse(
        request,
        "recon_run.html",
        {
            "run": _run_to_dict(run),
            "items": items,
            "new_after_snapshot": new_after_snapshot,
            "flagged": recon_store.is_flagged(run["device_id"]),
            "result_display": RESULT_DISPLAY,
        },
    )


@app.get("/api/reconcile/runs/{run_id}")
def api_recon_run(run_id: int):
    run = recon_store.get_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="对账批次不存在")
    return {
        **_run_to_dict(run),
        "items": [_item_to_dict(r) for r in recon_store.list_items(run_id)],
        "new_confirmed_after_snapshot": recon_store.count_confirmed_since(
            run["device_id"], run["created_at"]
        ),
        "device_flagged": recon_store.is_flagged(run["device_id"]),
    }


@app.get("/api/reconcile/flags")
def api_recon_flags():
    return [dict(r) for r in recon_store.flagged_devices()]


@app.post("/api/reconcile/flags/{device_id}/clear")
def api_clear_flag(device_id: str, reason: str = Form("人工确认后解除待排查标记")):
    """Manual acknowledgement only; the flag is never auto-cleared."""
    if not recon_store.clear_flag(device_id, reason.strip() or "人工解除"):
        raise HTTPException(status_code=404, detail="该设备没有生效中的待排查标记")
    return {"ok": True, "device_id": device_id}
