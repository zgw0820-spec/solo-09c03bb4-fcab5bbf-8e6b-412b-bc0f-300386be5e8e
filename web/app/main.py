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

from .dispatcher import Dispatcher
from .store import DISPLAY, CommandIdConflict, Store

DB_PATH = os.environ.get("OPS_DB", "/data/ops.db")
GATEWAY_URL = os.environ.get("GATEWAY_URL", "http://gateway:8001")

store = Store(DB_PATH)
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
dispatcher: Dispatcher | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global dispatcher
    store.init()
    dispatcher = Dispatcher(store, GATEWAY_URL)
    dispatcher.start()
    try:
        yield
    finally:
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
