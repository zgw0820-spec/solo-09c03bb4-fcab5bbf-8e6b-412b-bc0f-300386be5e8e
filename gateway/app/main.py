"""Edge gateway simulator (FastAPI).

Endpoints
---------
POST /api/commands        execute a command (idempotent by command_id)
GET  /api/commands/{id}   authoritative execution record for verification
POST /api/commands/{id}/drop-receipt
                          make the response for this id disappear while still
                          committing the execution (simulates a lost receipt
                          on the way back to the operations web)
GET  /api/executions      what was really executed (demo / audit view)
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .store import GatewayStore

DB_PATH = os.environ.get("GATEWAY_DB", "/data/gateway.db")
EXECUTION_DELAY = float(os.environ.get("GATEWAY_EXECUTION_DELAY", "1.5"))

store = GatewayStore(DB_PATH)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    store.init()
    yield


app = FastAPI(title="Edge Gateway Simulator", lifespan=lifespan)


class CommandIn(BaseModel):
    command_id: str = Field(min_length=1, max_length=128)
    device_id: str = Field(min_length=1, max_length=128)
    drop_receipt: bool = False


def receipt_payload(row) -> dict:
    return {
        "command_id": row["command_id"],
        "device_id": row["device_id"],
        "state": row["state"],
        "status": row["receipt_status"],
        "executions": row["executions"],
        "executed_at": row["executed_at"],
    }


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True}


@app.post("/api/commands")
def execute(payload: CommandIn):
    action, row = store.accept(
        payload.command_id, payload.device_id, payload.drop_receipt
    )
    if action == "other":
        raise HTTPException(
            status_code=409,
            detail="command_id already registered for a different device",
        )
    if action == "busy":
        raise HTTPException(status_code=409, detail="command is still executing")
    if action == "duplicate":
        # Replay the recorded outcome.  Crucially the original execution is
        # not repeated: only the stored receipt is returned (or lost again if
        # this id is armed to drop receipts).
        if row["drop_receipt"]:
            return JSONResponse(
                status_code=202,
                content={
                    "command_id": row["command_id"],
                    "device_id": row["device_id"],
                    "state": "receipt_lost",
                    "status": None,
                    "executions": row["executions"],
                },
            )
        return receipt_payload(row)

    row = store.commit_execution(
        payload.command_id,
        payload.device_id,
        status="succeeded",
        delay_seconds=EXECUTION_DELAY,
    )
    if row["drop_receipt"]:
        return JSONResponse(
            status_code=202,
            content={
                "command_id": row["command_id"],
                "device_id": row["device_id"],
                "state": "receipt_lost",
                "status": None,
                "executions": row["executions"],
            },
        )
    return receipt_payload(row)


@app.get("/api/commands/{command_id}")
def query(command_id: str):
    row = store.get(command_id)
    if row is None:
        raise HTTPException(status_code=404, detail="unknown command")
    return receipt_payload(row)


@app.post("/api/commands/{command_id}/drop-receipt")
def drop_receipt(command_id: str):
    """Make the next response for this id disappear.

    This works while the command is still `running` (the POST is sleeping in
    the execution delay) and is harmless afterwards: the execution record is
    already committed and stays queryable.
    """
    row = store.get(command_id)
    if row is None:
        raise HTTPException(status_code=404, detail="unknown command")
    store.set_drop_receipt(command_id)
    return {"ok": True, "command_id": command_id, "state": row["state"]}


@app.get("/api/executions")
def executions(limit: int = 100) -> list[dict]:
    return [receipt_payload(r) for r in store.list(limit)]
