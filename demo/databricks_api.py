from __future__ import annotations

import itertools
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel

from demo.config import settings
from demo.stages import bronze_ingest, replenishment_calc, silver_conform


app = FastAPI(title="Local Databricks Jobs API", version="2.1-demo")
counter = itertools.count(1000)
lock = threading.Lock()
runs: dict[int, dict[str, Any]] = {}

JOB_MAP = {
    440: ("bronze_ingest", bronze_ingest),
    441: ("silver_conform", silver_conform),
    447: ("replenishment_calc", replenishment_calc),
}


class RunNowRequest(BaseModel):
    job_id: int
    idempotency_token: str | None = None
    notebook_params: dict[str, str] | None = None
    job_parameters: dict[str, str] | None = None
    python_params: list[str] | None = None


def _execute(run_id: int, job_id: int, params: dict[str, str]) -> None:
    stage_name, function = JOB_MAP[job_id]
    with lock:
        runs[run_id]["state"] = {"life_cycle_state": "RUNNING", "state_message": stage_name}
        runs[run_id]["start_time"] = int(datetime.now(timezone.utc).timestamp() * 1000)
    try:
        trading_date = datetime.strptime(
            params.get("trading_date", settings.trading_date.isoformat()), "%Y-%m-%d"
        ).date()
        result = function(trading_date)
    except Exception as exc:
        with lock:
            runs[run_id]["state"] = {
                "life_cycle_state": "TERMINATED",
                "result_state": "FAILED",
                "state_message": str(exc),
            }
            runs[run_id]["error"] = str(exc)
            runs[run_id]["end_time"] = int(datetime.now(timezone.utc).timestamp() * 1000)
    else:
        with lock:
            runs[run_id]["state"] = {
                "life_cycle_state": "TERMINATED",
                "result_state": "SUCCESS",
                "state_message": "Completed",
            }
            runs[run_id]["result"] = result
            runs[run_id]["end_time"] = int(datetime.now(timezone.utc).timestamp() * 1000)


@app.get("/health")
def health() -> dict:
    return {"status": "healthy", "jobs": sorted(JOB_MAP)}


@app.post("/api/2.1/jobs/run-now")
@app.post("/api/2.0/jobs/run-now")
@app.post("/api/2.2/jobs/run-now")
def run_now(request: RunNowRequest, background_tasks: BackgroundTasks) -> dict:
    if request.job_id not in JOB_MAP:
        raise HTTPException(404, f"Unknown local Databricks job_id {request.job_id}")
    params = request.job_parameters or request.notebook_params or {}
    if request.idempotency_token:
        with lock:
            for run_id, run in runs.items():
                if run.get("idempotency_token") == request.idempotency_token:
                    return {"run_id": run_id, "number_in_job": 1}
    run_id = next(counter)
    with lock:
        runs[run_id] = {
            "run_id": run_id,
            "run_page_url": f"http://databricks-local:8000/demo/runs/{run_id}",
            "job_id": request.job_id,
            "run_name": JOB_MAP[request.job_id][0],
            "idempotency_token": request.idempotency_token,
            "state": {"life_cycle_state": "PENDING", "state_message": "Queued"},
            "tasks": [],
        }
    background_tasks.add_task(_execute, run_id, request.job_id, params)
    return {"run_id": run_id, "number_in_job": 1}


@app.get("/api/2.1/jobs/runs/get")
@app.get("/api/2.0/jobs/runs/get")
@app.get("/api/2.2/jobs/runs/get")
def get_run(run_id: int) -> dict:
    with lock:
        run = runs.get(run_id)
        if run is None:
            raise HTTPException(404, f"Unknown run_id {run_id}")
        return dict(run)


@app.get("/api/2.1/jobs/runs/get-output")
@app.get("/api/2.0/jobs/runs/get-output")
@app.get("/api/2.2/jobs/runs/get-output")
def get_output(run_id: int) -> dict:
    with lock:
        run = runs.get(run_id)
        if run is None:
            raise HTTPException(404, f"Unknown run_id {run_id}")
        return {
            "metadata": dict(run),
            "notebook_output": {"result": str(run.get("result", "")), "truncated": False},
            "error": run.get("error"),
        }


@app.get("/api/2.1/jobs/list")
@app.get("/api/2.2/jobs/list")
def list_jobs() -> dict:
    return {
        "jobs": [
            {"job_id": job_id, "settings": {"name": name}}
            for job_id, (name, _) in JOB_MAP.items()
        ],
        "has_more": False,
    }
