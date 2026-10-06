"""车队任务编排服务 HTTP API。"""
from __future__ import annotations

import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from fleet import (
    DependencyCycleError,
    FaultPolicy,
    Orchestrator,
    OrchestratorError,
    Robot,
    Task,
    Zone,
)

app = FastAPI(title="Fleet Mission Orchestrator")
orch = Orchestrator()


class RobotIn(BaseModel):
    robot_id: str
    capabilities: set[str]
    battery: float = Field(ge=0, le=100)


class ZoneIn(BaseModel):
    zone_id: str


class TaskIn(BaseModel):
    task_id: str
    capability: str
    zone: str
    priority: int = 0
    dependencies: list[str] = []
    fault_policy: FaultPolicy = FaultPolicy.RETRY
    max_retries: int = 2


class GraphIn(BaseModel):
    idempotency_key: str
    tasks: list[TaskIn]


class CompleteIn(BaseModel):
    success: bool = True


@app.post("/robots", status_code=201)
def register_robot(body: RobotIn):
    orch.register_robot(Robot(**body.model_dump()))
    return {"ok": True}


@app.post("/zones", status_code=201)
def register_zone(body: ZoneIn):
    orch.register_zone(Zone(**body.model_dump()))
    return {"ok": True}


@app.post("/graphs", status_code=201)
def submit_graph(body: GraphIn):
    try:
        ids = orch.submit_graph(body.idempotency_key,
                                [Task(**t.model_dump()) for t in body.tasks])
    except DependencyCycleError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except OrchestratorError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"task_ids": ids}


@app.post("/dispatch")
def dispatch():
    return {"dispatched": orch.dispatch_ready()}


@app.post("/tasks/{task_id}/pause")
def pause(task_id: str):
    orch.pause_task(task_id)
    return {"ok": True}


@app.post("/tasks/{task_id}/resume")
def resume(task_id: str):
    orch.resume_task(task_id)
    return {"ok": True}


@app.post("/tasks/{task_id}/cancel")
def cancel(task_id: str):
    orch.cancel_task(task_id)
    return {"ok": True}


@app.post("/tasks/{task_id}/complete")
def complete(task_id: str, body: CompleteIn):
    orch.complete_task(task_id, success=body.success)
    return {"ok": True}


@app.post("/robots/{robot_id}/offline")
def robot_offline(robot_id: str):
    orch.report_robot_offline(robot_id)
    return {"ok": True}


@app.post("/robots/{robot_id}/fault")
def robot_fault(robot_id: str):
    orch.report_robot_fault(robot_id)
    return {"ok": True}


@app.post("/robots/{robot_id}/back")
def robot_back(robot_id: str):
    orch.report_robot_back(robot_id)
    return {"ok": True}


@app.get("/state")
def state():
    s = orch.store
    with s.lock:
        return {
            "tasks": {t.task_id: {"status": t.status.value, "robot": t.robot_id,
                                  "retries": t.retries, "error": t.error}
                      for t in s.tasks.values()},
            "robots": {r.robot_id: {"status": r.status.value, "battery": r.battery,
                                    "current_task": r.current_task}
                       for r in s.robots.values()},
            "zones": {z.zone_id: {"occupied_by": z.occupied_by}
                      for z in s.zones.values()},
        }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
