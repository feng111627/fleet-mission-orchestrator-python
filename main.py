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
    # 多作业区任务：声明执行期间需要同时独占的一整组区；
    # 省略或为空时退化为单区 [zone]，老用法不变。
    zones: list[str] | None = None


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
        tasks = []
        for t in body.tasks:
            data = t.model_dump()
            # zones 省略时由 Task 退化为 [zone]；显式空列表视为非法
            if data.get("zones") is not None and len(data["zones"]) == 0:
                raise HTTPException(
                    status_code=400,
                    detail="zones must contain at least one zone id")
            tasks.append(Task(**data))
        ids = orch.submit_graph(body.idempotency_key, tasks)
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
                                  "zones": t.required_zones,
                                  "retries": t.retries, "error": t.error,
                                  "blocked_resource": t.blocked_resource}
                      for t in s.tasks.values()},
            "robots": {r.robot_id: {"status": r.status.value, "battery": r.battery,
                                    "current_task": r.current_task}
                       for r in s.robots.values()},
            "zones": {z.zone_id: {"occupied_by": z.occupied_by}
                      for z in s.zones.values()},
        }


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
