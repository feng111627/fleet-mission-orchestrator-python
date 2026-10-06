"""HTTP API 层：把编排器暴露为 REST 接口。"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from .models import Robot, TaskSpec
from .orchestrator import DependencyError, Orchestrator
from .scheduler import Scheduler
from .store import FleetStore


class RobotIn(BaseModel):
    robot_id: str
    capabilities: list[str]
    battery: float = 100.0


class TaskIn(BaseModel):
    task_id: str
    capability: str
    required_resources: list[str] = Field(default_factory=list)
    depends_on: list[str] = Field(default_factory=list)
    priority: int = 0
    min_battery: float = 20.0
    max_retries: int = 1


class MissionIn(BaseModel):
    idempotency_key: str
    tasks: list[TaskIn]


class EventIn(BaseModel):
    event: str  # started / succeeded / failed / offline / online / fault


def create_app() -> FastAPI:
    store = FleetStore()
    scheduler = Scheduler(store)
    orchestrator = Orchestrator(store, scheduler)
    app = FastAPI(title="fleet-mission-orchestrator")
    app.state.orchestrator = orchestrator
    app.state.store = store

    def orch(request: Request) -> Orchestrator:
        return request.app.state.orchestrator

    @app.post("/robots")
    def add_robot(body: RobotIn):
        store.add_robot(Robot(
            robot_id=body.robot_id,
            capabilities=frozenset(body.capabilities),
            battery=body.battery,
        ))
        return {"ok": True}

    @app.post("/missions")
    def submit_mission(body: MissionIn, request: Request):
        specs = [
            TaskSpec(
                task_id=t.task_id,
                capability=t.capability,
                required_resources=tuple(t.required_resources),
                depends_on=tuple(t.depends_on),
                priority=t.priority,
                min_battery=t.min_battery,
                max_retries=t.max_retries,
            )
            for t in body.tasks
        ]
        try:
            mission_id = orch(request).submit_mission(body.idempotency_key, specs)
        except DependencyError as exc:
            raise HTTPException(status_code=400, detail=str(exc))
        return {"mission_id": mission_id}

    @app.post("/tasks/{task_id}/pause")
    def pause_task(task_id: str, request: Request):
        try:
            orch(request).pause_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"ok": True}

    @app.post("/tasks/{task_id}/resume")
    def resume_task(task_id: str, request: Request):
        try:
            orch(request).resume_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"ok": True}

    @app.post("/tasks/{task_id}/cancel")
    def cancel_task(task_id: str, request: Request):
        try:
            orch(request).cancel_task(task_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        return {"ok": True}

    @app.post("/robots/{robot_id}/events")
    def robot_event(robot_id: str, body: EventIn, request: Request):
        if robot_id not in store.robots:
            raise HTTPException(status_code=404, detail=f"unknown robot: {robot_id}")
        orch(request).report_robot_event(robot_id, body.event)
        return {"ok": True}

    @app.get("/state")
    def get_state(request: Request):
        return orch(request).snapshot()

    return app
