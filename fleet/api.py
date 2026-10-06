"""HTTP API 层：把编排器暴露为 REST 接口。"""
from __future__ import annotations

from fastapi import FastAPI

from .orchestrator import Orchestrator
from .scheduler import Scheduler
from .store import FleetStore


def create_app() -> FastAPI:
    store = FleetStore()
    scheduler = Scheduler(store)
    orchestrator = Orchestrator(store, scheduler)
    app = FastAPI(title="fleet-mission-orchestrator")
    app.state.orchestrator = orchestrator
    app.state.store = store
    return app
