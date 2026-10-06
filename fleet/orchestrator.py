"""编排器：任务图提交（幂等）、派发、暂停/取消/抢占、异常恢复。"""
from __future__ import annotations

from .scheduler import Scheduler
from .store import FleetStore


class Orchestrator:
    def __init__(self, store: FleetStore, scheduler: Scheduler) -> None:
        self.store = store
        self.scheduler = scheduler

    def submit_mission(self, idempotency_key: str, task_specs: list) -> str:
        raise NotImplementedError

    def pause_task(self, task_id: str) -> None:
        raise NotImplementedError

    def cancel_task(self, task_id: str) -> None:
        raise NotImplementedError

    def report_robot_event(self, robot_id: str, event: str) -> None:
        raise NotImplementedError
