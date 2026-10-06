"""编排器：任务图提交（幂等/成环检测）、派发、暂停/取消/抢占、异常恢复。"""
from __future__ import annotations

from .models import FaultPolicy, Robot, Task, Zone
from .scheduler import Scheduler
from .store import StateStore


class Orchestrator:
    def __init__(self, store: StateStore | None = None) -> None:
        self._store = store or StateStore()
        self._scheduler = Scheduler(self._store)

    # ---- 车队与资源登记 ----
    def register_robot(self, robot: Robot) -> None:
        raise NotImplementedError

    def register_zone(self, zone: Zone) -> None:
        raise NotImplementedError

    # ---- 任务图提交 ----
    def submit_graph(self, idempotency_key: str, tasks: list[Task]) -> list[str]:
        """提交任务图；幂等键去重、依赖成环报错。骨架阶段为空实现。"""
        raise NotImplementedError

    # ---- 生命周期 ----
    def dispatch_ready(self) -> list[str]:
        """把所有 READY 任务尽量派发出去，返回成功派发的 task_id。"""
        raise NotImplementedError

    def pause_task(self, task_id: str) -> None:
        raise NotImplementedError

    def resume_task(self, task_id: str) -> None:
        raise NotImplementedError

    def cancel_task(self, task_id: str) -> None:
        raise NotImplementedError

    def complete_task(self, task_id: str, success: bool = True) -> None:
        raise NotImplementedError

    # ---- 异常恢复 ----
    def report_robot_offline(self, robot_id: str) -> None:
        raise NotImplementedError

    def report_robot_fault(self, robot_id: str) -> None:
        raise NotImplementedError

    def report_robot_back(self, robot_id: str) -> None:
        raise NotImplementedError
