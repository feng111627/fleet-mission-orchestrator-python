"""调度器：按能力/电量/资源占用挑选执行者并原子占用资源。"""
from __future__ import annotations

from .models import DispatchFailure, Task
from .store import FleetStore


class Scheduler:
    def __init__(self, store: FleetStore) -> None:
        self.store = store

    def try_dispatch(self, task: Task) -> tuple[bool, DispatchFailure | None]:
        """尝试为单个任务分配机器人与资源。骨架阶段为空实现。"""
        raise NotImplementedError
