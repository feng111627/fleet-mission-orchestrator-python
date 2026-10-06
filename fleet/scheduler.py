"""调度器：按能力、电量、作业区占用挑选执行者；负责抢占判定。"""
from __future__ import annotations

from .errors import RejectReason
from .models import Robot, Task
from .store import StateStore

MIN_BATTERY = 20.0  # 电量低于该值不可被派发


class Scheduler:
    def __init__(self, store: StateStore) -> None:
        self._store = store

    def pick_robot(self, task: Task) -> tuple[Robot | None, RejectReason | None]:
        """为任务挑选机器人。调用方须持有 store.lock。骨架阶段为空实现。"""
        raise NotImplementedError
