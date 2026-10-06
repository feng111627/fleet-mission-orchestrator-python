"""内存状态仓库：任务、机器人、作业区、提交记录，统一在锁内读写保证自洽。"""
from __future__ import annotations

import threading

from .models import Robot, Submission, Task, Zone


class StateStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.tasks: dict[str, Task] = {}
        self.robots: dict[str, Robot] = {}
        self.zones: dict[str, Zone] = {}
        self.submissions: dict[str, Submission] = {}

    @property
    def lock(self) -> threading.RLock:
        return self._lock
