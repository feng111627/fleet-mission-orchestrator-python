"""内存状态存储：任务、机器人、资源租约，全部在单锁下保证自洽。"""
from __future__ import annotations

import threading

from .models import Mission, ResourceLease, Robot


class FleetStore:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.missions: dict[str, Mission] = {}
        self.idempotency_index: dict[str, str] = {}  # key -> mission_id
        self.robots: dict[str, Robot] = {}
        self.leases: dict[str, ResourceLease] = {}   # resource_id -> lease

    def reset(self) -> None:
        raise NotImplementedError
