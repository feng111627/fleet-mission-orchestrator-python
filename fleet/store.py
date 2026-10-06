"""内存状态存储：任务、机器人、资源租约，全部在单锁下保证自洽。"""
from __future__ import annotations

import threading

from .models import Mission, ResourceLease, Robot


class FleetStore:
    """所有读写都必须持有 self.lock，保证多线程下状态自洽。"""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.missions: dict[str, Mission] = {}
        self.idempotency_index: dict[str, str] = {}  # idempotency_key -> mission_id
        self.robots: dict[str, Robot] = {}
        self.leases: dict[str, ResourceLease] = {}   # resource_id -> lease

    def reset(self) -> None:
        with self.lock:
            self.missions.clear()
            self.idempotency_index.clear()
            self.robots.clear()
            self.leases.clear()

    def add_robot(self, robot: Robot) -> None:
        with self.lock:
            self.robots[robot.robot_id] = robot

    def try_acquire_leases(self, task_id: str, resource_ids: tuple[str, ...]) -> bool:
        """全有或全无地占用资源；任一资源已被占用则整体失败。"""
        with self.lock:
            for rid in resource_ids:
                lease = self.leases.get(rid)
                if lease is not None and lease.owner_task_id != task_id:
                    return False
            for rid in resource_ids:
                self.leases[rid] = ResourceLease(resource_id=rid, owner_task_id=task_id)
            return True

    def release_leases_for(self, task_id: str) -> list[str]:
        """释放某任务持有的全部租约，返回被释放的资源 id。"""
        with self.lock:
            released = [rid for rid, l in self.leases.items() if l.owner_task_id == task_id]
            for rid in released:
                del self.leases[rid]
            return released

    def find_task(self, task_id: str):
        with self.lock:
            for mission in self.missions.values():
                if task_id in mission.tasks:
                    return mission, mission.tasks[task_id]
            return None, None
