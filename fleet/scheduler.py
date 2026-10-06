"""调度器：按能力/电量/资源占用挑选执行者并原子占用资源。"""
from __future__ import annotations

from .models import DispatchFailure, RobotState, Task, TaskState
from .store import FleetStore


class Scheduler:
    """所有决策都在 store.lock 内完成，并发请求被串行化，
    因此不会出现两个任务同时抢到同一机器人或同一资源。"""

    def __init__(self, store: FleetStore) -> None:
        self.store = store

    def try_dispatch(self, task: Task) -> tuple[bool, DispatchFailure | None]:
        """尝试为单个任务分配机器人与资源。

        返回 (True, None) 表示任务进入 DISPATCHED；
        返回 (False, 原因) 表示未占用任何机器人/资源，可安全重试。
        """
        spec = task.spec
        with self.store.lock:
            capable = [
                r for r in self.store.robots.values()
                if spec.capability in r.capabilities
            ]
            if not capable:
                return False, DispatchFailure.NO_CAPABLE_ROBOT

            idle = [r for r in capable if r.state == RobotState.IDLE]
            if not idle:
                if all(r.state in (RobotState.OFFLINE, RobotState.FAULT) for r in capable):
                    return False, DispatchFailure.ROBOT_OFFLINE
                return False, DispatchFailure.ROBOT_BUSY

            charged = [r for r in idle if r.battery >= spec.min_battery]
            if not charged:
                return False, DispatchFailure.BATTERY_TOO_LOW

            if not self.store.try_acquire_leases(task.spec.task_id, spec.required_resources):
                return False, DispatchFailure.RESOURCE_BUSY

            # 选电量最高的机器人，简单且确定性
            robot = max(charged, key=lambda r: r.battery)
            robot.state = RobotState.BUSY
            robot.current_task_id = spec.task_id
            task.state = TaskState.DISPATCHED
            task.robot_id = robot.robot_id
            task.attempts += 1
            task.last_error = None
            return True, None
