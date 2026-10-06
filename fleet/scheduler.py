"""调度器：按能力、电量、作业区占用挑选执行者。"""
from __future__ import annotations

from .errors import RejectReason
from .models import Robot, Task
from .store import StateStore

MIN_BATTERY = 20.0  # 电量低于该值不可被派发


class Scheduler:
    """无状态选人逻辑；所有方法都假定调用方已持有 store.lock。"""

    def __init__(self, store: StateStore) -> None:
        self._store = store

    def pick_robot(self, task: Task) -> tuple[Robot | None, RejectReason | None]:
        """为任务挑选机器人，返回 (robot, None) 或 (None, 拒绝原因)。

        规则：
        1. 必须具备任务所需能力，否则 NO_CAPABLE_ROBOT；
        2. 候选中电量须 >= MIN_BATTERY，否则 BATTERY_TOO_LOW；
        3. 作业区须空闲，否则 ZONE_OCCUPIED；
        4. 机器人须 IDLE，全部繁忙则 ROBOT_BUSY；
        5. 多个合格候选时取电量最高者（并列取 robot_id 字典序小者，保证确定性）。
        """
        store = self._store
        capable = [r for r in store.robots.values() if task.capability in r.capabilities]
        if not capable:
            return None, RejectReason.NO_CAPABLE_ROBOT

        online = [r for r in capable if r.status != r.status.OFFLINE and r.status != r.status.FAULT]
        if not online:
            return None, RejectReason.ROBOT_UNAVAILABLE

        charged = [r for r in online if r.battery >= MIN_BATTERY]
        if not charged:
            return None, RejectReason.BATTERY_TOO_LOW

        zone = store.zones.get(task.zone)
        if zone is not None and not zone.free and zone.occupied_by != task.task_id:
            return None, RejectReason.ZONE_OCCUPIED

        idle = [r for r in charged if r.available]
        if not idle:
            return None, RejectReason.ROBOT_BUSY

        # 电量最高者优先；并列时取 robot_id 较小者，保证确定性
        best = sorted(idle, key=lambda r: (-r.battery, r.robot_id))[0]
        return best, None
