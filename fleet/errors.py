"""可区分原因的业务错误，供并发抢占失败等场景向上传递。"""
from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    NO_CAPABLE_ROBOT = "no_capable_robot"      # 没有具备所需能力的机器人
    BATTERY_TOO_LOW = "battery_too_low"        # 有能力但电量不足
    ZONE_OCCUPIED = "zone_occupied"            # 作业区被其他任务占用（含被同级/高优先级挡住、抢不动）
    ROBOT_BUSY = "robot_busy"                  # 目标机器人正忙
    ROBOT_UNAVAILABLE = "robot_unavailable"    # 机器人掉线/故障
    LOST_RACE = "lost_race"                    # 并发竞争中落败（状态已被他人改变）
    INVALID_ZONES = "invalid_zones"            # 声明的作业区集合为空或未登记


class OrchestratorError(Exception):
    """业务错误基类。"""


class DependencyCycleError(OrchestratorError):
    """任务依赖成环。"""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"dependency cycle detected: {' -> '.join(cycle)}")


class UnknownTaskError(OrchestratorError):
    pass


class UnknownRobotError(OrchestratorError):
    pass


class DispatchRejected(OrchestratorError):
    """派发被拒绝，携带可区分的原因与卡住的资源说明。

    resource 形如 "zone:z-a@task-x(priority=1)" / "robot:r1@task-y(priority=9)"，
    让调用方能直接看出是被哪个资源、被什么优先级的任务挡住。
    """

    def __init__(self, task_id: str, reason: RejectReason, detail: str = "",
                 resource: str | None = None):
        self.task_id = task_id
        self.reason = reason
        self.resource = resource
        msg = f"dispatch rejected for {task_id}: {reason.value}"
        if resource:
            msg += f" resource={resource}"
        if detail:
            msg += f" {detail}"
        super().__init__(msg)
