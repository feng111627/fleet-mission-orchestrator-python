"""核心数据模型：任务、机器人、作业区（独占资源）与各类状态枚举。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class TaskStatus(str, Enum):
    PENDING = "pending"        # 已入图，依赖未满足
    READY = "ready"            # 依赖已满足，等待派发
    DISPATCHED = "dispatched"  # 已指派机器人，执行中
    PAUSED = "paused"          # 已暂停（保留占用）
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class RobotStatus(str, Enum):
    IDLE = "idle"
    BUSY = "busy"
    OFFLINE = "offline"
    FAULT = "fault"


class FaultPolicy(str, Enum):
    RETRY = "retry"        # 机器人故障/掉线后重新排队重试
    ROLLBACK = "rollback"  # 回退：任务直接标记失败并释放占用


TERMINAL_STATUSES = {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}


@dataclass
class Task:
    task_id: str
    capability: str            # 所需能力标签，如 "delivery" / "cleaning"
    zone: str                  # 需要独占的作业区
    priority: int = 0          # 数值越大优先级越高
    dependencies: list[str] = field(default_factory=list)
    fault_policy: FaultPolicy = FaultPolicy.RETRY
    max_retries: int = 2
    status: TaskStatus = TaskStatus.PENDING
    retries: int = 0
    robot_id: str | None = None
    error: str | None = None
    created_at: float = field(default_factory=time.time)


@dataclass
class Robot:
    robot_id: str
    capabilities: set[str]
    battery: float             # 0~100
    status: RobotStatus = RobotStatus.IDLE
    current_task: str | None = None

    @property
    def available(self) -> bool:
        return self.status == RobotStatus.IDLE


@dataclass
class Zone:
    """独占作业区：同一时间只能被一个任务占用。"""
    zone_id: str
    occupied_by: str | None = None  # task_id

    @property
    def free(self) -> bool:
        return self.occupied_by is None


@dataclass
class Submission:
    """一次任务图提交（按 idempotency_key 去重）。"""
    idempotency_key: str
    task_ids: list[str]
    created_at: float = field(default_factory=time.time)
