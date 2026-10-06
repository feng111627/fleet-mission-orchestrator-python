"""领域模型：任务、任务图、机器人、资源租约与状态枚举。"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class TaskState(str, Enum):
    PENDING = "PENDING"        # 已入图，依赖未满足
    READY = "READY"            # 依赖满足，等待派发
    DISPATCHED = "DISPATCHED"  # 已派发给机器人
    RUNNING = "RUNNING"        # 机器人执行中
    PAUSED = "PAUSED"          # 已暂停
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    PREEMPTED = "PREEMPTED"    # 被高优先级任务抢占


class RobotState(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    OFFLINE = "OFFLINE"
    FAULT = "FAULT"


class DispatchFailure(str, Enum):
    """派发/抢占失败原因，便于调用方区分。"""
    NO_CAPABLE_ROBOT = "NO_CAPABLE_ROBOT"
    BATTERY_TOO_LOW = "BATTERY_TOO_LOW"
    RESOURCE_BUSY = "RESOURCE_BUSY"
    ROBOT_BUSY = "ROBOT_BUSY"
    ROBOT_OFFLINE = "ROBOT_OFFLINE"
    DEPENDENCY_UNSATISFIED = "DEPENDENCY_UNSATISFIED"


@dataclass(frozen=True)
class TaskSpec:
    """提交时的任务描述（不可变）。"""
    task_id: str
    capability: str                 # 需要的机器人能力
    required_resources: tuple[str, ...] = ()   # 需要独占的资源
    depends_on: tuple[str, ...] = ()
    priority: int = 0               # 越大优先级越高
    min_battery: float = 20.0
    max_retries: int = 1


@dataclass
class Task:
    spec: TaskSpec
    state: TaskState = TaskState.PENDING
    robot_id: str | None = None
    attempts: int = 0
    last_error: str | None = None
    updated_at: float = field(default_factory=time.time)


@dataclass
class Robot:
    robot_id: str
    capabilities: frozenset[str]
    battery: float = 100.0
    state: RobotState = RobotState.IDLE
    current_task_id: str | None = None


@dataclass
class ResourceLease:
    """独占资源租约：同一资源同一时间只能有一个持有者。"""
    resource_id: str
    owner_task_id: str


@dataclass
class Mission:
    """一次提交（任务图）。idempotency_key 用于去重。"""
    mission_id: str
    idempotency_key: str
    tasks: dict[str, Task] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
