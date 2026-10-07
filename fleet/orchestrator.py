"""编排器：任务图提交（幂等/成环检测）、派发、暂停/取消/抢占、异常恢复。

所有状态变更都在 store.lock 内完成，保证任意时刻：
- 被占用的作业区一定属于某个非终态任务；
- DISPATCHED 任务的机器人一定处于 BUSY 且 current_task 指向它；
- 取消/失败/成功的任务不持有任何机器人或作业区。
"""
from __future__ import annotations

import logging

from .errors import DependencyCycleError, UnknownRobotError, UnknownTaskError
from .models import (
    TERMINAL_STATUSES,
    FaultPolicy,
    Robot,
    RobotStatus,
    Submission,
    Task,
    TaskStatus,
    Zone,
)
from .scheduler import Scheduler
from .store import StateStore

logger = logging.getLogger("fleet.orchestrator")


class Orchestrator:
    def __init__(self, store: StateStore | None = None) -> None:
        self._store = store or StateStore()
        self._scheduler = Scheduler(self._store)

    @property
    def store(self) -> StateStore:
        return self._store

    # ---- 车队与资源登记 ----
    def register_robot(self, robot: Robot) -> None:
        with self._store.lock:
            self._store.robots[robot.robot_id] = robot
            logger.info("robot registered: %s caps=%s", robot.robot_id, robot.capabilities)

    def register_zone(self, zone: Zone) -> None:
        with self._store.lock:
            self._store.zones[zone.zone_id] = zone

    # ---- 任务图提交 ----
    def submit_graph(self, idempotency_key: str, tasks: list[Task]) -> list[str]:
        """提交任务图。同一 idempotency_key 重复提交直接返回首次结果，不重复调度。"""
        with self._store.lock:
            existing = self._store.submissions.get(idempotency_key)
            if existing is not None:
                logger.info("duplicate submission ignored: key=%s", idempotency_key)
                return list(existing.task_ids)

            known = set(self._store.tasks) | {t.task_id for t in tasks}
            for t in tasks:
                for dep in t.dependencies:
                    if dep not in known:
                        raise UnknownTaskError(f"unknown dependency: {dep}")

            merged = dict(self._store.tasks)
            merged.update({t.task_id: t for t in tasks})
            self._check_cycle(merged)

            for t in tasks:
                self._store.tasks[t.task_id] = t
            self._store.submissions[idempotency_key] = Submission(
                idempotency_key=idempotency_key,
                task_ids=[t.task_id for t in tasks],
            )
            self._promote_ready()
            logger.info("graph submitted: key=%s tasks=%s", idempotency_key,
                        [t.task_id for t in tasks])
            return [t.task_id for t in tasks]

    @staticmethod
    def _check_cycle(tasks: dict[str, Task]) -> None:
        """DFS 检测依赖环，发现即抛 DependencyCycleError 并给出环路径。"""
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {tid: WHITE for tid in tasks}
        path: list[str] = []

        def dfs(tid: str) -> None:
            color[tid] = GRAY
            path.append(tid)
            for dep in tasks[tid].dependencies:
                if dep not in tasks:
                    continue
                if color[dep] == GRAY:
                    cycle = path[path.index(dep):] + [dep]
                    raise DependencyCycleError(cycle)
                if color[dep] == WHITE:
                    dfs(dep)
            path.pop()
            color[tid] = BLACK

        for tid in tasks:
            if color[tid] == WHITE:
                dfs(tid)

    def _promote_ready(self) -> None:
        """把依赖已全部成功的 PENDING 任务提升为 READY。调用方须持有锁。"""
        for t in self._store.tasks.values():
            if t.status != TaskStatus.PENDING:
                continue
            if all(self._store.tasks[d].status == TaskStatus.SUCCEEDED
                   for d in t.dependencies):
                t.status = TaskStatus.READY
                logger.info("task ready: %s", t.task_id)

    # ---- 派发（含精确抢占） ----
    def dispatch_ready(self) -> list[str]:
        """按优先级从高到低派发 READY 任务。

        每个任务先经 Scheduler 生成只读规划，再在同一把锁内原子执行：
        需要抢占多个任务时先确认全部可抢（严格低优先级），然后一次性全部抢占，
        再把该任务的整组作业区与机器人原子占上；任一挡路者不可抢则整体不动。
        """
        dispatched: list[str] = []
        with self._store.lock:
            ready = sorted(
                (t for t in self._store.tasks.values() if t.status == TaskStatus.READY),
                key=lambda t: (-t.priority, t.created_at),
            )
            for task in ready:
                if task.status != TaskStatus.READY:
                    continue  # 可能在本轮被更早的高优任务抢占
                plan = self._scheduler.plan_dispatch(task)
                if not plan.accepted:
                    task.error = plan.reason.value if plan.reason else "unknown"
                    task.blocked_resource = plan.resource
                    logger.info("dispatch rejected: %s reason=%s resource=%s",
                                task.task_id, task.error, plan.resource)
                    continue
                # 规划已保证 preempt 中的任务全部严格低优先级；这里原子执行
                for victim in plan.preempt:
                    if victim.status in (TaskStatus.DISPATCHED, TaskStatus.PAUSED):
                        self._preempt_victim(victim, by=task)
                self._assign(task, plan.robot)
                dispatched.append(task.task_id)
        return dispatched

    def _preempt_victim(self, victim: Task, by: Task) -> None:
        """抢占单个任务：一次性释放它占的机器人与**全部**作业区，重新排队。

        调用方须持有锁；调用前已确认 victim.priority < by.priority。
        """
        self._release(victim)
        victim.status = TaskStatus.READY
        victim.error = f"preempted by {by.task_id}"
        victim.blocked_resource = None
        logger.info("preempt: task=%s preempts victim=%s zones=%s",
                    by.task_id, victim.task_id, victim.required_zones)

    def _assign(self, task: Task, robot: Robot) -> None:
        """原子地建立 任务<->机器人<->整组作业区 的占用关系。调用方须持有锁。

        进入这里时所需作业区已被确认全部空闲（或已通过抢占清空），
        因此一次性全部占上，不存在先占一半的中间态对外可见。
        """
        task.status = TaskStatus.DISPATCHED
        task.robot_id = robot.robot_id
        task.error = None
        task.blocked_resource = None
        robot.status = RobotStatus.BUSY
        robot.current_task = task.task_id
        acquired: list[str] = []
        for zid in task.required_zones:
            zone = self._store.zones.get(zid)
            if zone is None:
                continue  # 未登记的区无需加锁，与单区时代一致
            zone.occupied_by = task.task_id
            acquired.append(zid)
        logger.info("task dispatched: %s -> robot=%s zones=%s",
                    task.task_id, robot.robot_id, acquired)

    def _release(self, task: Task, release_zones: bool = True) -> None:
        """一次性释放任务占用的机器人与**全部**作业区。调用方须持有锁。

        释放永远按"该任务当前声明的整组区 + 实际归属它的区"清理，
        保证取消/抢占/失败/掉线时不会只放一部分、不留悬挂占用。
        """
        if task.robot_id is not None:
            robot = self._store.robots.get(task.robot_id)
            if robot is not None and robot.current_task == task.task_id:
                robot.current_task = None
                if robot.status == RobotStatus.BUSY:
                    robot.status = RobotStatus.IDLE
            task.robot_id = None
        if release_zones:
            zone_ids = set(task.required_zones)
            # 兜底：扫一遍所有区，把仍挂在该任务名下的也清掉
            for zid, zone in self._store.zones.items():
                if zone.occupied_by == task.task_id:
                    zone_ids.add(zid)
            for zid in zone_ids:
                zone = self._store.zones.get(zid)
                if zone is not None and zone.occupied_by == task.task_id:
                    zone.occupied_by = None

    # ---- 生命周期 ----
    def pause_task(self, task_id: str) -> None:
        """暂停执行中的任务：释放机器人回池，但保留整组作业区占用。"""
        with self._store.lock:
            task = self._get_task(task_id)
            if task.status != TaskStatus.DISPATCHED:
                return
            self._release(task, release_zones=False)
            task.status = TaskStatus.PAUSED
            logger.info("task paused: %s (zones %s kept)",
                        task_id, task.required_zones)

    def resume_task(self, task_id: str) -> None:
        """恢复暂停的任务：放开保留的作业区，回到 READY 重新走整组原子派发。"""
        with self._store.lock:
            task = self._get_task(task_id)
            if task.status != TaskStatus.PAUSED:
                return
            for zid in task.required_zones:
                zone = self._store.zones.get(zid)
                if zone is not None and zone.occupied_by == task.task_id:
                    zone.occupied_by = None  # 重新走派发流程，由 _assign 重新整组占用
            task.status = TaskStatus.READY
            logger.info("task resumed: %s", task_id)

    def cancel_task(self, task_id: str) -> None:
        """取消任务：释放全部占用，状态置为 CANCELLED。"""
        with self._store.lock:
            task = self._get_task(task_id)
            if task.status in TERMINAL_STATUSES:
                return
            self._release(task)
            task.status = TaskStatus.CANCELLED
            logger.info("task cancelled: %s", task_id)

    def complete_task(self, task_id: str, success: bool = True) -> None:
        """执行结束上报：成功则推进依赖后继，失败按策略处理。"""
        with self._store.lock:
            task = self._get_task(task_id)
            if task.status not in (TaskStatus.DISPATCHED, TaskStatus.PAUSED):
                return
            self._release(task)
            if success:
                task.status = TaskStatus.SUCCEEDED
                logger.info("task succeeded: %s", task_id)
                self._promote_ready()
            else:
                self._handle_failure(task, "reported failure")

    # ---- 异常恢复 ----
    def report_robot_offline(self, robot_id: str) -> None:
        with self._store.lock:
            robot = self._get_robot(robot_id)
            robot.status = RobotStatus.OFFLINE
            logger.warning("robot offline: %s", robot_id)
            self._rescue_current_task(robot, "robot offline")

    def report_robot_fault(self, robot_id: str) -> None:
        with self._store.lock:
            robot = self._get_robot(robot_id)
            robot.status = RobotStatus.FAULT
            logger.warning("robot fault: %s", robot_id)
            self._rescue_current_task(robot, "robot fault")

    def report_robot_back(self, robot_id: str) -> None:
        with self._store.lock:
            robot = self._get_robot(robot_id)
            if robot.status in (RobotStatus.OFFLINE, RobotStatus.FAULT):
                robot.status = RobotStatus.IDLE
                logger.info("robot back online: %s", robot_id)

    def _rescue_current_task(self, robot: Robot, cause: str) -> None:
        """机器人异常时，一次性放开在执任务的机器人与整组作业区，再按策略处理。

        调用方须持有锁。
        """
        tid = robot.current_task
        robot.current_task = None
        if tid is None:
            return
        task = self._store.tasks[tid]
        task.robot_id = None
        # 整组作业区一次性释放（含兜底扫描，杜绝悬挂占用）
        zone_ids = set(task.required_zones)
        for zid, zone in self._store.zones.items():
            if zone.occupied_by == tid:
                zone_ids.add(zid)
        for zid in zone_ids:
            zone = self._store.zones.get(zid)
            if zone is not None and zone.occupied_by == tid:
                zone.occupied_by = None
        self._handle_failure(task, cause)

    def _handle_failure(self, task: Task, cause: str) -> None:
        """按 fault_policy 决定重试（回 READY）或回退（FAILED）。调用方须持有锁。"""
        if task.fault_policy == FaultPolicy.RETRY and task.retries < task.max_retries:
            task.retries += 1
            task.status = TaskStatus.READY
            task.error = f"{cause}; retry {task.retries}/{task.max_retries}"
            logger.warning("task retry scheduled: %s (%s)", task.task_id, task.error)
        else:
            task.status = TaskStatus.FAILED
            task.error = cause if task.fault_policy == FaultPolicy.ROLLBACK else \
                f"{cause}; retries exhausted"
            logger.warning("task failed: %s policy=%s cause=%s",
                           task.task_id, task.fault_policy.value, cause)

    # ---- 查询 ----
    def _get_task(self, task_id: str) -> Task:
        task = self._store.tasks.get(task_id)
        if task is None:
            raise UnknownTaskError(task_id)
        return task

    def _get_robot(self, robot_id: str) -> Robot:
        robot = self._store.robots.get(robot_id)
        if robot is None:
            raise UnknownRobotError(robot_id)
        return robot
