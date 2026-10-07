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
from .scheduler import MIN_BATTERY, Scheduler
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

    # ---- 派发（含抢占） ----
    def dispatch_ready(self) -> list[str]:
        """按优先级从高到低派发 READY 任务；全部繁忙时尝试抢占低优先级任务。"""
        dispatched: list[str] = []
        with self._store.lock:
            ready = sorted(
                (t for t in self._store.tasks.values() if t.status == TaskStatus.READY),
                key=lambda t: (-t.priority, t.created_at),
            )
            for task in ready:
                if task.status != TaskStatus.READY:
                    continue  # 可能在本轮被抢占逻辑改动
                task.blocked_resource = None
                task.blocked_by = None
                robot, reason = self._scheduler.pick_robot(task)
                if robot is None and reason is not None and reason.value in (
                        "robot_busy", "zone_occupied"):
                    # 机器人全忙或作业区被占时，仅就本次所需资源尝试原子抢占
                    if self._try_preempt(task):
                        robot, reason = self._scheduler.pick_robot(task)
                if robot is None:
                    task.error = reason.value if reason else "unknown"
                    logger.info("dispatch rejected: %s reason=%s blocked_resource=%s "
                                "blocked_by=%s",
                                task.task_id, task.error,
                                task.blocked_resource, task.blocked_by)
                    continue
                self._assign(task, robot)
                dispatched.append(task.task_id)
        return dispatched

    def _assign(self, task: Task, robot: Robot) -> None:
        """原子地建立 任务<->机器人<->全部作业区 的占用关系。调用方须持有锁。

        调用方必须已确认 task.required_zones 全部空闲；这里一次性全部占上，
        中间不会留下"占了一部分"的状态（整个过程在同一把锁内）。
        """
        task.status = TaskStatus.DISPATCHED
        task.robot_id = robot.robot_id
        task.error = None
        task.blocked_resource = None
        task.blocked_by = None
        robot.status = RobotStatus.BUSY
        robot.current_task = task.task_id
        for zid in task.required_zones:
            zone = self._store.zones.get(zid)
            if zone is not None:
                zone.occupied_by = task.task_id
        logger.info("task dispatched: %s -> robot=%s zones=%s",
                    task.task_id, robot.robot_id, task.required_zones)

    def _release(self, task: Task, release_zones: bool = True) -> None:
        """一次性释放任务占用的机器人与全部作业区。调用方须持有锁。

        只释放确实登记在该任务名下的区，避免误释放被别人重新占走的资源。
        """
        if task.robot_id is not None:
            robot = self._store.robots.get(task.robot_id)
            if robot is not None and robot.current_task == task.task_id:
                robot.current_task = None
                if robot.status == RobotStatus.BUSY:
                    robot.status = RobotStatus.IDLE
            task.robot_id = None
        if release_zones:
            for zid in task.required_zones:
                zone = self._store.zones.get(zid)
                if zone is not None and zone.occupied_by == task.task_id:
                    zone.occupied_by = None

    def _try_preempt(self, task: Task) -> bool:
        """为任务尝试抢占资源，成功返回 True。调用方须持有锁。

        抢占范围严格限定在"本次派发真正需要的资源"上：
        - 任务所需作业区的当前占用者；
        - 没有空闲合格机器人时，一台正在执行更低优先级任务的合格机器人。

        原子性与优先级边界：
        - 先做干跑分析、收集全部待动任务；只要其中有一个优先级 >= 本任务
          （同优先级或更高优先级挡路），就整体不抢，一个都不动；
        - 与所需资源无关、在别的区/别的机器人上跑的任务绝不会被牵连；
        - 校验全部通过后，才一次性释放所有受害者并让其重新排队。
        无法抢占时把卡住的资源（blocked_resource）与占用者（blocked_by）写到任务上。
        """
        store = self._store

        # 1) 收集占用所需作业区的任务（只收非终态持有者）
        zone_victims: dict[str, tuple[Task, str]] = {}
        for zid in task.required_zones:
            zone = store.zones.get(zid)
            if zone is None or zone.free or zone.occupied_by == task.task_id:
                continue
            holder = store.tasks.get(zone.occupied_by)
            if holder is None or holder.status in TERMINAL_STATUSES:
                continue
            zone_victims.setdefault(holder.task_id, (holder, zid))

        # 2) 区占用者里只要有同优先级/更高优先级的，就一个都不能抢
        for holder, zid in zone_victims.values():
            if holder.priority >= task.priority:
                task.blocked_resource = zid
                task.blocked_by = holder.task_id
                logger.info(
                    "preempt aborted: %s blocked on zone %s held by %s "
                    "(holder priority %s >= %s)",
                    task.task_id, zid, holder.task_id, holder.priority, task.priority)
                return False

        # 3) 确定机器人：优先用现成空闲合格机器人；否则需要再抢一台
        charged_online = [
            r for r in store.robots.values()
            if task.capability in r.capabilities
            and r.status not in (RobotStatus.OFFLINE, RobotStatus.FAULT)
            and r.battery >= MIN_BATTERY
        ]
        victims: dict[str, Task] = {tid: v for tid, (v, _zid) in zone_victims.items()}
        idle = [r for r in charged_online if r.available]
        if not idle:
            # 区占用者自己的机器人抢占后会自然空出，优先复用，避免牵连无关任务
            holder_ids = set(zone_victims)
            holder_robots = [r for r in charged_online
                             if r.status == RobotStatus.BUSY and r.current_task in holder_ids]
            busy_pool = holder_robots or [
                r for r in charged_online if r.status == RobotStatus.BUSY
                and r.current_task is not None]
            robot_victim: Task | None = None
            for r in sorted(busy_pool,
                            key=lambda x: (store.tasks[x.current_task].priority, x.robot_id)):
                owner = store.tasks[r.current_task]
                if owner.priority < task.priority:
                    robot_victim = owner
                    break
            if robot_victim is None:
                # 没有可抢的低优先级合格机器人：记录是哪台机器人、谁在用
                blocked_robot = next(
                    (r for r in charged_online if r.status == RobotStatus.BUSY), None)
                if blocked_robot is not None and blocked_robot.current_task is not None:
                    owner = store.tasks[blocked_robot.current_task]
                    task.blocked_resource = blocked_robot.robot_id
                    task.blocked_by = owner.task_id
                    logger.info(
                        "preempt aborted: %s blocked on robot %s held by %s "
                        "(owner priority %s >= %s)",
                        task.task_id, blocked_robot.robot_id, owner.task_id,
                        owner.priority, task.priority)
                return False
            victims[robot_victim.task_id] = robot_victim

        # 4) 整体二次校验：任一受害者不是严格低优先级，则全部不动
        for victim in victims.values():
            if victim.priority >= task.priority:
                zid = next((z for z in task.required_zones
                            if (store.zones.get(z).occupied_by if store.zones.get(z) else None)
                            == victim.task_id), None)
                task.blocked_resource = zid or (victim.robot_id or "unknown")
                task.blocked_by = victim.task_id
                logger.info(
                    "preempt aborted atomically: %s cannot preempt %s (same/higher priority)",
                    task.task_id, victim.task_id)
                return False

        # 5) 校验通过：一次性释放全部受害者（每个都放开自己的机器人与全部作业区）
        logger.info("preempt: task=%s preempts victims=%s",
                    task.task_id, sorted(victims))
        for victim in victims.values():
            self._release(victim)
            victim.status = TaskStatus.READY  # 重新排队等待再次派发
            victim.error = f"preempted by {task.task_id}"
        return True

    # ---- 生命周期 ----
    def pause_task(self, task_id: str) -> None:
        """暂停执行中的任务：释放机器人回池，但保留全部作业区占用。"""
        with self._store.lock:
            task = self._get_task(task_id)
            if task.status != TaskStatus.DISPATCHED:
                return
            self._release(task, release_zones=False)
            task.status = TaskStatus.PAUSED
            logger.info("task paused: %s (zones %s kept)", task_id, task.required_zones)

    def resume_task(self, task_id: str) -> None:
        """恢复暂停的任务：放开保留的作业区，回到 READY 重新走派发流程。"""
        with self._store.lock:
            task = self._get_task(task_id)
            if task.status != TaskStatus.PAUSED:
                return
            for zid in task.required_zones:
                zone = self._store.zones.get(zid)
                if zone is not None and zone.occupied_by == task.task_id:
                    zone.occupied_by = None  # 重新走派发流程，由 _assign 重新占用
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
        """机器人异常时，按任务既定策略重试或回退。调用方须持有锁。

        机器人与该任务占用的全部作业区在这里一次性释放。
        """
        tid = robot.current_task
        robot.current_task = None
        if tid is None:
            return
        task = self._store.tasks[tid]
        task.robot_id = None
        for zid in task.required_zones:
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
