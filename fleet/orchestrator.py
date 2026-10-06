"""编排器：任务图提交（幂等）、派发、暂停/取消/抢占、异常恢复。

一致性约定：所有状态迁移都在 store.lock 内完成；
任务离开活动态（DISPATCHED/RUNNING/PAUSED）时，必须同时
释放其资源租约并把机器人置回 IDLE，保证三方状态自洽。
"""
from __future__ import annotations

import uuid

from .models import (
    DispatchFailure,
    Mission,
    RobotState,
    Task,
    TaskSpec,
    TaskState,
)
from .scheduler import Scheduler
from .store import FleetStore

ACTIVE_STATES = {TaskState.DISPATCHED, TaskState.RUNNING, TaskState.PAUSED}
TERMINAL_STATES = {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}


class DependencyError(ValueError):
    """任务图非法：未知依赖或依赖成环。"""


class Orchestrator:
    def __init__(self, store: FleetStore, scheduler: Scheduler) -> None:
        self.store = store
        self.scheduler = scheduler

    # ---------- 提交 ----------

    def submit_mission(self, idempotency_key: str, task_specs: list[TaskSpec]) -> str:
        """提交任务图。相同 idempotency_key 重复提交返回已有 mission_id，不重复调度。"""
        with self.store.lock:
            existing = self.store.idempotency_index.get(idempotency_key)
            if existing is not None:
                return existing

            self._validate_graph(task_specs)
            mission = Mission(mission_id=uuid.uuid4().hex, idempotency_key=idempotency_key)
            for spec in task_specs:
                mission.tasks[spec.task_id] = Task(spec=spec)
            self.store.missions[mission.mission_id] = mission
            self.store.idempotency_index[idempotency_key] = mission.mission_id
            self._refresh_mission(mission)
            return mission.mission_id

    @staticmethod
    def _validate_graph(task_specs: list[TaskSpec]) -> None:
        ids = [s.task_id for s in task_specs]
        if len(ids) != len(set(ids)):
            raise DependencyError(f"duplicate task_id in graph: {sorted({i for i in ids if ids.count(i) > 1})}")
        by_id = {s.task_id: s for s in task_specs}
        for spec in task_specs:
            for dep in spec.depends_on:
                if dep not in by_id:
                    raise DependencyError(f"task {spec.task_id} depends on unknown task {dep}")
        # Kahn 拓扑排序检测环
        indegree = {s.task_id: len(s.depends_on) for s in task_specs}
        queue = [tid for tid, d in indegree.items() if d == 0]
        visited = 0
        while queue:
            tid = queue.pop()
            visited += 1
            for spec in task_specs:
                if tid in spec.depends_on:
                    indegree[spec.task_id] -= 1
                    if indegree[spec.task_id] == 0:
                        queue.append(spec.task_id)
        if visited != len(task_specs):
            cyclic = sorted(tid for tid, d in indegree.items() if d > 0)
            raise DependencyError(f"dependency cycle detected among tasks: {cyclic}")

    # ---------- 状态推进 ----------

    def _refresh_mission(self, mission: Mission) -> None:
        """在锁内：级联取消 -> 提升 READY -> 按优先级派发。"""
        changed = True
        while changed:
            changed = False
            for task in mission.tasks.values():
                if task.state != TaskState.PENDING:
                    continue
                dep_states = [mission.tasks[d].state for d in task.spec.depends_on]
                if any(s in (TaskState.FAILED, TaskState.CANCELLED) for s in dep_states):
                    task.state = TaskState.CANCELLED
                    task.last_error = "dependency failed or cancelled"
                    changed = True
                elif all(s == TaskState.SUCCEEDED for s in dep_states):
                    task.state = TaskState.READY
                    changed = True

        ready = [t for t in mission.tasks.values() if t.state == TaskState.READY]
        ready.sort(key=lambda t: (-t.spec.priority, t.spec.task_id))
        for task in ready:
            self._dispatch_with_preemption(mission, task)

    def _dispatch_with_preemption(self, mission: Mission, task: Task) -> None:
        """派发失败且原因是机器人/资源被占时，抢占优先级更低的活动任务后重试。"""
        for _ in range(len(mission.tasks) + 1):
            ok, reason = self.scheduler.try_dispatch(task)
            if ok:
                return
            if reason not in (DispatchFailure.ROBOT_BUSY, DispatchFailure.RESOURCE_BUSY):
                return
            victim = self._pick_preemption_victim(mission, task, reason)
            if victim is None:
                return
            self._release_task(victim, TaskState.PREEMPTED, "preempted by higher priority task")
            victim.state = TaskState.READY  # 被抢占任务回退为待调度，不消耗重试次数

    def _pick_preemption_victim(self, mission: Mission, task: Task, reason: DispatchFailure) -> Task | None:
        # 抢占是全局的：低优先级的活动任务可能属于其他 mission
        candidates = [
            t for m in self.store.missions.values() for t in m.tasks.values()
            if t.state in ACTIVE_STATES and t.spec.priority < task.spec.priority
        ]
        if reason == DispatchFailure.RESOURCE_BUSY:
            needed = set(task.spec.required_resources)
            candidates = [
                t for t in candidates
                if needed & {rid for rid, l in self.store.leases.items() if l.owner_task_id == t.spec.task_id}
            ]
        elif reason == DispatchFailure.ROBOT_BUSY:
            candidates = [
                t for t in candidates
                if t.robot_id is not None
                and task.spec.capability in self.store.robots[t.robot_id].capabilities
            ]
        if not candidates:
            return None
        return min(candidates, key=lambda t: (t.spec.priority, t.spec.task_id))

    def _release_task(self, task: Task, new_state: TaskState, error: str | None = None) -> None:
        """释放任务占用的机器人与资源，并设置终态/中间态。"""
        self.store.release_leases_for(task.spec.task_id)
        if task.robot_id is not None:
            robot = self.store.robots.get(task.robot_id)
            if robot is not None and robot.current_task_id == task.spec.task_id:
                robot.current_task_id = None
                if robot.state == RobotState.BUSY:
                    robot.state = RobotState.IDLE  # OFFLINE/FAULT 状态保持不变
        task.robot_id = None
        task.state = new_state
        task.last_error = error

    # ---------- 外部操作 ----------

    def pause_task(self, task_id: str) -> None:
        with self.store.lock:
            _, task = self._require_task(task_id)
            if task.state in (TaskState.DISPATCHED, TaskState.RUNNING):
                task.state = TaskState.PAUSED

    def resume_task(self, task_id: str) -> None:
        with self.store.lock:
            _, task = self._require_task(task_id)
            if task.state == TaskState.PAUSED:
                task.state = TaskState.RUNNING

    def cancel_task(self, task_id: str) -> None:
        with self.store.lock:
            mission, task = self._require_task(task_id)
            if task.state in TERMINAL_STATES:
                return
            self._release_task(task, TaskState.CANCELLED, "cancelled by user")
            self._refresh_mission(mission)

    # ---------- 机器人事件与异常恢复 ----------

    def report_robot_event(self, robot_id: str, event: str) -> None:
        """事件：started / succeeded / failed / offline / online / fault。"""
        with self.store.lock:
            robot = self.store.robots[robot_id]
            mission, task = (None, None)
            if robot.current_task_id is not None:
                mission, task = self.store.find_task(robot.current_task_id)

            if event == "started":
                if task is not None and task.state == TaskState.DISPATCHED:
                    task.state = TaskState.RUNNING
                return
            if event == "online":
                if robot.state in (RobotState.OFFLINE, RobotState.FAULT):
                    robot.state = RobotState.IDLE
                self._refresh_all()
                return

            if event == "succeeded" and task is not None:
                self._release_task(task, TaskState.SUCCEEDED)
            elif event in ("failed", "offline", "fault"):
                if event == "offline":
                    robot.state = RobotState.OFFLINE
                elif event == "fault":
                    robot.state = RobotState.FAULT
                if task is not None and task.state in ACTIVE_STATES:
                    self._handle_task_failure(task)
            else:
                return

            if mission is not None:
                self._refresh_mission(mission)
            self._refresh_all()

    def _handle_task_failure(self, task: Task) -> None:
        """重试策略：未超次数则释放后回退 READY 重新调度，否则 FAILED 并级联取消。"""
        if task.attempts <= task.spec.max_retries:
            self._release_task(task, TaskState.READY, "retrying after failure")
        else:
            self._release_task(task, TaskState.FAILED, "retries exhausted")

    def _refresh_all(self) -> None:
        for mission in self.store.missions.values():
            self._refresh_mission(mission)

    # ---------- 查询 ----------

    def _require_task(self, task_id: str):
        mission, task = self.store.find_task(task_id)
        if task is None:
            raise KeyError(f"unknown task: {task_id}")
        return mission, task

    def snapshot(self) -> dict:
        with self.store.lock:
            return {
                "missions": {
                    m.mission_id: {
                        tid: {"state": t.state.value, "robot_id": t.robot_id,
                              "attempts": t.attempts, "last_error": t.last_error}
                        for tid, t in m.tasks.items()
                    }
                    for m in self.store.missions.values()
                },
                "robots": {
                    rid: {"state": r.state.value, "battery": r.battery,
                          "current_task_id": r.current_task_id}
                    for rid, r in self.store.robots.items()
                },
                "leases": {rid: l.owner_task_id for rid, l in self.store.leases.items()},
            }
