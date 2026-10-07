"""调度器：按能力、电量、作业区占用挑选执行者，并规划精确到资源的抢占。

所有方法都是只读分析、不修改状态，且假定调用方已持有 store.lock；
抢占的实际执行由编排器拿到 DispatchPlan 后在同一把锁内原子完成。
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .errors import RejectReason
from .models import Robot, RobotStatus, Task

MIN_BATTERY = 20.0  # 电量低于该值不可被派发


@dataclass
class DispatchPlan:
    """一次派发的完整规划：要么可执行，要么带可区分的拒绝原因。

    - robot 不为 None 且 preempt 为空：可直接派发；
    - robot 不为 None 且 preempt 非空：必须把 preempt 里的任务**一次性全部**
      抢占（它们都已确认严格低优先级），再把任务的整组作业区原子占上；
    - robot 为 None：拒绝派发，reason/resource 说明被哪个资源、什么情况卡住。
    """
    robot: Robot | None = None
    reason: RejectReason | None = None
    resource: str | None = None
    preempt: list[Task] = field(default_factory=list)

    @property
    def accepted(self) -> bool:
        return self.robot is not None


def _zone_resource(zone_id: str, task: Task) -> str:
    return f"zone:{zone_id}@{task.task_id}(priority={task.priority})"


def _robot_resource(robot: Robot, task: Task) -> str:
    return f"robot:{robot.robot_id}@{task.task_id}(priority={task.priority})"


class Scheduler:
    """无状态选人/抢占规划逻辑；所有方法都假定调用方已持有 store.lock。"""

    def __init__(self, store) -> None:
        self._store = store

    def plan_dispatch(self, task: Task) -> DispatchPlan:
        """为任务生成派发规划，判定顺序与单区时代保持一致：

        1. 无具备所需能力的机器人 → NO_CAPABLE_ROBOT；
        2. 候选全部掉线/故障 → ROBOT_UNAVAILABLE；
        3. 候选电量全部低于 MIN_BATTERY → BATTERY_TOO_LOW；
        4. 所需作业区里有一个拿不到 → ZONE_OCCUPIED；
           - 占用者全部严格低优先级：记为待抢占对象，继续选机器人；
           - 有任一占用者同级/更高优先级（含暂停中保留作业区的任务）：
             整体不抢，直接拒绝，resource 指出被哪个区、哪个任务挡住；
        5. 优先用空闲机器人（电量最高者），抢占只针对作业区占用者；
        6. 其次用"正跑在某个作业区占用者上"的机器人（抢占它同时腾出区与机器）；
        7. 再退而抢占一台正执行严格低优先级任务的机器人才腾得出机器人；
        8. 全部候选被同级/更高优先级任务占着 → ROBOT_BUSY。
        """
        store = self._store

        required = task.required_zones
        if not required:
            return DispatchPlan(reason=RejectReason.INVALID_ZONES,
                                resource="zones:<empty>")

        capable = [r for r in store.robots.values() if task.capability in r.capabilities]
        if not capable:
            return DispatchPlan(reason=RejectReason.NO_CAPABLE_ROBOT)

        online = [r for r in capable if r.status != RobotStatus.OFFLINE
                  and r.status != RobotStatus.FAULT]
        if not online:
            return DispatchPlan(reason=RejectReason.ROBOT_UNAVAILABLE)

        charged = [r for r in online if r.battery >= MIN_BATTERY]
        if not charged:
            return DispatchPlan(reason=RejectReason.BATTERY_TOO_LOW)

        # ---- 作业区：一组区必须一次性全部可拿 ----
        blockers: list[Task] = []
        blocker_ids: set[str] = set()
        # zone_id -> 占用该任务资源说明（按 required 顺序，便于确定性报告）
        blocked_at: dict[str, str] = {}
        for zid in required:
            zone = store.zones.get(zid)
            if zone is None or zone.free or zone.occupied_by == task.task_id:
                continue  # 未登记的区按"无需加锁"兼容老用法；自己占的区不算挡路
            occupant = store.tasks.get(zone.occupied_by)
            blocked_at[zid] = _zone_resource(zid, occupant)
            if occupant.task_id not in blocker_ids:
                blocker_ids.add(occupant.task_id)
                blockers.append(occupant)

        # 只要有一个占用者不是严格低优先级，就整体不抢占
        hard_block = [b for b in blockers if b.priority >= task.priority]
        if hard_block:
            # 报告第一个"硬挡路"的区（按 required 区顺序），而不是软挡路的低优任务
            hard_ids = {b.task_id for b in hard_block}
            resource = next(blocked_at[zid] for zid in required
                            if zid in blocked_at
                            and store.tasks[store.zones[zid].occupied_by].task_id
                            in hard_ids)
            return DispatchPlan(reason=RejectReason.ZONE_OCCUPIED, resource=resource)

        def by_battery(robots: list[Robot]) -> Robot:
            return sorted(robots, key=lambda r: (-r.battery, r.robot_id))[0]

        # 第 5 档：空闲机器人 —— 只抢作业区占用者，不动任何无关任务
        idle = [r for r in charged if r.available]
        if idle:
            return DispatchPlan(robot=by_battery(idle), preempt=list(blockers))

        # 第 6 档：正跑在作业区占用者上的机器人，抢占它同时腾出区和机器
        blocker_robots = [r for r in charged
                          if r.current_task in blocker_ids]
        if blocker_robots:
            return DispatchPlan(robot=by_battery(blocker_robots),
                                preempt=list(blockers))

        # 第 7 档：作业区占用者要抢，还得另抢一台正执行严格低优先级任务的机器人
        robot_victims: list[tuple[int, str, Robot]] = []
        for r in charged:
            tid = r.current_task
            if tid is None:
                continue
            victim = store.tasks.get(tid)
            if victim is not None and victim.priority < task.priority:
                robot_victims.append((victim.priority, victim.task_id, r))
        if robot_victims:
            _, victim_id, robot = sorted(robot_victims)[0]
            victims = list(blockers)
            if victim_id not in blocker_ids:
                victims.append(store.tasks[victim_id])
            return DispatchPlan(robot=robot, preempt=victims)

        # 第 8 档：机器人全被同级/更高优先级任务占着
        busy = sorted((r for r in charged if r.current_task is not None),
                      key=lambda r: r.robot_id)
        resource = None
        if busy:
            holder = store.tasks[busy[0].current_task]
            resource = _robot_resource(busy[0], holder)
        return DispatchPlan(reason=RejectReason.ROBOT_BUSY, resource=resource)

    def pick_robot(self, task: Task) -> tuple[Robot | None, RejectReason | None]:
        """兼容旧接口：仅当无需抢占即可派发时返回机器人，否则返回 (None, 原因)。"""
        plan = self.plan_dispatch(task)
        if plan.accepted and not plan.preempt:
            return plan.robot, None
        return None, plan.reason
