"""场景二：并发抢占（多线程抢同一机器人/作业区、高优先级抢占、拒绝原因可区分）。"""
from __future__ import annotations

import logging
import threading

from fleet import (
    Orchestrator,
    RejectReason,
    Robot,
    RobotStatus,
    Task,
    TaskStatus,
    Zone,
)

logger = logging.getLogger("fleet.tests")


def make_orch() -> Orchestrator:
    o = Orchestrator()
    o.register_robot(Robot(robot_id="r1", capabilities={"delivery"}, battery=90))
    o.register_zone(Zone(zone_id="z-a"))
    return o


def test_concurrent_dispatch_only_one_wins():
    """10 个线程同时派发抢同一台机器人/同一作业区：恰好一个成功，其余拿到可区分原因。"""
    logger.info("场景: 多线程并发抢同一机器人与作业区")
    orch = make_orch()
    for i in range(10):
        orch.store.tasks[f"t{i}"] = Task(task_id=f"t{i}", capability="delivery",
                                         zone="z-a", status=TaskStatus.READY)

    results: list[list[str]] = []
    barrier = threading.Barrier(10)

    def worker(out: list[str]):
        barrier.wait()
        out.extend(orch.dispatch_ready())

    threads = [threading.Thread(target=worker, args=(r,)) for r in
               [results.append([]) or results[-1] for _ in range(10)]]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    winners = [tid for out in results for tid in out]
    assert len(winners) == 1, f"必须恰好一个任务抢到，实际: {winners}"
    winner = winners[0]
    assert orch.store.tasks[winner].status == TaskStatus.DISPATCHED
    # 失败者都有可区分原因：作业区被占 或 机器人忙
    losers = [t for tid, t in orch.store.tasks.items() if tid != winner]
    reasons = {t.error for t in losers}
    assert all(t.status == TaskStatus.READY for t in losers)
    assert reasons <= {RejectReason.ZONE_OCCUPIED.value, RejectReason.ROBOT_BUSY.value}
    assert reasons, "失败任务必须带有拒绝原因"
    # 状态自洽：机器人只服务赢家，作业区只归赢家
    robot = orch.store.robots["r1"]
    assert robot.status == RobotStatus.BUSY and robot.current_task == winner
    assert orch.store.zones["z-a"].occupied_by == winner


def test_high_priority_preempts_low_priority():
    """高优先级任务抢占正在执行的低优先级任务，被抢占者重新排队且不再算在执行。"""
    logger.info("场景: 高优先级抢占低优先级")
    orch = make_orch()
    orch.submit_graph("k1", [Task(task_id="low", capability="delivery",
                                  zone="z-a", priority=1)])
    assert orch.dispatch_ready() == ["low"]

    orch.submit_graph("k2", [Task(task_id="high", capability="delivery",
                                  zone="z-a", priority=9)])
    assert orch.dispatch_ready() == ["high"]

    low, high = orch.store.tasks["low"], orch.store.tasks["high"]
    assert high.status == TaskStatus.DISPATCHED and high.robot_id == "r1"
    assert low.status == TaskStatus.READY  # 被抢占后重新排队
    assert low.robot_id is None and "preempted by high" in (low.error or "")
    assert orch.store.zones["z-a"].occupied_by == "high"  # 作业区只归高优先级


def test_reject_reasons_are_distinguishable():
    """不同失败原因可区分：无能力 / 电量低 / 机器人忙。"""
    logger.info("场景: 拒绝原因可区分")

    def one_robot_orch(battery: float) -> Orchestrator:
        o = Orchestrator()
        o.register_robot(Robot(robot_id="r1", capabilities={"delivery"}, battery=battery))
        o.register_zone(Zone(zone_id="z-a"))
        return o

    # 电量不足
    orch = one_robot_orch(battery=10)
    orch.submit_graph("k1", [Task(task_id="low-batt", capability="delivery", zone="z-a")])
    assert orch.dispatch_ready() == []
    assert orch.store.tasks["low-batt"].error == RejectReason.BATTERY_TOO_LOW.value

    # 无具备能力的机器人
    orch = one_robot_orch(battery=90)
    orch.submit_graph("k2", [Task(task_id="no-cap", capability="flying", zone="z-a")])
    assert orch.dispatch_ready() == []
    assert orch.store.tasks["no-cap"].error == RejectReason.NO_CAPABLE_ROBOT.value

    # 机器人忙（同优先级不触发抢占）
    orch = one_robot_orch(battery=90)
    orch.submit_graph("k3", [Task(task_id="first", capability="delivery", zone="z-a")])
    orch.submit_graph("k4", [Task(task_id="second", capability="delivery", zone="z-a")])
    assert orch.dispatch_ready() == ["first"]
    assert orch.store.tasks["second"].error in (
        RejectReason.ROBOT_BUSY.value, RejectReason.ZONE_OCCUPIED.value)
