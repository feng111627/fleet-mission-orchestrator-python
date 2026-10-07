"""场景四：多作业区任务（整组原子占用、精确抢占边界、并发唯一性、异常整体释放）。

每个用例开头都用日志标注所属场景，pytest 的 log_cli 会把用例名与结果逐行打印，
配合 conftest 里的 [CASE] 行可核对场景覆盖。
"""
from __future__ import annotations

import logging
import threading

import pytest

from fleet import (
    FaultPolicy,
    Orchestrator,
    RejectReason,
    Robot,
    RobotStatus,
    Task,
    TaskStatus,
    Zone,
)

logger = logging.getLogger("fleet.tests")


def make_orch(robots=("r1", "r2", "r3"), zones=("z1", "z2", "z3")) -> Orchestrator:
    o = Orchestrator()
    for i, rid in enumerate(robots):
        o.register_robot(Robot(robot_id=rid, capabilities={"delivery"},
                               battery=90 - i * 10))
    for z in zones:
        o.register_zone(Zone(zone_id=z))
    return o


def mz(task_id: str, zones: list[str], priority: int = 0,
       policy: FaultPolicy = FaultPolicy.RETRY) -> Task:
    return Task(task_id=task_id, capability="delivery", zone=zones[0],
                zones=list(zones), priority=priority, fault_policy=policy)


def assert_zones_free(o: Orchestrator, zones) -> None:
    for z in zones:
        assert o.store.zones[z].occupied_by is None, f"{z} 仍被占用"


# ---- 1. 跨多个区派发成功：整组一次性占上 ----
def test_dispatch_acquires_all_zones_atomically():
    """多区任务派发成功时，整组区与机器人一次性全部归它独占。"""
    logger.info("场景: 多区任务派发成功，整组区一次占全")
    o = make_orch()
    o.submit_graph("k1", [mz("m", ["z1", "z2", "z3"], priority=5)])
    assert o.dispatch_ready() == ["m"]
    t = o.store.tasks["m"]
    assert t.status == TaskStatus.DISPATCHED
    assert t.robot_id is not None
    for z in ("z1", "z2", "z3"):
        assert o.store.zones[z].occupied_by == "m"
    robot = o.store.robots[t.robot_id]
    assert robot.status == RobotStatus.BUSY and robot.current_task == "m"
    # 单区老用法仍然工作（required_zones 退化为 [zone]）
    o.submit_graph("k-old", [Task(task_id="old", capability="delivery", zone="z-other")])
    # z-other 未登记：无需加锁即可派发，行为与单区时代一致
    assert o.dispatch_ready() == ["old"]
    assert o.store.tasks["old"].required_zones == ["z-other"]


# ---- 2. 资源拿不全：不产生任何部分占用，留在 READY ----
def test_partial_unavailability_leaves_no_partial_hold():
    """整组区里只要有一个拿不到，任务留在 READY，其余区一个都不许先占。"""
    logger.info("场景: 整组区拿不全时不留部分占用")
    o = make_orch()
    # holder 占住 z2
    o.submit_graph("k1", [Task(task_id="holder", capability="delivery",
                               zone="z2", priority=8)])
    assert o.dispatch_ready() == ["holder"]

    # waiter 需要 z1+z2+z3：z2 被同级(8)任务挡住 -> 拒绝且整体不抢
    o.submit_graph("k2", [mz("waiter", ["z1", "z2", "z3"], priority=8)])
    assert o.dispatch_ready() == []
    w = o.store.tasks["waiter"]
    assert w.status == TaskStatus.READY
    assert w.error == RejectReason.ZONE_OCCUPIED.value
    # 能从结果看出被哪个资源、什么情况卡住
    assert w.blocked_resource is not None
    assert "z2" in w.blocked_resource and "holder" in w.blocked_resource
    # 关键不变量：z1/z3 没被 waiter 先占，空闲机器人也没被它扣住
    assert o.store.zones["z1"].occupied_by is None
    assert o.store.zones["z3"].occupied_by is None
    busy = {r.current_task for r in o.store.robots.values() if r.current_task}
    assert busy == {"holder"}


# ---- 3. 高优抢占只影响真正挡路的任务 ----
def test_preemption_only_touches_resource_holders():
    """高优任务只抢占着它所需区/机器的低优任务；在别的区跑的任务不受牵连。"""
    logger.info("场景: 精确抢占，无关任务不受牵连")
    o = make_orch(zones=("z1", "z2", "z3", "z4"))
    # blocker 占 z1；by-stander 在无关的 z4 上执行（同为低优先级）
    o.submit_graph("k1", [Task(task_id="blocker", capability="delivery",
                               zone="z1", priority=1)])
    o.submit_graph("k2", [Task(task_id="bystander", capability="delivery",
                               zone="z4", priority=1)])
    assert sorted(o.dispatch_ready()) == ["blocker", "bystander"]
    bystander_robot = o.store.tasks["bystander"].robot_id

    # high 需要 z1+z2+z3，只 blocker 挡路
    o.submit_graph("k3", [mz("high", ["z1", "z2", "z3"], priority=9)])
    assert o.dispatch_ready() == ["high"]

    high = o.store.tasks["high"]
    assert high.status == TaskStatus.DISPATCHED
    for z in ("z1", "z2", "z3"):
        assert o.store.zones[z].occupied_by == "high"
    blocker = o.store.tasks["blocker"]
    assert blocker.status == TaskStatus.READY and blocker.robot_id is None
    assert "preempted by high" in (blocker.error or "")
    # 旁观者：仍在自己的机器人、自己的区上继续执行，error 不变
    by = o.store.tasks["bystander"]
    assert by.status == TaskStatus.DISPATCHED
    assert by.robot_id == bystander_robot
    assert o.store.zones["z4"].occupied_by == "bystander"
    assert by.error is None


# ---- 4. 多个挡路者中一个不该抢 -> 整体都不抢 ----
def test_preemption_is_all_or_nothing_across_victims():
    """要动多个任务才能腾资源时，只要一个挡路者同级/更高优先级，就一个都不抢。"""
    logger.info("场景: 多挡路者中其一不可抢则整体不抢，不先抢一半")
    o = make_orch()
    o.submit_graph("k1", [Task(task_id="low", capability="delivery",
                               zone="z1", priority=1)])
    o.submit_graph("k2", [Task(task_id="peer", capability="delivery",
                               zone="z2", priority=9)])
    o.dispatch_ready()
    low_robot = o.store.tasks["low"].robot_id

    # high(9) 需要 z1+z2+z3：z1 上的 low 可抢，z2 上的 peer(9) 同级不可抢
    o.submit_graph("k3", [mz("high", ["z1", "z2", "z3"], priority=9)])
    assert o.dispatch_ready() == []

    high = o.store.tasks["high"]
    assert high.status == TaskStatus.READY
    assert high.error == RejectReason.ZONE_OCCUPIED.value
    assert high.blocked_resource and "z2" in high.blocked_resource \
        and "peer" in high.blocked_resource
    # 关键：low 没有被"先抢掉一半"，原样继续
    low = o.store.tasks["low"]
    assert low.status == TaskStatus.DISPATCHED and low.robot_id == low_robot
    assert o.store.zones["z1"].occupied_by == "low"
    assert o.store.zones["z3"].occupied_by is None
    assert o.store.tasks["peer"].status == TaskStatus.DISPATCHED


# ---- 4b. 全部挡路者都严格低优先级（含暂停中）时才整体抢占 ----
def test_preempts_all_lower_holders_including_paused():
    """一个执行者 + 一个暂停中保留作业区的任务同时挡路，两者都严格低优时整体抢占。"""
    logger.info("场景: 执行中与暂停中的低优挡路者被一并抢占")
    o = make_orch()
    o.submit_graph("k1", [Task(task_id="low1", capability="delivery",
                               zone="z1", priority=1)])
    o.submit_graph("k2", [mz("low2", ["z2", "z3"], priority=2)])
    o.dispatch_ready()
    o.pause_task("low2")  # 释放机器人，但 z2/z3 仍归 low2

    o.submit_graph("k3", [mz("high", ["z1", "z2", "z3"], priority=9)])
    assert o.dispatch_ready() == ["high"]
    assert o.store.tasks["low1"].status == TaskStatus.READY
    assert o.store.tasks["low2"].status == TaskStatus.READY
    for z in ("z1", "z2", "z3"):
        assert o.store.zones[z].occupied_by == "high"
    # 被抢占者重新排队，资源空出后还能再被派出去
    o.cancel_task("high")
    assert o.dispatch_ready()  # low1/low2 至少其一被重新派发
    redispatched = [t.status for t in
                    (o.store.tasks["low1"], o.store.tasks["low2"])]
    assert TaskStatus.DISPATCHED in redispatched


# ---- 5. 多线程同时抢同一组区：恰好一个成功 ----
def test_concurrent_same_zone_set_only_one_winner():
    """N 个线程同时派发抢同一组多区资源：唯一赢家，失败者原因/资源可区分。"""
    logger.info("场景: 多线程抢同一组多区资源仅 1 个成功")
    n = 16
    o = make_orch(robots=tuple(f"r{i}" for i in range(n)),
                  zones=("z1", "z2", "z3"))
    for i in range(n):
        o.store.tasks[f"t{i}"] = Task(
            task_id=f"t{i}", capability="delivery", zone="z1",
            zones=["z1", "z2", "z3"], status=TaskStatus.READY)

    results: list[list[str]] = [[] for _ in range(n)]
    barrier = threading.Barrier(n)

    def worker(idx: int) -> None:
        barrier.wait()
        results[idx].extend(o.dispatch_ready())

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    winners = [tid for out in results for tid in out]
    assert len(winners) == 1, f"必须恰好一个赢家，实际: {winners}"
    winner = winners[0]
    # 三个区与一台机器人全部只归赢家，不存在两个任务同占一区
    for z in ("z1", "z2", "z3"):
        assert o.store.zones[z].occupied_by == winner
    current = {r.current_task for r in o.store.robots.values() if r.current_task}
    assert current == {winner}
    losers = [t for tid, t in o.store.tasks.items() if tid != winner]
    assert all(t.status == TaskStatus.READY for t in losers)
    reasons = {t.error for t in losers}
    assert reasons <= {RejectReason.ZONE_OCCUPIED.value,
                       RejectReason.ROBOT_BUSY.value}
    assert reasons, "失败者必须带可区分原因"
    # 失败者能区分是"资源被占用"而非别的原因
    assert all(t.blocked_resource and
               t.blocked_resource.startswith(("zone:", "robot:"))
               for t in losers)


# ---- 6. 取消 / 掉线 / 故障 / 失败：多个资源整体释放 ----
@pytest.mark.parametrize("event", ["cancel", "offline", "fault", "fail"])
def test_failure_events_release_all_resources(event: str):
    """取消、被抢占后取消、机器人掉线/故障、执行失败上报：整组区与机器人一次释放。"""
    logger.info("场景: 异常事件 %s 下多资源整体释放", event)
    policy = FaultPolicy.ROLLBACK if event in ("fault", "fail") else FaultPolicy.RETRY
    o = make_orch(robots=("r1",))
    o.submit_graph("k1", [mz("m", ["z1", "z2", "z3"], policy=policy)])
    assert o.dispatch_ready() == ["m"]

    if event == "cancel":
        o.cancel_task("m")
        expect = TaskStatus.CANCELLED
    elif event == "offline":
        o.report_robot_offline("r1")
        expect = TaskStatus.READY  # RETRY：重新排队
    elif event == "fault":
        o.report_robot_fault("r1")
        expect = TaskStatus.FAILED  # ROLLBACK：直接失败
    else:
        o.complete_task("m", success=False)
        expect = TaskStatus.FAILED

    t = o.store.tasks["m"]
    assert t.status == expect
    assert t.robot_id is None
    assert_zones_free(o, ("z1", "z2", "z3"))
    assert all(r.status != RobotStatus.BUSY for r in o.store.robots.values())
    assert all(r.current_task is None for r in o.store.robots.values())
    # 掉线重试场景：资源空出后同一任务能再次被派发
    if event == "offline":
        o.register_robot(Robot(robot_id="r2", capabilities={"delivery"}, battery=80))
        assert o.dispatch_ready() == ["m"]
        assert o.store.tasks["m"].robot_id == "r2"


# ---- 7. 被抢占任务放开全部资源并重新排队，之后可再次派发 ----
def test_preempted_task_releases_all_and_redispatches():
    """被抢任务的机器人与整组区全部放开；等高优释放后它能再被派出。"""
    logger.info("场景: 被抢占任务整体释放并重新排队，资源空出后再次派发")
    o = make_orch(robots=("r1", "r2"))
    o.submit_graph("k1", [mz("low", ["z1", "z2", "z3"], priority=1)])
    assert o.dispatch_ready() == ["low"]
    o.submit_graph("k2", [mz("high", ["z1", "z2", "z3"], priority=9)])
    assert o.dispatch_ready() == ["high"]
    low = o.store.tasks["low"]
    assert low.status == TaskStatus.READY and low.robot_id is None
    # 被抢后整组区全部归 high，low 名下无任何残留占用
    for z in ("z1", "z2", "z3"):
        assert o.store.zones[z].occupied_by == "high"
    assert o.store.robots["r1"].status == RobotStatus.IDLE
    assert o.store.robots["r1"].current_task is None
    # high 完成释放整组区后，low 再次被派出（按电量最高取 r1，规则不变）
    o.complete_task("high", success=True)
    assert o.dispatch_ready() == ["low"]
    low2 = o.store.tasks["low"]
    assert low2.status == TaskStatus.DISPATCHED and low2.robot_id == "r1"
    for z in ("z1", "z2", "z3"):
        assert o.store.zones[z].occupied_by == "low"
