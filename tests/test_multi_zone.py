"""场景四：多作业区任务（原子占用、精准抢占、并发唯一、异常整体释放）。"""
from __future__ import annotations

import logging
import threading

from fleet import (
    Orchestrator,
    Robot,
    RobotStatus,
    Task,
    TaskStatus,
    Zone,
)

logger = logging.getLogger("fleet.tests")


def make_orch(robots=("r1", "r2"), zones=("z-a", "z-b", "z-c")) -> Orchestrator:
    o = Orchestrator()
    for rid in robots:
        o.register_robot(Robot(robot_id=rid, capabilities={"delivery"}, battery=90))
    for zid in zones:
        o.register_zone(Zone(zone_id=zid))
    return o


def test_multi_zone_dispatch_acquires_all_atomically():
    """场景: 多区任务派发成功时一次性占住全部所需作业区。"""
    logger.info("场景: 多区任务一次性占用多个作业区")
    orch = make_orch()
    orch.submit_graph("k1", [Task(
        task_id="m1", capability="delivery", zone="z-a", zones=["z-a", "z-b"])])
    assert orch.dispatch_ready() == ["m1"]
    t = orch.store.tasks["m1"]
    assert t.status == TaskStatus.DISPATCHED and t.robot_id is not None
    for zid in ("z-a", "z-b"):
        assert orch.store.zones[zid].occupied_by == "m1", f"{zid} 应归 m1 独占"
    # 没被声明的区保持空闲
    assert orch.store.zones["z-c"].occupied_by is None


def test_partial_unavailable_leaves_no_partial_hold():
    """场景: 所需区有一个拿不到 -> 留在 READY，不产生任何部分占用，并能看出卡点。"""
    logger.info("场景: 资源拿不全时无部分占用且卡点可查")
    orch = make_orch()
    # r1 先占住 z-b
    orch.submit_graph("k1", [Task(task_id="low", capability="delivery",
                                  zone="z-b", priority=1)])
    assert orch.dispatch_ready() == ["low"]

    # 新任务需要 z-a + z-b：z-b 被同优先级 low 占着 -> 不能抢，必须等待
    orch.submit_graph("k2", [Task(task_id="need-both", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=1)])
    assert orch.dispatch_ready() == []
    t = orch.store.tasks["need-both"]
    assert t.status == TaskStatus.READY, "拿不全时必须留在等待状态"
    assert orch.store.zones["z-a"].occupied_by is None, "不得先占住空闲的那部分"
    assert t.error == "zone_occupied"
    assert t.blocked_resource == "z-b" and t.blocked_by == "low", \
        f"应能看出被哪个资源/任务卡住，实际: {t.blocked_resource}/{t.blocked_by}"


def test_preemption_only_hits_real_blockers():
    """场景: 高优先级只抢真正挡路的任务；在别的区/别的机器人上跑的任务不被牵连。"""
    logger.info("场景: 抢占范围精准，无关任务继续执行")
    orch = make_orch(robots=("r1", "r2", "r3"),
                     zones=("z-a", "z-b", "z-c"))
    # 低优 blocker 占 z-a/z-b（r1）；另一个低优 unrelated 在 z-c（r2）
    orch.submit_graph("k1", [Task(task_id="blocker", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=1)])
    orch.submit_graph("k2", [Task(task_id="unrelated", capability="delivery",
                                  zone="z-c", priority=1)])
    assert orch.dispatch_ready() == ["blocker", "unrelated"]

    # r3 空闲。高优任务需要 z-a+z-b，只该动 blocker，不该动 unrelated
    orch.submit_graph("k3", [Task(task_id="high", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=9)])
    assert orch.dispatch_ready() == ["high"]

    high = orch.store.tasks["high"]
    blocker = orch.store.tasks["blocker"]
    unrelated = orch.store.tasks["unrelated"]
    assert high.status == TaskStatus.DISPATCHED
    assert blocker.status == TaskStatus.READY, "挡路者被抢并重新排队"
    assert "preempted by high" in (blocker.error or "")
    assert unrelated.status == TaskStatus.DISPATCHED, "无关任务必须继续执行"
    assert unrelated.robot_id == "r2" and orch.store.zones["z-c"].occupied_by == "unrelated"
    # 资源归属一致
    assert orch.store.zones["z-a"].occupied_by == "high"
    assert orch.store.zones["z-b"].occupied_by == "high"
    assert blocker.robot_id is None


def test_preemption_all_or_nothing_when_multiple_blockers():
    """场景: 要腾多个区需动多个任务时，其中一个不能抢（同优先级）就整体都不抢。"""
    logger.info("场景: 多受害者抢占的全有或全无")
    orch = make_orch(robots=("r1", "r2"), zones=("z-a", "z-b"))
    # lowA 低优占 z-a（r1）；sameB 同优先级占 z-b（r2）
    orch.submit_graph("k1", [Task(task_id="lowA", capability="delivery",
                                  zone="z-a", priority=1)])
    orch.submit_graph("k2", [Task(task_id="needB", capability="delivery",
                                  zone="z-b", priority=5)])
    assert orch.dispatch_ready() == ["needB", "lowA"]

    orch.submit_graph("k3", [Task(task_id="high", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=5)])
    assert orch.dispatch_ready() == []  # z-b 被同优先级卡 -> 抢不动，未派出
    high = orch.store.tasks["high"]
    lowA = orch.store.tasks["lowA"]
    needB = orch.store.tasks["needB"]
    assert high.status == TaskStatus.READY, "高优任务继续排队"
    assert lowA.status == TaskStatus.DISPATCHED, "不能先抢掉一半"
    assert needB.status == TaskStatus.DISPATCHED
    assert high.blocked_resource == "z-b" and high.blocked_by == "needB"
    # 两边的资源归属都没被搅动
    assert orch.store.zones["z-a"].occupied_by == "lowA"
    assert orch.store.zones["z-b"].occupied_by == "needB"


def test_same_or_higher_priority_blocks_preemption():
    """场景: 挡路的是同优先级/更高优先级任务时不抢，高优任务排队且卡点可查。"""
    logger.info("场景: 同优先级/更高优先级挡路不抢占")
    orch = make_orch(robots=("r1",), zones=("z-a", "z-b"))
    orch.submit_graph("k1", [Task(task_id="running", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=5)])
    assert orch.dispatch_ready() == ["running"]
    orch.submit_graph("k2", [Task(task_id="same", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=5)])
    assert orch.dispatch_ready() == []
    same = orch.store.tasks["same"]
    assert same.status == TaskStatus.READY and same.blocked_resource == "z-a"
    assert same.blocked_by == "running"
    assert orch.store.tasks["running"].status == TaskStatus.DISPATCHED


def test_concurrent_claims_same_zone_set_only_one_wins():
    """场景: 多线程同时抢同一组作业区，恰好一个成功，其余带可区分原因且无部分占用。"""
    logger.info("场景: 并发抢同一组作业区只有一个成功")
    orch = make_orch(robots=tuple(f"r{i}" for i in range(8)),
                     zones=("z-a", "z-b"))
    n = 8
    for i in range(n):
        orch.submit_graph(f"k{i}", [Task(
            task_id=f"t{i}", capability="delivery", zone="z-a",
            zones=["z-a", "z-b"], priority=1)])

    results: list[list[str]] = []
    barrier = threading.Barrier(n)

    def worker(out: list[str]) -> None:
        barrier.wait()
        out.extend(orch.dispatch_ready())

    buckets = [[] for _ in range(n)]
    threads = [threading.Thread(target=worker, args=(b,)) for b in buckets]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    winners = [tid for out in buckets for tid in out]
    assert len(winners) == 1, f"必须恰好一个任务抢到整组区，实际: {winners}"
    winner = winners[0]
    assert orch.store.tasks[winner].status == TaskStatus.DISPATCHED
    # 两个区都必须且只能归赢家
    for zid in ("z-a", "z-b"):
        assert orch.store.zones[zid].occupied_by == winner
    losers = [t for tid, t in orch.store.tasks.items() if tid != winner]
    assert all(t.status == TaskStatus.READY for t in losers)
    reasons = {t.error for t in losers}
    assert reasons <= {"zone_occupied", "robot_busy"}, reasons
    assert reasons, "失败方必须能看出是资源被占还是机器人忙"
    # 不能有两个任务都声称占着同一个区
    for zid in ("z-a", "z-b"):
        claimants = [t.task_id for t in orch.store.tasks.values()
                     if any(orch.store.zones.get(z) and
                            orch.store.zones[z].occupied_by == t.task_id
                            for z in t.required_zones)
                     and orch.store.zones[zid].occupied_by == t.task_id]
        assert claimants == [winner]


def test_preempted_task_redispatches_after_resources_free():
    """场景: 被抢任务释放全部资源并重新排队，资源空出后还能再被派出去。"""
    logger.info("场景: 被抢占任务释放资源后可再次派发")
    orch = make_orch(robots=("r1",), zones=("z-a", "z-b"))
    orch.submit_graph("k1", [Task(task_id="low", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=1)])
    assert orch.dispatch_ready() == ["low"]
    orch.submit_graph("k2", [Task(task_id="high", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"], priority=9)])
    assert orch.dispatch_ready() == ["high"]
    low = orch.store.tasks["low"]
    assert low.status == TaskStatus.READY and low.robot_id is None
    for zid in ("z-a", "z-b"):
        assert orch.store.zones[zid].occupied_by == "high"

    orch.complete_task("high", success=True)
    assert orch.dispatch_ready() == ["low"], "资源空出后被抢任务应重新派出"
    assert orch.store.tasks["low"].status == TaskStatus.DISPATCHED
    for zid in ("z-a", "z-b"):
        assert orch.store.zones[zid].occupied_by == "low"


def test_cancel_and_fault_release_all_zones_atomically():
    """场景: 取消、机器人掉线/故障、执行失败时，机器人与全部作业区一次性释放。"""
    logger.info("场景: 取消/异常时多资源整体释放")

    def fresh() -> tuple[Orchestrator, str]:
        orch = make_orch(robots=("r1",), zones=("z-a", "z-b"))
        orch.submit_graph("k1", [Task(task_id="m", capability="delivery",
                                      zone="z-a", zones=["z-a", "z-b"],
                                      max_retries=0)])
        assert orch.dispatch_ready() == ["m"]
        return orch, "m"

    # 执行失败（重试耗尽）：任务自身不持有任何资源，所有区回到空闲
    orch, mid = fresh()
    orch.complete_task(mid, success=False)  # max_retries=0 -> FAILED
    t = orch.store.tasks[mid]
    assert t.status == TaskStatus.FAILED and t.robot_id is None
    assert all(orch.store.zones[z].occupied_by is None for z in ("z-a", "z-b")), \
        "失败后所有区必须释放"
    assert orch.store.robots["r1"].status == RobotStatus.IDLE

    # 机器人掉线：在执多区任务的机器人与全部区整体释放
    orch, mid = fresh()
    orch.report_robot_offline("r1")
    t = orch.store.tasks[mid]
    assert t.robot_id is None
    assert all(orch.store.zones[z].occupied_by is None for z in ("z-a", "z-b")), \
        "掉线后所有区必须释放"

    # 机器人故障 + 回退策略：同样整体释放
    orch = make_orch(robots=("r1",), zones=("z-a", "z-b"))
    from fleet import FaultPolicy
    orch.submit_graph("k9", [Task(task_id="rb", capability="delivery",
                                  zone="z-a", zones=["z-a", "z-b"],
                                  fault_policy=FaultPolicy.ROLLBACK)])
    orch.dispatch_ready()
    orch.report_robot_fault("r1")
    assert orch.store.tasks["rb"].status == TaskStatus.FAILED
    assert all(orch.store.zones[z].occupied_by is None for z in ("z-a", "z-b")), \
        "故障回退后所有区必须释放"

    # 取消路径：全部区一次性释放，无悬挂占用
    orch, mid = fresh()
    orch.cancel_task(mid)
    t = orch.store.tasks[mid]
    assert t.status == TaskStatus.CANCELLED and t.robot_id is None
    assert all(orch.store.zones[z].occupied_by is None for z in ("z-a", "z-b")), \
        "取消后所有区必须释放"
    assert orch.store.robots["r1"].status == RobotStatus.IDLE
