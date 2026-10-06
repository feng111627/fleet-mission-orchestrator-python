"""场景：异常恢复与状态自洽。"""
import logging

from fleet.models import Robot, RobotState, TaskSpec, TaskState
from fleet.models import Task
from fleet.orchestrator import Orchestrator
from fleet.scheduler import Scheduler
from fleet.store import FleetStore

logger = logging.getLogger("fleet.tests")


def make_orch(robots):
    store = FleetStore()
    for r in robots:
        store.add_robot(r)
    return Orchestrator(store, Scheduler(store))


def assert_consistent(orch):
    """全局不变量：终态任务不占资源/机器人；机器人当前任务必须是活动任务。"""
    snap = orch.snapshot()
    active = {"DISPATCHED", "RUNNING", "PAUSED"}
    task_state = {}
    for m in snap["missions"].values():
        task_state.update(m)
    for tid, info in task_state.items():
        if info["state"] not in active:
            assert info["robot_id"] is None, f"{tid} 已终态却仍绑定机器人"
    for rid, owner in snap["leases"].items():
        assert task_state[owner]["state"] in active, f"资源 {rid} 被非活动任务 {owner} 持有"
    for rid, r in snap["robots"].items():
        if r["current_task_id"] is not None:
            assert task_state[r["current_task_id"]]["state"] in active
            assert r["state"] == "BUSY"


def test_fault_triggers_retry_on_another_robot():
    orch = make_orch([
        Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0),
        Robot(robot_id="r2", capabilities=frozenset({"carry"}), battery=80.0),
    ])
    mid = orch.submit_mission("k", [TaskSpec(task_id="t1", capability="carry", max_retries=2)])
    task = orch.store.missions[mid].tasks["t1"]
    assert task.robot_id == "r1"

    orch.report_robot_event("r1", "fault")
    assert orch.store.robots["r1"].state == RobotState.FAULT
    assert task.state == TaskState.DISPATCHED and task.robot_id == "r2"  # 重试到 r2
    assert task.attempts == 2
    assert_consistent(orch)
    logger.info("r1 故障后 t1 重试到 r2，attempts=%d", task.attempts)


def test_retries_exhausted_then_fail_and_cascade_cancel():
    orch = make_orch([Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0)])
    mid = orch.submit_mission("k", [
        TaskSpec(task_id="t1", capability="carry", max_retries=1),
        TaskSpec(task_id="t2", capability="carry", depends_on=("t1",)),
    ])
    mission = orch.store.missions[mid]
    orch.report_robot_event("r1", "failed")   # 第 1 次失败 -> 重试
    assert mission.tasks["t1"].state == TaskState.DISPATCHED
    orch.report_robot_event("r1", "failed")   # 第 2 次失败 -> 次数用尽
    assert mission.tasks["t1"].state == TaskState.FAILED
    assert mission.tasks["t2"].state == TaskState.CANCELLED  # 级联取消
    assert_consistent(orch)
    logger.info("重试次数用尽：t1=FAILED，下游 t2 级联 CANCELLED")


def test_offline_robot_task_recovers_when_peer_available():
    orch = make_orch([
        Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0),
        Robot(robot_id="r2", capabilities=frozenset({"carry"}), battery=80.0),
    ])
    mid = orch.submit_mission("k", [TaskSpec(task_id="t1", capability="carry")])
    task = orch.store.missions[mid].tasks["t1"]
    orch.report_robot_event("r1", "offline")
    assert orch.store.robots["r1"].state == RobotState.OFFLINE
    assert task.state == TaskState.DISPATCHED and task.robot_id == "r2"
    orch.report_robot_event("r1", "online")
    assert orch.store.robots["r1"].state == RobotState.IDLE
    assert_consistent(orch)
    logger.info("r1 掉线后任务迁移到 r2，r1 恢复上线回到 IDLE")


def test_cancel_releases_everything():
    orch = make_orch([Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0)])
    mid = orch.submit_mission("k", [
        TaskSpec(task_id="t1", capability="carry", required_resources=("zone-A",)),
    ])
    orch.report_robot_event("r1", "started")
    orch.cancel_task("t1")
    task = orch.store.missions[mid].tasks["t1"]
    assert task.state == TaskState.CANCELLED
    assert task.robot_id is None
    assert orch.store.leases == {}                       # 取消后不得再占资源
    assert orch.store.robots["r1"].state == RobotState.IDLE
    assert orch.store.robots["r1"].current_task_id is None
    assert_consistent(orch)
    logger.info("取消后资源租约清空、机器人回到 IDLE")


def test_pause_resume_keeps_assignment():
    orch = make_orch([Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0)])
    mid = orch.submit_mission("k", [TaskSpec(task_id="t1", capability="carry")])
    orch.report_robot_event("r1", "started")
    orch.pause_task("t1")
    task = orch.store.missions[mid].tasks["t1"]
    assert task.state == TaskState.PAUSED
    assert task.robot_id == "r1"  # 暂停期间保留占用，便于恢复
    orch.resume_task("t1")
    assert task.state == TaskState.RUNNING
    assert_consistent(orch)
    logger.info("暂停保留占用，恢复后继续 RUNNING")


def test_preempted_task_not_counted_as_running():
    orch = make_orch([Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0)])
    orch.submit_mission("low", [TaskSpec(task_id="t-low", capability="carry", priority=1)])
    orch.report_robot_event("r1", "started")
    orch.submit_mission("high", [TaskSpec(task_id="t-high", capability="carry", priority=9)])
    running = [
        t.spec.task_id for m in orch.store.missions.values() for t in m.tasks.values()
        if t.state in (TaskState.DISPATCHED, TaskState.RUNNING)
    ]
    assert running == ["t-high"]  # 被抢占的 t-low 不再算作在执行
    assert_consistent(orch)
    logger.info("抢占后仅 t-high 处于执行态：%s", running)
