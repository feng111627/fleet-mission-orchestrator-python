"""场景三：异常恢复（掉线/故障重试与回退、取消与暂停后的状态自洽）。"""
from __future__ import annotations

import logging

from fleet import (
    FaultPolicy,
    Orchestrator,
    RobotStatus,
    Task,
    TaskStatus,
)

logger = logging.getLogger("fleet.tests")


def submit_one(orch: Orchestrator, task_id: str = "t1",
               policy: FaultPolicy = FaultPolicy.RETRY,
               max_retries: int = 2) -> None:
    orch.submit_graph(f"k-{task_id}", [Task(
        task_id=task_id, capability="delivery", zone="z-a",
        fault_policy=policy, max_retries=max_retries)])
    orch.dispatch_ready()


def test_robot_offline_triggers_retry(orch: Orchestrator):
    """机器人掉线：任务按 RETRY 策略重新排队，机器人恢复后可再次派发。"""
    logger.info("场景: 机器人掉线后任务重试")
    submit_one(orch)
    orch.report_robot_offline("r1")

    task = orch.store.tasks["t1"]
    assert task.status == TaskStatus.READY and task.retries == 1
    assert task.robot_id is None
    assert orch.store.zones["z-a"].free, "掉线后作业区必须释放"
    # 车队里还有 r2 可用，重试任务应立即被它接走
    assert orch.dispatch_ready() == ["t1"]
    assert orch.store.tasks["t1"].robot_id == "r2"
    # 掉线的机器人恢复后回到空闲池
    orch.report_robot_back("r1")
    assert orch.store.robots["r1"].status == RobotStatus.IDLE


def test_retries_exhausted_marks_failed(orch: Orchestrator):
    """重试次数耗尽：任务置 FAILED 且不持有任何资源。"""
    logger.info("场景: 重试耗尽后任务失败")
    submit_one(orch, max_retries=1)
    orch.report_robot_fault("r1")
    assert orch.store.tasks["t1"].status == TaskStatus.READY  # 第 1 次重试
    orch.report_robot_back("r1")
    orch.dispatch_ready()
    orch.report_robot_fault("r1")  # 再次故障，重试耗尽
    task = orch.store.tasks["t1"]
    assert task.status == TaskStatus.FAILED
    assert task.robot_id is None
    assert orch.store.zones["z-a"].free


def test_rollback_policy_fails_immediately(orch: Orchestrator):
    """ROLLBACK 策略：机器人故障时任务直接回退为 FAILED。"""
    logger.info("场景: 回退策略直接失败")
    submit_one(orch, task_id="t-rb", policy=FaultPolicy.ROLLBACK)
    orch.report_robot_fault("r1")
    task = orch.store.tasks["t-rb"]
    assert task.status == TaskStatus.FAILED and task.retries == 0
    assert orch.store.zones["z-a"].free


def test_cancel_releases_everything(orch: Orchestrator):
    """取消执行中的任务：机器人回池、作业区释放，状态自洽。"""
    logger.info("场景: 取消后资源全部释放")
    submit_one(orch)
    orch.cancel_task("t1")
    task = orch.store.tasks["t1"]
    robot = orch.store.robots[task.robot_id] if task.robot_id else None
    assert task.status == TaskStatus.CANCELLED
    assert task.robot_id is None
    assert orch.store.zones["z-a"].free
    busy = [r for r in orch.store.robots.values() if r.status == RobotStatus.BUSY]
    assert busy == [], "取消后不应有机器人仍被占用"


def test_pause_keeps_zone_resume_redispatches(orch: Orchestrator):
    """暂停：释放机器人但保留作业区；恢复后重新派发。"""
    logger.info("场景: 暂停保留作业区，恢复后重派")
    submit_one(orch)
    orch.pause_task("t1")
    task = orch.store.tasks["t1"]
    assert task.status == TaskStatus.PAUSED
    assert orch.store.zones["z-a"].occupied_by == "t1", "暂停期间作业区仍归该任务"
    assert all(r.status == RobotStatus.IDLE for r in orch.store.robots.values())

    orch.resume_task("t1")
    assert task.status == TaskStatus.READY
    assert orch.dispatch_ready() == ["t1"]
    assert orch.store.zones["z-a"].occupied_by == "t1"


def test_offline_robot_with_no_task_is_safe(orch: Orchestrator):
    """空闲机器人掉线：不影响其他任务，状态标记正确。"""
    logger.info("场景: 空闲机器人掉线无副作用")
    orch.report_robot_offline("r2")
    assert orch.store.robots["r2"].status == RobotStatus.OFFLINE
    assert orch.store.robots["r2"].current_task is None
