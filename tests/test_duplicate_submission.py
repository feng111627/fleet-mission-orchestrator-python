"""场景：重复提交与任务图校验。"""
import logging
import threading

import pytest

from fleet.models import TaskSpec, TaskState
from fleet.orchestrator import DependencyError

logger = logging.getLogger("fleet.tests")


def test_duplicate_submit_returns_same_mission(orch_with_robots):
    orch = orch_with_robots
    specs = [TaskSpec(task_id="t1", capability="carry")]
    m1 = orch.submit_mission("key-1", specs)
    m2 = orch.submit_mission("key-1", specs)  # 客户端超时重试
    assert m1 == m2
    assert len(orch.store.missions) == 1
    task = orch.store.missions[m1].tasks["t1"]
    assert task.attempts == 1  # 没有被重复调度
    logger.info("重复提交返回同一 mission=%s，attempts=%d", m1, task.attempts)


def test_concurrent_duplicate_submit_schedules_once(orch_with_robots):
    orch = orch_with_robots
    specs = [TaskSpec(task_id="t1", capability="carry")]
    results, errors = [], []

    def submit():
        try:
            results.append(orch.submit_mission("key-race", specs))
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(set(results)) == 1
    assert len(orch.store.missions) == 1
    task = orch.store.missions[results[0]].tasks["t1"]
    assert task.attempts == 1
    logger.info("20 线程并发重复提交，仅 1 个 mission，attempts=%d", task.attempts)


def test_dependency_gates_dispatch(orch_with_robots):
    orch = orch_with_robots
    specs = [
        TaskSpec(task_id="t1", capability="carry"),
        TaskSpec(task_id="t2", capability="scan", depends_on=("t1",)),
    ]
    mid = orch.submit_mission("key-dep", specs)
    mission = orch.store.missions[mid]
    assert mission.tasks["t1"].state == TaskState.DISPATCHED
    assert mission.tasks["t2"].state == TaskState.PENDING  # 依赖未满足不得派发

    orch.report_robot_event(mission.tasks["t1"].robot_id, "started")
    orch.report_robot_event(mission.tasks["t1"].robot_id, "succeeded")
    assert mission.tasks["t2"].state == TaskState.DISPATCHED
    logger.info("依赖满足前 t2=PENDING，t1 成功后 t2=%s", mission.tasks["t2"].state)


def test_dependency_cycle_rejected(orchestrator):
    specs = [
        TaskSpec(task_id="a", capability="carry", depends_on=("b",)),
        TaskSpec(task_id="b", capability="carry", depends_on=("a",)),
    ]
    with pytest.raises(DependencyError, match="cycle"):
        orchestrator.submit_mission("key-cycle", specs)
    logger.info("依赖成环被明确拒绝")


def test_unknown_dependency_rejected(orchestrator):
    specs = [TaskSpec(task_id="a", capability="carry", depends_on=("ghost",))]
    with pytest.raises(DependencyError, match="unknown task"):
        orchestrator.submit_mission("key-unknown", specs)
    logger.info("未知依赖被明确拒绝")
