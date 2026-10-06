"""场景一：重复提交与任务图校验（幂等、依赖门控、成环报错）。"""
from __future__ import annotations

import logging

import pytest

from fleet import DependencyCycleError, Orchestrator, Task, TaskStatus
from fleet.errors import UnknownTaskError

logger = logging.getLogger("fleet.tests")


def t(task_id: str, deps: list[str] | None = None, zone: str = "z-a") -> Task:
    return Task(task_id=task_id, capability="delivery", zone=zone,
                dependencies=deps or [])


def test_duplicate_submission_is_idempotent(orch: Orchestrator):
    """同一幂等键重复提交：任务不重复入图、不重复调度。"""
    logger.info("场景: 客户端超时重试同一提交")
    first = orch.submit_graph("key-1", [t("a"), t("b", deps=["a"])])
    second = orch.submit_graph("key-1", [t("a"), t("b", deps=["a"])])
    assert first == second == ["a", "b"]
    assert len(orch.store.tasks) == 2
    assert len(orch.store.submissions) == 1


def test_dependency_gates_dispatch(orch: Orchestrator):
    """依赖未满足的任务保持 PENDING，不能被提前派发。"""
    logger.info("场景: 依赖未满足不派发")
    orch.submit_graph("key-2", [t("a"), t("b", deps=["a"])])
    assert orch.store.tasks["b"].status == TaskStatus.PENDING
    dispatched = orch.dispatch_ready()
    assert dispatched == ["a"]
    assert orch.store.tasks["b"].status == TaskStatus.PENDING
    # a 完成后 b 才进入 READY 并可被派发
    orch.complete_task("a", success=True)
    assert orch.store.tasks["b"].status == TaskStatus.READY
    assert orch.dispatch_ready() == ["b"]


def test_dependency_cycle_rejected(orch: Orchestrator):
    """依赖成环：明确报错，且任务不入图。"""
    logger.info("场景: 依赖成环明确报错")
    with pytest.raises(DependencyCycleError) as exc:
        orch.submit_graph("key-3", [t("x", deps=["y"]), t("y", deps=["x"])])
    assert "x" in str(exc.value) and "y" in str(exc.value)
    assert len(orch.store.tasks) == 0  # 失败提交不留半成品


def test_unknown_dependency_rejected(orch: Orchestrator):
    """依赖不存在的任务：提交即报错而不是永远卡住。"""
    logger.info("场景: 悬空依赖报错")
    with pytest.raises(UnknownTaskError):
        orch.submit_graph("key-4", [t("a", deps=["ghost"])])
    assert len(orch.store.tasks) == 0
