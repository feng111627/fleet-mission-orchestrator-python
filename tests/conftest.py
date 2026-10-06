import logging

import pytest

from fleet.models import Robot
from fleet.orchestrator import Orchestrator
from fleet.scheduler import Scheduler
from fleet.store import FleetStore

logger = logging.getLogger("fleet.tests")

# 场景分类：从测试文件名映射到业务场景，便于日志归类
SCENARIO_BY_MODULE = {
    "test_duplicate_submission": "重复提交与任务图校验",
    "test_concurrent_preemption": "并发抢占与资源互斥",
    "test_failure_recovery": "异常恢复与状态自洽",
}


@pytest.fixture()
def orchestrator():
    store = FleetStore()
    scheduler = Scheduler(store)
    return Orchestrator(store, scheduler)


@pytest.fixture()
def orch_with_robots(orchestrator):
    """预置两台不同能力的机器人。"""
    orchestrator.store.add_robot(Robot(robot_id="r1", capabilities=frozenset({"carry", "scan"}), battery=90.0))
    orchestrator.store.add_robot(Robot(robot_id="r2", capabilities=frozenset({"carry"}), battery=50.0))
    return orchestrator


def pytest_runtest_logreport(report):
    """每个用例结束时输出一行场景化日志，便于从日志核对覆盖情况。"""
    if report.when != "call":
        return
    module = report.nodeid.split("::")[0].split("/")[-1].removesuffix(".py")
    scenario = SCENARIO_BY_MODULE.get(module, module)
    case = report.nodeid.split("::")[-1]
    logger.info("[场景:%s] 用例:%s 结果:%s", scenario, case, report.outcome.upper())
