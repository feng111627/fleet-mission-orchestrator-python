"""公共夹具与测试日志：每个用例的开始/结果都会落到日志，便于核对场景覆盖。"""
from __future__ import annotations

import logging

import pytest

from fleet import Orchestrator, Robot, Zone

logger = logging.getLogger("fleet.tests")


@pytest.fixture
def orch() -> Orchestrator:
    o = Orchestrator()
    o.register_robot(Robot(robot_id="r1", capabilities={"delivery"}, battery=90))
    o.register_robot(Robot(robot_id="r2", capabilities={"delivery", "cleaning"}, battery=80))
    o.register_zone(Zone(zone_id="z-a"))
    o.register_zone(Zone(zone_id="z-b"))
    return o


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.when == "call":
        logger.info("[CASE] %s :: %s -> %s",
                    item.module.__name__, item.name, report.outcome.upper())
