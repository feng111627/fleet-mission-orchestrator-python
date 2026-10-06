"""HTTP 层端到端：注册机器人 -> 提交任务图 -> 事件上报 -> 状态查询。"""
import logging

from fastapi.testclient import TestClient

from fleet.api import create_app

logger = logging.getLogger("fleet.tests")


def test_http_end_to_end():
    client = TestClient(create_app())
    assert client.post("/robots", json={"robot_id": "r1", "capabilities": ["carry"]}).status_code == 200

    mission = {"idempotency_key": "http-1", "tasks": [{"task_id": "t1", "capability": "carry"}]}
    r1 = client.post("/missions", json=mission)
    r2 = client.post("/missions", json=mission)  # 重复提交
    assert r1.status_code == 200 and r1.json() == r2.json()

    assert client.post("/robots/r1/events", json={"event": "started"}).status_code == 200
    assert client.post("/robots/r1/events", json={"event": "succeeded"}).status_code == 200

    state = client.get("/state").json()
    mid = r1.json()["mission_id"]
    assert state["missions"][mid]["t1"]["state"] == "SUCCEEDED"
    assert state["robots"]["r1"]["state"] == "IDLE"
    logger.info("HTTP 端到端：重复提交幂等，任务走到 SUCCEEDED，机器人回到 IDLE")


def test_http_cycle_returns_400():
    client = TestClient(create_app())
    body = {"idempotency_key": "http-cycle", "tasks": [
        {"task_id": "a", "capability": "carry", "depends_on": ["b"]},
        {"task_id": "b", "capability": "carry", "depends_on": ["a"]},
    ]}
    resp = client.post("/missions", json=body)
    assert resp.status_code == 400 and "cycle" in resp.json()["detail"]
    logger.info("HTTP 层对依赖成环返回 400：%s", resp.json()["detail"])
