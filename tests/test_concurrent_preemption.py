"""场景：并发抢占与资源互斥。"""
import logging
import threading

from fleet.models import DispatchFailure, Robot, TaskSpec, TaskState
from fleet.orchestrator import Orchestrator
from fleet.scheduler import Scheduler
from fleet.store import FleetStore

logger = logging.getLogger("fleet.tests")


def make_orch(robots):
    store = FleetStore()
    for r in robots:
        store.add_robot(r)
    return Orchestrator(store, Scheduler(store))


def test_concurrent_dispatch_single_robot_exactly_one_wins():
    orch = make_orch([Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0)])
    barrier = threading.Barrier(10)

    def submit(i):
        barrier.wait()
        orch.submit_mission(f"key-{i}", [TaskSpec(task_id=f"t{i}", capability="carry")])

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    states = [t.state for m in orch.store.missions.values() for t in m.tasks.values()]
    assert states.count(TaskState.DISPATCHED) == 1
    assert states.count(TaskState.READY) == 9
    robot = orch.store.robots["r1"]
    owners = [t.spec.task_id for m in orch.store.missions.values() for t in m.tasks.values()
              if t.robot_id == "r1"]
    assert len(owners) == 1 and robot.current_task_id == owners[0]
    logger.info("10 线程抢 1 台机器人：1 个 DISPATCHED、9 个 READY，持有者=%s", owners)


def test_concurrent_exclusive_resource_single_owner():
    orch = make_orch([
        Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0),
        Robot(robot_id="r2", capabilities=frozenset({"carry"}), battery=80.0),
    ])
    barrier = threading.Barrier(8)

    def submit(i):
        barrier.wait()
        orch.submit_mission(f"key-{i}", [
            TaskSpec(task_id=f"t{i}", capability="carry", required_resources=("zone-A",)),
        ])

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    lease = orch.store.leases["zone-A"]
    owner_states = [
        t.state for m in orch.store.missions.values() for t in m.tasks.values()
        if t.spec.task_id == lease.owner_task_id
    ]
    assert owner_states == [TaskState.DISPATCHED]  # 资源持有者确实是那个被派发的任务
    logger.info("8 线程抢独占资源 zone-A：唯一持有者=%s", lease.owner_task_id)


def test_dispatch_failure_reasons_distinguishable():
    orch = make_orch([
        Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=10.0),
        Robot(robot_id="r2", capabilities=frozenset({"carry"}), battery=90.0),
    ])
    from fleet.models import Task
    # 无能力匹配
    ok, reason = orch.scheduler.try_dispatch(Task(spec=TaskSpec(task_id="x1", capability="fly")))
    assert (ok, reason) == (False, DispatchFailure.NO_CAPABLE_ROBOT)
    # 电量不足（r1 低电量，r2 被占）
    orch.submit_mission("k0", [TaskSpec(task_id="busy", capability="carry")])
    ok, reason = orch.scheduler.try_dispatch(Task(spec=TaskSpec(task_id="x2", capability="carry", min_battery=50.0)))
    assert (ok, reason) == (False, DispatchFailure.BATTERY_TOO_LOW)
    # 机器人忙（r1 也被占掉之后）
    orch.submit_mission("k0b", [TaskSpec(task_id="busy2", capability="carry", min_battery=5.0)])
    ok, reason = orch.scheduler.try_dispatch(Task(spec=TaskSpec(task_id="x3", capability="carry", min_battery=5.0)))
    assert (ok, reason) == (False, DispatchFailure.ROBOT_BUSY)
    # 资源被占
    orch2 = make_orch([
        Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0),
        Robot(robot_id="r2", capabilities=frozenset({"carry"}), battery=80.0),
    ])
    orch2.submit_mission("k1", [TaskSpec(task_id="holder", capability="carry", required_resources=("zone-A",))])
    ok, reason = orch2.scheduler.try_dispatch(
        Task(spec=TaskSpec(task_id="x4", capability="carry", required_resources=("zone-A",))))
    assert (ok, reason) == (False, DispatchFailure.RESOURCE_BUSY)
    logger.info("失败原因可区分：%s", [r.value for r in DispatchFailure][1:5])


def test_high_priority_preempts_running_task():
    orch = make_orch([Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0)])
    orch.submit_mission("low", [TaskSpec(task_id="t-low", capability="carry", priority=1)])
    orch.report_robot_event("r1", "started")
    orch.submit_mission("high", [TaskSpec(task_id="t-high", capability="carry", priority=9)])

    low = orch.store.missions[[k for k, m in orch.store.missions.items()
                               if m.idempotency_key == "low"][0]].tasks["t-low"]
    high = orch.store.missions[[k for k, m in orch.store.missions.items()
                                if m.idempotency_key == "high"][0]].tasks["t-high"]
    assert high.state == TaskState.DISPATCHED and high.robot_id == "r1"
    assert low.state == TaskState.READY  # 被抢占后回退待调度，不再算在执行
    assert low.robot_id is None
    assert orch.store.robots["r1"].current_task_id == "t-high"
    logger.info("高优先级抢占：t-high=DISPATCHED，t-low 回退 READY 且已释放机器人")


def test_high_priority_preempts_resource_holder():
    orch = make_orch([
        Robot(robot_id="r1", capabilities=frozenset({"carry"}), battery=90.0),
        Robot(robot_id="r2", capabilities=frozenset({"carry"}), battery=80.0),
    ])
    orch.submit_mission("low", [TaskSpec(task_id="t-low", capability="carry",
                                         required_resources=("zone-A",), priority=1)])
    orch.submit_mission("high", [TaskSpec(task_id="t-high", capability="carry",
                                          required_resources=("zone-A",), priority=9)])
    assert orch.store.leases["zone-A"].owner_task_id == "t-high"
    logger.info("高优先级抢占独占资源：zone-A 持有者=t-high")
