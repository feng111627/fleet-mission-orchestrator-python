# 车队任务调度与执行编排（内存模拟版）

在 FastAPI 服务上实现的一套简化机器人车队任务编排：任务以带依赖的任务图提交，
在内存中模拟机器人车队，跑通任务下发、抢占与异常恢复链路，便于后续对接真机。

## 模块结构

| 模块 | 职责 |
| --- | --- |
| `fleet/models.py` | 任务/机器人/资源租约模型与状态枚举 |
| `fleet/store.py` | 内存状态存储，单把 `RLock` 保护全部读写，资源租约全有或全无 |
| `fleet/scheduler.py` | 选机与资源占用，决策全程在锁内，并发安全 |
| `fleet/orchestrator.py` | 任务图校验、幂等提交、派发、暂停/取消/抢占、故障重试 |
| `fleet/api.py` | REST 接口层 |

## 调度规则

任务进入 `READY`（依赖全部成功）后按 **优先级降序、task_id 升序** 依次派发。
为单个任务挑选执行者时按以下顺序判定，失败原因可区分（`DispatchFailure`）：

1. `NO_CAPABLE_ROBOT`：没有任何机器人具备所需 `capability`；
2. `ROBOT_OFFLINE` / `ROBOT_BUSY`：有能力的机器人全部掉线/故障或全部在忙；
3. `BATTERY_TOO_LOW`：空闲机器人电量都低于任务的 `min_battery`；
4. `RESOURCE_BUSY`：所需独占资源（`required_resources`）已被其他任务持有；
5. 全部满足时，在空闲且电量达标的机器人中选 **电量最高** 的一台，
   并在同一把锁内原子完成「占资源 + 占机器人 + 任务置 DISPATCHED」，
   因此并发抢同一机器人/资源时绝不会出现两个任务都声称抢到。

## 抢占与重试策略

- **抢占**：派发失败且原因是 `ROBOT_BUSY`/`RESOURCE_BUSY` 时，在 **全局所有任务** 中
  寻找优先级严格更低、且释放了恰好能解堵（占着兼容机器人或所需资源）的活动任务，
  抢占优先级最低的一个：其资源与机器人被释放、状态回退为 `READY`（记 `PREEMPTED` 原因），
  **不消耗重试次数**，之后会被自动重新调度。
- **重试**：机器人上报 `failed`/`offline`/`fault` 时，若任务 `attempts <= max_retries`，
  释放其占用并回退 `READY` 重新调度（可迁移到其他机器人）；否则置 `FAILED`，
  下游依赖任务级联 `CANCELLED`。
- **暂停/取消**：`pause` 保留机器人与资源占用（便于 `resume` 继续）；
  `cancel` 立即释放全部占用并置 `CANCELLED`，下游级联取消。
- **一致性约定**：任务离开活动态（DISPATCHED/RUNNING/PAUSED）时必定同时释放资源租约
  并解除机器人绑定；机器人 `OFFLINE`/`FAULT` 状态不会因任务释放而被误重置。
  测试中的 `assert_consistent` 对每条链路校验这些不变量。

## 幂等与任务图校验

- 提交必须带 `idempotency_key`，重复提交（含并发重试）返回同一个 `mission_id`，不重复调度；
- 依赖未满足的任务保持 `PENDING`，不会被提前派发；
- 依赖引用未知任务或依赖成环时，提交直接报 `DependencyError`（HTTP 400），不会卡死。

## 本地验证

```bash
uv sync
uv run pytest            # 全部用例，日志按场景归类输出
uv run python main.py    # 启动服务（随机端口，见输出）
```

测试日志中每个用例会输出一行 `[场景:...] 用例:... 结果:PASSED`，覆盖三类场景：

- `tests/test_duplicate_submission.py`：重复提交（含 20 线程并发重试）、依赖门控、环/未知依赖报错；
- `tests/test_concurrent_preemption.py`：多线程抢单台机器人/独占资源（恰好一个赢家）、
  失败原因可区分、高优先级抢占机器人与资源；
- `tests/test_failure_recovery.py`：故障/掉线重试与迁移、重试耗尽后级联取消、
  取消释放全部占用、暂停/恢复、抢占后状态自洽；
- `tests/test_api.py`：HTTP 端到端链路。

### curl 示例

```bash
curl -X POST localhost:8000/robots -H 'content-type: application/json' \
  -d '{"robot_id":"r1","capabilities":["carry"],"battery":90}'
curl -X POST localhost:8000/missions -H 'content-type: application/json' \
  -d '{"idempotency_key":"demo-1","tasks":[{"task_id":"t1","capability":"carry","required_resources":["zone-A"]}]}'
curl -X POST localhost:8000/robots/r1/events -H 'content-type: application/json' -d '{"event":"started"}'
curl -X POST localhost:8000/robots/r1/events -H 'content-type: application/json' -d '{"event":"succeeded"}'
curl localhost:8000/state
```
