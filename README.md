# Fleet Mission Orchestrator

基于内存状态的机器人车队任务调度与执行编排服务（FastAPI + 纯 Python 领域层），
用于在对接真机前跑通 **任务下发 → 派发 → 抢占 → 异常恢复** 的完整链路。

## 结构

```
fleet/
  models.py        # 任务/机器人/作业区模型与状态枚举
  errors.py        # 可区分原因的业务错误（RejectReason 等）
  store.py         # 内存状态仓库，所有读写在同一把 RLock 内
  scheduler.py     # 选人规则（无状态，假定调用方持锁）
  orchestrator.py  # 提交/派发/抢占/暂停/取消/异常恢复
main.py            # HTTP API 薄层
tests/             # 三类场景共 13 个用例
```

## 调度规则

任务以**任务图**形式提交（`POST /graphs`），每个任务声明所需能力 `capability`、
独占作业区 `zone`、优先级 `priority` 与依赖 `dependencies`。

- **幂等提交**：同一 `idempotency_key` 重复提交直接返回首次结果，不重复入图、不重复调度。
- **依赖门控**：依赖未全部 `SUCCEEDED` 的任务保持 `PENDING`，不会被派发；
  依赖成环或依赖不存在时提交直接报错（422/400），不会永远卡住。
- **选人规则**（`Scheduler.pick_robot`，按序判定，失败原因可区分）：
  1. 无具备所需能力的机器人 → `no_capable_robot`
  2. 候选全部掉线/故障 → `robot_unavailable`
  3. 候选电量全部低于 `MIN_BATTERY`(20) → `battery_too_low`
  4. 作业区被其他任务占用 → `zone_occupied`
  5. 候选全部繁忙 → `robot_busy`
  6. 多个合格候选取**电量最高**者，并列取 `robot_id` 较小者（确定性）。
- **独占约束**：作业区同一时间只归一个非终态任务；机器人同一时刻只执行一个任务。
  所有状态变更都在 `store.lock` 内完成，并发抢同一机器人/作业区时恰好一个成功，
  失败者留在 `READY` 且 `error` 字段带有上述可区分原因。

## 抢占与重试策略

- **高优先级抢占**：派发时若因 `robot_busy` / `zone_occupied` 被拒，编排器会寻找
  正在执行**更低优先级**任务、且能力匹配的机器人，抢占之（被抢占任务释放机器人与
  作业区、回到 `READY` 重新排队，`error` 记录 `preempted by <task>`），随后重试选人。
- **暂停/恢复**：`pause` 释放机器人回池但**保留作业区**；`resume` 回到 `READY` 重新走派发。
- **取消**：任何非终态任务可取消，机器人与作业区立即全部释放。
- **异常恢复**：机器人掉线（`offline`）或上报故障（`fault`）时，其在执任务按
  `fault_policy` 处理：
  - `RETRY`（默认）：释放占用、回到 `READY` 重试，最多 `max_retries` 次，耗尽后置 `FAILED`；
  - `ROLLBACK`：直接置 `FAILED` 并释放全部占用。
- **自洽性不变量**：任意时刻，被占作业区一定属于某个非终态任务；`DISPATCHED` 任务的
  机器人一定 `BUSY` 且 `current_task` 回指该任务；终态任务不持有任何资源。

## 本地验证

```bash
# 跑全部单测（日志会打印每个用例所属场景与结果）
uv run pytest -q

# 启动服务（另开终端用 curl 验证）
uv run python -c "import uvicorn; uvicorn.run('main:app', host='127.0.0.1', port=8000)"

curl -X POST :8000/robots -H 'content-type: application/json' \
  -d '{"robot_id":"r1","capabilities":["delivery"],"battery":90}'
curl -X POST :8000/zones  -H 'content-type: application/json' -d '{"zone_id":"z-a"}'
curl -X POST :8000/graphs -H 'content-type: application/json' -d '{
  "idempotency_key":"demo-1",
  "tasks":[{"task_id":"a","capability":"delivery","zone":"z-a"},
           {"task_id":"b","capability":"delivery","zone":"z-a","dependencies":["a"]}]}'
curl -X POST :8000/dispatch            # -> {"dispatched":["a"]}，b 被依赖门控
curl -X POST :8000/tasks/a/complete -H 'content-type: application/json' -d '{"success":true}'
curl -X POST :8000/dispatch            # -> {"dispatched":["b"]}
curl :8000/state                       # 查看任务/机器人/作业区自洽状态
```

测试覆盖三类场景（见 `tests/`，日志中 `[CASE]` 行逐条列出用例与结果）：

| 场景 | 文件 | 用例 |
| --- | --- | --- |
| 重复提交与图校验 | `test_duplicate_submission.py` | 幂等重试、依赖门控、成环报错、悬空依赖 |
| 并发抢占 | `test_concurrent_preemption.py` | 10 线程抢同一机器人仅 1 胜、高优先级抢占、拒绝原因可区分 |
| 异常恢复 | `test_fault_recovery.py` | 掉线重试、重试耗尽、回退策略、取消/暂停的资源自洽 |
