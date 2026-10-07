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
tests/             # 四类场景共 21 个用例
```

## 调度规则

任务以**任务图**形式提交（`POST /graphs`），每个任务声明所需能力 `capability`、
独占作业区、优先级 `priority` 与依赖 `dependencies`。

### 多作业区任务

- **声明方式**：任务除主作业区 `zone` 外，可用 `zones` 声明数量不固定的一组作业区
  （如"从取货区送到卸货区"：`"zone":"pick","zones":["pick","drop"]"`）。
  只给 `zone`、不给 `zones` 的**老用法完全不变**；任务实际需要的区由
  `Task.required_zones` 统一得到（自动去重、保序，并兼容 `zone`）。
- **原子占用**：派发时必须把这组区**一次性全部占上**才算成功；只要有一个区拿不到，
  任务就留在 `READY` 等待，**不会先占住空闲的那一部分**。执行期间这组区全部归它
  独占，别的任务不能共用。按能力和电量挑执行者的规则保持不变。
- **受阻可查**：等待中的任务在 `error` 带原因（`zone_occupied` / `robot_busy` 等），
  并通过 `blocked_resource`（哪个区/机器人）与 `blocked_by`（被哪个任务占着）
  能看出具体卡点；`GET /state` 同样返回这两个字段与 `zones`。

- **幂等提交**：同一 `idempotency_key` 重复提交直接返回首次结果，不重复入图、不重复调度。
- **依赖门控**：依赖未全部 `SUCCEEDED` 的任务保持 `PENDING`，不会被派发；
  依赖成环或依赖不存在时提交直接报错（422/400），不会永远卡住。
- **选人规则**（`Scheduler.pick_robot`，按序判定，失败原因可区分）：
  1. 无具备所需能力的机器人 → `no_capable_robot`
  2. 候选全部掉线/故障 → `robot_unavailable`
  3. 候选电量全部低于 `MIN_BATTERY`(20) → `battery_too_low`
  4. **声明的所有作业区有任意一个被其他任务占用 → `zone_occupied`**
  5. 候选全部繁忙 → `robot_busy`
  6. 多个合格候选取**电量最高**者，并列取 `robot_id` 较小者（确定性）。
- **独占约束**：每个作业区同一时间只归一个非终态任务；机器人同一时刻只执行一个任务。
  所有状态变更都在 `store.lock` 内完成，并发抢同一组作业区时恰好一个成功，
  失败者留在 `READY` 且 `error` 字段带有上述可区分原因。

## 抢占与重试策略

- **高优先级抢占（范围精准 + 全有或全无）**：派发时若因 `robot_busy` /
  `zone_occupied` 被拒，编排器只围绕**本次真正需要的资源**做抢占：
  - 只考虑**正占着本次所需作业区**的任务，以及（在没有空闲合格机器人时）一台
    正在执行更低优先级任务的合格机器人；**在别的区、别的机器人上跑的无关任务
    绝不会被牵连**，继续正常执行。
  - 先做"干跑"分析、收集全部待动任务；只要其中有**任意一个**任务的优先级
    **≥ 本任务**（同优先级或更高优先级挡路），就**整体都不抢**，不会出现
    先抢掉一半再发现抢不动。挡路的是同/更高优先级时高优任务继续排队，
    `blocked_resource`/`blocked_by` 标明是被哪个资源、谁卡住。
  - 只有当全部待动任务都**严格低优先级**时，才一次性把它们各自占用的
    **机器人与全部作业区全部释放**，并置回 `READY` 重新排队
    （`error` 记录 `preempted by <task>`），随后高优任务重试派发；
    被抢任务等资源空出来后可再次被派出。
- **暂停/恢复**：`pause` 释放机器人回池但**保留全部作业区**；`resume` 放开这些区、
  回到 `READY` 重新走派发。
- **取消**：任何非终态任务可取消，机器人与全部作业区立即一次性释放。
- **异常恢复**：机器人掉线（`offline`）或上报故障（`fault`）时，其在执任务的
  机器人与**全部作业区一次性释放**，再按 `fault_policy` 处理：
  - `RETRY`（默认）：释放占用、回到 `READY` 重试，最多 `max_retries` 次，耗尽后置 `FAILED`；
  - `ROLLBACK`：直接置 `FAILED` 并释放全部占用。
- **并发与资源原子性**：任务的"选人—抢占—占用/释放"整段都在同一把 `store.lock` 内，
  多线程同时提交/派发时，同一组作业区只有一个任务能占上，其余拿到
  `zone_occupied`/`robot_busy` 等可区分原因；取消、抢占、失败、掉线、故障
  任一路径都不会留下只释放一部分、资源悬挂或两个任务同占一个区的状态。
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

# 多作业区任务：一次性占住 pick + drop（老的单区写法不受影响）
curl -X POST :8000/zones -H 'content-type: application/json' -d '{"zone_id":"drop"}'
curl -X POST :8000/graphs -H 'content-type: application/json' -d '{
  "idempotency_key":"multi-1",
  "tasks":[{"task_id":"haul","capability":"delivery","zone":"pick","zones":["pick","drop"]}]}'
curl -X POST :8000/dispatch            # -> {"dispatched":["haul"]}，pick/drop 同归 haul
curl :8000/state | python -m json.tool # zones 两个区 occupied_by 都是 haul
```

新增多作业区行为可用如下方式复现与核对：

```bash
# 只跑多作业区场景（日志逐条打印每个用例的场景与 PASSED/FAILED）
uv run pytest tests/test_multi_zone.py -v -s
```

测试覆盖四类场景（见 `tests/`，日志中 `[CASE]` 行逐条列出用例与结果）：

| 场景 | 文件 | 用例 |
| --- | --- | --- |
| 重复提交与图校验 | `test_duplicate_submission.py` | 幂等重试、依赖门控、成环报错、悬空依赖 |
| 并发抢占 | `test_concurrent_preemption.py` | 10 线程抢同一机器人仅 1 胜、高优先级抢占、拒绝原因可区分 |
| 异常恢复 | `test_fault_recovery.py` | 掉线重试、重试耗尽、回退策略、取消/暂停的资源自洽 |
| 多作业区任务 | `test_multi_zone.py` | 多区一次性占全、拿不全无部分占用且卡点可查、抢占只动挡路者、多受害者全有或全无、同/更高优先级挡路不抢、8 线程抢同一组区仅 1 胜、被抢任务资源空出后重派、失败/掉线/故障/取消多资源整体释放 |
