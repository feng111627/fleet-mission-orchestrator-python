# Fleet Mission Orchestrator

基于内存状态的机器人车队任务调度与执行编排服务（FastAPI + 纯 Python 领域层），
用于在对接真机前跑通 **任务下发 → 派发 → 抢占 → 异常恢复** 的完整链路。

## 结构

```
fleet/
  models.py        # 任务/机器人/作业区模型与状态枚举（Task.zones 多区声明）
  errors.py        # 可区分原因的业务错误（RejectReason、卡住资源说明）
  store.py         # 内存状态仓库，所有读写在同一把 RLock 内
  scheduler.py     # 选人 + 精确抢占规划（只读 DispatchPlan，假定调用方持锁）
  orchestrator.py  # 提交/派发/抢占/暂停/取消/异常恢复，规划在锁内原子执行
main.py            # HTTP API 薄层
tests/             # 四类场景共 24 个用例
```

## 调度规则

任务以**任务图**形式提交（`POST /graphs`），每个任务声明所需能力 `capability`、
独占作业区 `zone`、优先级 `priority` 与依赖 `dependencies`。

- **多作业区任务**：可额外声明 `zones: ["pickup", "dropoff", ...]`，表示执行期间
  需要**同时独占一整组作业区**（数量不固定，自动去重）。省略 `zones` 时退化为
  `[zone]`，单区老用法与既有行为完全一致。派发时这组区必须**一次性全部占上**
  才算成功：只要有一个区拿不到，任务留在 `READY`，**不会先占一部分再等补**，
  也不会扣住任何空闲机器人。未登记的 zone id 按"无需加锁"处理（兼容老行为）；
  显式提交空 `zones` 列表会被 400 拒绝。
- **幂等提交**：同一 `idempotency_key` 重复提交直接返回首次结果，不重复入图、不重复调度。
- **依赖门控**：依赖未全部 `SUCCEEDED` 的任务保持 `PENDING`，不会被派发；
  依赖成环或依赖不存在时提交直接报错（422/400），不会永远卡住。
- **选人规则**（`Scheduler.plan_dispatch`，按序判定，失败原因可区分）：
  1. 无具备所需能力的机器人 → `no_capable_robot`
  2. 候选全部掉线/故障 → `robot_unavailable`
  3. 候选电量全部低于 `MIN_BATTERY`(20) → `battery_too_low`
  4. 整组作业区里有一个拿不到 → `zone_occupied`
  5. 候选机器人全被同级/更高优先级任务占着 → `robot_busy`
  6. 多个合格候选取**电量最高**者，并列取 `robot_id` 较小者（确定性）。

  挑机器人按三档进行，尽量少打扰无关任务：① 优先用空闲机器人（只抢作业区
  占用者）；② 其次用"正跑在某个作业区占用者上"的机器人（抢它同时腾出区和
  机器）；③ 再退而抢占一台正执行严格低优先级任务的机器人。
- **独占约束**：作业区同一时间只归一个非终态任务；机器人同一时刻只执行一个任务。
  所有状态变更都在 `store.lock` 内完成，并发抢同一组区/机器人时恰好一个成功，
  失败者留在 `READY`，`error` 带可区分原因，`blocked_resource` 形如
  `zone:z-a@task-x(priority=1)` / `robot:r1@task-y(priority=9)`，直接说明被哪个
  资源、什么优先级的任务卡住（`GET /state` 可查）。

## 抢占与重试策略

- **抢占范围（只动真正挡路的资源）**：高优先级任务被挡住时，抢占对象**仅限**
  正占着它本次所需作业区、或正占着目标机器人的任务。在**别的区、别的机器**上
  执行的无关低优任务一律不动，继续照常执行。
- **优先级边界（严格更低才能抢）**：挡路任务必须优先级**严格低于**本任务才可被
  抢占。多个挡路者中只要有一个是同级或更高优先级（含暂停中保留作业区的任务），
  则**整体都不抢**——不会先抢掉一半再发现抢不动；本任务继续排队，拒绝原因为
  `zone_occupied`，`blocked_resource` 指向第一个同级/高优挡路者。机器人全被
  同级/高优任务占着时同理，原因 `robot_busy`。
- **抢占的原子执行**：调度器先只读地算好"要抢的全部任务 + 要用的机器人"
  （`DispatchPlan`），编排器在同一把锁内一次性执行：每个被抢任务**放开它占有的
  机器人和全部作业区**、回到 `READY` 重新排队（`error` 记录
  `preempted by <task>`），随后新任务把整组区与机器人原子占上。资源空出来后，
  被抢任务可再次被派发。
- **暂停/恢复**：`pause` 释放机器人回池但**保留整组作业区**；`resume` 放开保留
  的区、回到 `READY` 重新走整组原子派发。暂停中的任务若占着高优任务所需的区，
  且其优先级严格更低，同样会被整体抢占。
- **取消**：任何非终态任务可取消，机器人与整组作业区立即一次性全部释放。
- **异常恢复**：机器人掉线（`offline`）、上报故障（`fault`）或执行失败上报时，
  在执任务占有的机器人与**全部**作业区一次性释放，再按 `fault_policy` 处理：
  - `RETRY`（默认）：释放占用、回到 `READY` 重试，最多 `max_retries` 次，耗尽后置 `FAILED`；
  - `ROLLBACK`：直接置 `FAILED` 并释放全部占用。
- **并发与资源原子性**：派发决策与执行都在 `store.lock` 一把锁内，多线程同时抢
  同一组区时恰好一个成功，其余拿到 `zone_occupied`/`robot_busy` 与
  `blocked_resource`。各释放路径（取消/抢占/失败/掉线/故障）按"声明的整组区 +
  兜底扫描实际归属它的区"清理，杜绝只释放一部分、悬挂占用或两个任务同占一区。
- **自洽性不变量**：任意时刻，被占作业区一定属于某个非终态任务；`DISPATCHED`
  任务的机器人一定 `BUSY` 且 `current_task` 回指该任务；终态任务不持有任何资源。

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

复现**多作业区原子派发与精确抢占**（取货区→卸货区）：

```bash
# 两台机器人 + 三个区
curl -X POST :8000/robots -H 'content-type: application/json' \
  -d '{"robot_id":"r1","capabilities":["delivery"],"battery":90}'
curl -X POST :8000/robots -H 'content-type: application/json' \
  -d '{"robot_id":"r2","capabilities":["delivery"],"battery":80}'
for z in pickup dropoff other; do
  curl -X POST :8000/zones -H 'content-type: application/json' -d "{\"zone_id\":\"$z\"}"
done

# 低优任务占住 pickup
curl -X POST :8000/graphs -H 'content-type: application/json' -d '{
  "idempotency_key":"mz-1",
  "tasks":[{"task_id":"low","capability":"delivery","zone":"pickup","priority":1}]}'
curl -X POST :8000/dispatch            # -> {"dispatched":["low"]}

# 高优任务同时需要 pickup+dropoff：整组抢占 low（只动 pickup 占用者）
curl -X POST :8000/graphs -H 'content-type: application/json' -d '{
  "idempotency_key":"mz-2",
  "tasks":[{"task_id":"high","capability":"delivery","zone":"pickup",
            "zones":["pickup","dropoff"],"priority":9}]}'
curl -X POST :8000/dispatch            # -> {"dispatched":["high"]}
curl :8000/state                       # low 回 ready 且不持资源；pickup/dropoff 均归 high
```

> 验证"拿不全不留部分占用"：再提交一个 `zones` 同时含空闲区和被占区的同级任务，
> `/dispatch` 返回空，`/state` 中该任务为 `ready`、`error=zone_occupied`、
> `blocked_resource` 指出具体被哪个任务占住，而它声明的其它空闲区仍为空闲。

测试覆盖四类场景（见 `tests/`，日志中 `场景:` 与 `[CASE]` 行逐条列出用例与结果）：

| 场景 | 文件 | 用例 |
| --- | --- | --- |
| 重复提交与图校验 | `test_duplicate_submission.py` | 幂等重试、依赖门控、成环报错、悬空依赖 |
| 并发抢占 | `test_concurrent_preemption.py` | 10 线程抢同一机器人仅 1 胜、高优先级抢占、拒绝原因可区分 |
| 异常恢复 | `test_fault_recovery.py` | 掉线重试、重试耗尽、回退策略、取消/暂停的资源自洽 |
| 多作业区任务 | `test_multi_zone.py` | 整组派发成功、拿不全不留部分占用、精确抢占不牵连无关任务、多挡路者整体不抢（含暂停中挡路）、16 线程抢同一组区仅 1 胜、取消/掉线/故障/失败整组释放、被抢后重新排队再派发 |
