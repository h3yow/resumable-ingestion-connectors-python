# Resumable Ingestion Connectors (Python)

本地可运行、可恢复的数据摄入与落库服务。用本地文件（JSONL / CSV）模拟来源端、
SQLite 模拟目标端，保证在**来源结构变化、重复消费、并发写入**下数据状态可预期。

## 快速开始

```bash
uv sync                      # 安装依赖
uv run pytest -q             # 运行全部测试（含并发/重启/结构演进场景）
uv run python main.py        # 启动服务，默认 http://127.0.0.1:8000
```

唯一配置项：环境变量 `INGEST_DB`，SQLite 数据库文件路径，默认 `./ingest.db`。

## 架构

```
SourceConnector (jsonl/csv) --> normalize --> Sink (SQLite) 
                                     |            |
                                     v            v
                              rejections 表   records 表 + change_log
                                     |
                              CheckpointStore (checkpoints 表)
```

| 模块 | 职责 |
|---|---|
| `app/connectors/` | 连接器协议与 JSONL/CSV 实现 |
| `app/normalize.py` | 归一化与校验，拒绝原因可区分 |
| `app/checkpoint.py` | 位点持久化，回退显式拒绝 |
| `app/sink.py` | 幂等落库与结构演进 |
| `app/schema_version.py` | 结构版本注册表：版本号、历史、拒绝留痕 |
| `app/history.py` | 操作流水：摄入与结构变更按真实顺序落库 |
| `app/replay.py` | 干净实例重放与逐项状态比对 |
| `app/pipeline.py` | 单事务编排：读→归一化→落库→提交位点；事务化结构变更 |
| `app/main.py` | FastAPI HTTP 接口 |

## 连接器协议

连接器实现 `SourceConnector` 协议（`app/connectors/base.py`）：

- `name: str` — 来源标识，作为位点与幂等键（`record_id = "{source}#{id}"`）的命名空间；
- `read(from_offset) -> Iterator[RawRecord]` — 产出 `from_offset` 之后的记录，
  `offset` 在同一来源内**严格单调递增**（JSONL/CSV 均为行号）。

连接器不得静默丢弃畸形数据：非 JSON 行、列数不符的 CSV 行等以原始形式产出，
由归一化层统一判定为 `invalid_format`。

## 归一化与拒绝原因

每条记录独立校验，同批合法记录不受畸形记录影响。拒绝原因可区分：

| reason | 含义 |
|---|---|
| `missing_field` | schema 声明的必填字段缺失或为 null |
| `type_mismatch` | 字段类型不匹配且安全强转失败（如 `"abc"` -> int） |
| `invalid_format` | 记录整体非法（非 JSON、非对象、列数不符） |
| `offset_regression` | 位点回退（由检查点存储抛出） |
| `schema_incompatible` | 删列/改名等不兼容演进（由目标端抛出） |

类型强转是确定性的：`"42"`→`42`、`"3.14"`→`3.14`、`bool` 不被当作 `int`。
被拒绝的记录持久化到 `rejections` 表（按 `(source, offset)` 幂等），可审计、可复现。

## 检查点语义

- 每个来源一条位点：`last_offset` 为已确认落库的最大 offset；
- 一个批次内**落库、拒绝记录持久化、位点提交在同一事务**（`BEGIN IMMEDIATE`），
  崩溃不产生半提交状态，重启后从位点续传；
- 位点只允许**严格前进**：提交小于等于当前位点的 offset 抛出
  `OffsetRegressionError`（HTTP 409），绝不"从中间继续"；
- `BEGIN IMMEDIATE` 使并发消费者的"读位点→落库→提交位点"整体串行化，
  位点回退检查不会被快照隔离绕过。

## 幂等与并发

- 目标表以 `record_id` 为主键，写入走 UPSERT：重复消费、重复触发、
  并发写入均不产生重复行；
- 多消费者竞争同一来源时，只有一个能推进位点，其余被显式拒绝
  （`OffsetRegressionError`），目标端不重复、不丢失。

## 结构演进（兼容规则）

- **新增列：允许。** 缺省值按类型确定且稳定：`str→''`、`int→0`、`float→0.0`、
  `bool→0`、其他→`NULL`。历史行读回时恒定得到该缺省值；
- **删列：拒绝。** 记录 `column_drop_rejected` 流水并抛 `SchemaEvolutionError`（HTTP 409）；
- **改名：拒绝。** 同一次声明更新中删一列且加一列视为改名，记录
  `column_rename_rejected` 流水并抛错；
- 所有演进事件写入 `change_log` 流水（`seq` 单调递增），重启后重放结论一致，
  规则更新不影响历史结论的可复现性。

## 结构版本

每次结构变更尝试都记入 `schema_history`，可查询、可审计：

- **版本号**：首次启动以初始 schema 建立 `v1`；此后每次兼容变更（新增列）
  产生一个新版本，**接在当前版本后面、严格递增、不跳号**（被拒绝的尝试不消耗版本号）；
- **历史可查**：`GET /schema` 看当前生效版本与字段，`GET /schema/history`
  看全部条目——每个版本具体改了什么（`detail`）、每次被拒绝的尝试及其原因；
- **旧版本覆盖**：旧版本结构是当前版本的子集，覆盖尝试按删列规则被明确拒绝；
  也可在 `POST /schema` 携带 `base_version` 做乐观并发控制，基线过期返回 409，
  绝不会"拿旧版本悄悄覆盖新版本还当成成功"；
- **重启保证**：重启后从库中加载**最新已生效版本**继续运行，不报错、不退回旧结构；
  重启后的新变更接在最新版本之后继续编号；
- **拒绝留痕**：不兼容变更被拒绝时不分配版本号，但尝试本身写入历史
  （`status=rejected`），事后可查。

## 历史重放

每次摄入批次与每次结构变更尝试（含被拒绝的）都按真实发生顺序写入 `op_log`
（与对应业务数据在**同一事务**提交，流水与状态始终一致）。

- **导出**：`GET /history` 返回全部操作（`ingest` / `schema`），`seq` 即真实顺序；
  代码侧用 `pipeline.history.export()`；
- **重放**：`app.replay.replay(ops, pipeline, sources)` 在一个**全新的干净实例**上
  按序重放——摄入操作按记录的批次区间重新消费对应来源，结构变更按记录的字段
  重新应用（被拒绝的变更要求重放时同样被拒绝，否则报错）；
- **比对**：`app.replay.diff_states(db_a, db_b)` 逐项比对两个实例的数据行、
  每来源位点、被拒记录、结构版本历史、演进流水与操作流水，返回空 dict 表示
  完全一致。比对字段不含时间戳等不确定内容，结论可复现。

## 并发结构变更

- 结构变更（含版本分配、声明字段更新、历史留痕）在 `BEGIN IMMEDIATE`
  事务内整体提交：**任何时刻读到的都是某个完整版本的结构**，不存在改了一半的
  中间状态；
- 变更中途失败**整体回滚**：版本、声明字段、位点与数据全部停在变更前；
- 不兼容变更被明确拒绝时，拒绝留痕与"结构不生效"同时成立——留痕入库、
  结构/位点/数据不动；
- 摄入与结构变更并发进行时，各自事务由 SQLite 串行化；多消费者竞争同一来源
  仍由位点乐观并发保护，不重复、不丢失（上万条记录、多消费者场景有测试覆盖）。

## HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `GET /health` | 健康检查 |
| `POST /sources` | 注册来源：`{"type": "jsonl"\|"csv", "path": ..., "name": ...}` |
| `POST /ingest/{source}?batch_size=100` | 消费一个批次；位点回退返回 409 |
| `GET /checkpoints` | 所有来源位点 |
| `GET /records` | 目标端全部记录 |
| `GET /rejections` | 拒绝记录及原因 |
| `GET /changes` | 结构演进流水 |
| `POST /schema` | 更新声明 schema：`{"fields": {...}, "base_version": 可选}`；删列/改名/基线过期返回 409 |
| `GET /schema` | 当前生效的结构版本与字段 |
| `GET /schema/history` | 结构版本历史（含被拒绝的尝试） |
| `GET /history` | 导出全部历史操作（摄入+结构变更，按真实顺序），可用于重放 |

## 测试

```bash
uv run pytest -q
```

- `tests/test_ingest.py` — 畸形记录归类（含 JSONL/CSV），批次内合法记录隔离；
- `tests/test_resume.py` — 重复消费幂等、重启续传、位点回退拒绝；
- `tests/test_concurrency.py` — 4 线程并发消费同一来源：不重复、不丢失、
  位点竞争失败者被显式拒绝；
- `tests/test_schema_evolution.py` — 加列缺省语义、删列/改名拒绝、流水重启可复现；
- `tests/test_schema_version.py` — 结构版本递增不跳号、拒绝留痕、重启后按最新
  版本运行、变更中途失败整体回滚；
- `tests/test_replay.py` — 导出历史操作并在干净实例上重放，逐项状态一致；
- `tests/test_concurrent_schema.py` — 上万条记录、多消费者并发摄入过程中做
  结构变更（含不兼容变更被明确拒绝），任意时刻只观察到完整版本；
- `tests/test_api.py` — HTTP 端到端（含结构版本与历史导出接口）。

测试日志（`pipeline` logger）打印每个批次的位点区间、拒绝明细、结构变更记录
与变更流水，可用 `uv run pytest -s --log-cli-level=INFO` 观察。

## 本地验证新能力

```bash
# 1. 结构版本与重启后继续生效
uv run pytest -s tests/test_schema_version.py

# 2. 干净实例重放历史操作并逐项比对
uv run pytest -s tests/test_replay.py

# 3. 并发摄入过程中的结构变更（含不兼容拒绝）
uv run pytest -s tests/test_concurrent_schema.py

# 4. 手动体验：启动服务后查询版本与历史
uv run python main.py &
curl -X POST localhost:8000/schema -H 'content-type: application/json' \
     -d '{"fields": {"id": "str", "name": "str", "age": "int"}}'
curl localhost:8000/schema          # 当前生效版本
curl localhost:8000/schema/history  # 版本历史（含被拒绝的尝试）
curl localhost:8000/history         # 操作流水，可导出用于重放
```
