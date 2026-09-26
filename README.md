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
| `app/pipeline.py` | 单事务编排：读→归一化→落库→提交位点 |
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
| `POST /schema` | 更新声明 schema：`{"fields": {"id": "str", ...}}`；删列/改名返回 409 |

## 测试

```bash
uv run pytest -q
```

- `tests/test_ingest.py` — 畸形记录归类（含 JSONL/CSV），批次内合法记录隔离；
- `tests/test_resume.py` — 重复消费幂等、重启续传、位点回退拒绝；
- `tests/test_concurrency.py` — 4 线程并发消费同一来源：不重复、不丢失、
  位点竞争失败者被显式拒绝；
- `tests/test_schema_evolution.py` — 加列缺省语义、删列/改名拒绝、流水重启可复现；
- `tests/test_api.py` — HTTP 端到端。

测试日志（`pipeline` logger）打印每个批次的位点区间、拒绝明细与变更流水，
可用 `uv run pytest -s --log-cli-level=INFO` 观察。
