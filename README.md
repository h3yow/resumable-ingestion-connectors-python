# Resumable Ingestion Connectors (Python / FastAPI)

本地可运行、可恢复的数据摄入与落库服务：以来源文件（JSONL / CSV）模拟来源端，
以 SQLite 模拟目标端，演示在**来源结构变化、重复消费、并发写入**下数据状态
依然可预期的摄入流水线。不依赖任何外部服务或真实凭据。

## 快速开始

```bash
uv sync                      # 安装依赖（fastapi + pytest）
uv run pytest                # 运行全部测试（含并发与结构演进用例）
mkdir -p data                # 放入 events.jsonl / events.csv 作为来源
uv run python main.py        # 启动服务，默认 127.0.0.1:8000
```

## 架构

```
来源文件                连接器                 流水线                 目标端(SQLite)
events.jsonl ──▶ JsonlConnector ─┐
                                 ├─▶ IngestionPipeline ─▶ TargetStore
events.csv   ──▶ CsvConnector  ──┘   fetch→normalize→commit   ├─ records（数据表）
                                                              ├─ checkpoints（位点）
                                                              ├─ rejects（拒绝记录）
                                                              ├─ changelog（变更流水）
                                                              └─ schema_versions（结构版本）
```

## 连接器协议

每个来源连接器实现 `app.connectors.base.SourceConnector`：

| 成员 | 语义 |
| --- | --- |
| `name` | 来源标识，同时是位点命名空间与数据表 `source` 列 |
| `fetch_batch(after_position, limit) -> Batch` | 返回位点 **严格大于** `after_position` 的至多 `limit` 条原始记录；`Batch.end_position` 为本批次扫描到的最大位点 |
| `normalize(RawRecord) -> NormalizedRecord \| RejectedRecord` | 把来源私有字段归一到统一模型；非法记录返回带原因的拒绝对象，**不得抛异常、不得静默丢弃** |

位点约定：来源内单调递增的整数（文件连接器用行号，从 1 开始）。
新连接器通过 `@register` 注册后即可被服务与流水线使用。

### 统一归一模型与拒绝原因

统一模型字段：`record_id, occurred_at(ISO8601), name, value(float), tags`。
拒绝原因（`RejectReason`）可区分且持久化到 `rejects` 表：

| 原因 | 触发场景 |
| --- | --- |
| `missing_field` | 必填字段缺失或为 null |
| `type_mismatch` | 字段类型不符（如 `value` 不是数值、`tags` 不是字符串数组） |
| `invalid_format` | 格式非法（如时间戳非 ISO8601、JSON 行无法解析、CSV 列数不符） |

同一批次内合法记录正常落库，非法记录仅进入 `rejects` 表，互不影响。

## 检查点语义

- **单事务提交**：数据写入、拒绝记录、位点推进在同一 SQLite 事务内完成，
  服务重启后要么整批可见、要么整批不可见，不丢不重。
- **幂等落库**：数据表以 `(source, record_id)` 为主键，`INSERT OR IGNORE`；
  重复消费同一批次的结果是 0 行新增，位点不变。
- **续传**：每次摄入从 `checkpoints` 表中该来源的已提交位点之后继续拉取。
- **位点回退拒绝**：`commit_batch` 的 `end_position` 小于当前检查点时抛出
  `PositionRollbackError`（HTTP 层映射为 409），绝不从中间继续。
- **并发消费**：多个消费者共享目标端时，已被其他消费者覆盖的批次会被
  识别为 superseded 并跳到最新检查点继续（日志有告警），最终状态仍然
  不重不丢；测试 `test_concurrent_consumption_no_dup_no_loss` 覆盖该场景。

## 结构演进（兼容规则）

目标表结构由 `app.schema.TableSchema` 版本化管理，规则确定且可复现：

1. **只允许在末尾追加新列**，且新列必须可选并带确定缺省值
   （`Column.default`，可为 `NULL`）。历史行与新行读到的新列值恒为该缺省值。
2. **删列、改名、改类型、重排一律抛出 `SchemaError`**（HTTP 422），
   绝不静默错列。
3. **版本号严格 +1**；空演进（无新增列）同样被拒绝。
4. 每次演进写入 `schema_versions` 与 `changelog`，同一操作序列重放
   结论一致（见 `test_changelog_reproducible_after_rule_update`）。

## HTTP 端点

| 方法/路径 | 说明 |
| --- | --- |
| `GET /health` | 健康检查 |
| `GET /sources` | 已注册连接器 |
| `POST /ingest/{source}?limit=&drain=` | 摄入一个批次；`drain=true` 拉空为止。位点回退返回 409 |
| `GET /checkpoints/{source}` | 查询来源位点 |
| `GET /rejects` | 拒绝记录及原因 |
| `GET /rows` | 目标表行数与内容 |
| `GET /changelog` | 变更流水（提交与结构演进） |
| `POST /schema/evolve` | 追加新列：`{"add_columns": [{"name","type","default"}]}` |

## 配置项

| 配置 | 环境变量 | 默认值 | 说明 |
| --- | --- | --- | --- |
| 数据目录 | `INGEST_DATA_DIR` | `./data` | 来源文件与 SQLite 库所在目录 |
| 库文件名 | — | `target.db` | 目标端 SQLite 文件 |
| 批次大小 | — | `100` | 单次摄入的默认 `limit` |

来源文件约定：`events.jsonl`（每行一个 JSON 对象）与 `events.csv`
（表头 `event_id,event_time,title,amount,labels`）。

## 测试

```bash
uv run pytest            # 15 个用例
uv run pytest -s -o log_cli=true   # 查看位点与变更流水日志
```

覆盖：拒绝原因分类（含畸形记录）、重复消费幂等、重启续传、并发消费
不重不丢、位点回退拒绝、加列缺省语义、删列/改名/改类型拒绝、
变更流水可复现。测试日志会打印各阶段位点与 `changelog` 流水。
