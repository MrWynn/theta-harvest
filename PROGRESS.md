# Project Progress

## Current Status

ThetaData 采集器现已支持单个认证 session 内的有界并发采集；正式 PRO 配置使用 8 workers。ClickHouse 历史采集严格按日期顺序执行，仅在当天 expiration 之间并发，当天写入和临时分片清理完成后才开始下一天。列表及三个历史接口的 `NoDataFoundError` 均按正常空结果处理，同日其他 expiration 的有效数据仍会写入。23 个非 live 测试全部通过；当前无代码阻塞。

## Completed

- 保留多 symbols、日期范围、1m OHLC/Quote/Greeks、全外连接、全部 strike/right 和 ThetaData 默认美东请求时段。
- CSV 继续按 `data/YYYY/MM/DD/SYMBOL.csv` 写入，保留 API timestamp，不进行上海时区转换。
- CSV 完成标记继续按 `symbol + data_date` 校验和跳过；`--force` 仅用于 CSV。
- CLI 新增 `--storage csv|clickhouse`，默认 `csv`；ClickHouse 与 `--force` 组合会在加载配置前拒绝。
- ClickHouse 模式不读取或生成 CSV/CSV 完成标记，进度完成时可在创建 ThetaData client 前跳过。
- 新增 `clickhouse_schema.sql`，数据表为无版本列 `ReplacingMergeTree()`，月分区且完整唯一键进入排序键；进度表为 `ReplacingMergeTree(completed_at)`。
- 程序只执行 `CREATE TABLE IF NOT EXISTS`，并校验列类型、引擎、分区键、主键和排序键，不自动修改已有表。
- 正式 `laevitas.thetadata_options_chain_1m` 与进度表已创建/确认兼容；校验兼容 ClickHouse 将 `toYYYY()` 规范化为 `toYear()` 的行为。
- ClickHouse 仅保存键、OHLC、Quote、`delta/gamma/theta/vega/rho/underlying_time/underlying_price`；`underlying_timestamp` 映射为 `underlying_time`。
- NaN/正负无穷转换为 `NULL`，合法零值保留；Call/Put 规范化为表内 `CALL/PUT` Enum。
- 写入采用本地无压缩 Arrow 分片、Polars streaming batches、约 250,000 行批次和 clickhouse-driver columnar insert，关闭逐值类型检查。
- 数据全部批次成功后才写 `complete` 进度；确认无数据只写 `no_data`；插入中断或部分 API 失败不写进度。
- 进度缺失时直接重新采集并插入，不执行数据 DELETE；同键数据由 `ReplacingMergeTree()` 后台合并。
- Lark 告警包含主机、模式、日期、上下文、操作、异常类型和截断 traceback；重试操作仅在最终耗尽时发送一次。
- Lark 最多尝试 3 次，发送失败只写日志；API key、ClickHouse 密码和 Webhook 会从消息中脱敏。
- AWS 管理脚本支持透传 `--storage csv|clickhouse` 和 `--force`；兼容脚本继续透传全部参数。
- `config.toml` 已写入正式 ClickHouse/Lark 配置且仍被 `.gitignore` 忽略；`config.example.toml` 只含占位值。
- 新增 clickhouse-driver LZ4 和 httpx 依赖，当前本地虚拟环境已安装 ClickHouse 驱动。
- 非 live 测试通过：`19 passed, 1 deselected`，覆盖字段白名单、时间映射、NaN/零值、列式插入、失败不写进度、no_data、FINAL 进度查询、CLI 互斥和本地 HTTP Lark 重试。
- 真实测试首次写入时，ThetaData 全部 26 个相关到期日检查和 25 个有数据到期日暂存成功；ClickHouse 第二批因原 120 秒读写超时失败，已验证只残留首批 53,958 行且进度表没有误写完成。
- ClickHouse `send_receive_timeout` 已提高到 900 秒；失败后普通重跑会完整重新采集，不删除旧行，并由 `ReplacingMergeTree()` 合并相同键。
- 修正后真实全链写入成功：本轮写入 1,584,332 行，进度为 `complete`，`checked_expiration_count=26`、`written_expiration_count=25`。
- 正式表验收通过：原始行数与 `FINAL` 行数均为 1,584,332，复合唯一键数同为 1,584,332；包含 25 个到期日、279 个行权价及 `CALL/PUT`。
- 真实时间范围为 `2026-09-15 09:30:00-04:00` 至 `16:00:00-04:00`，最早到期日 `2026-09-16`，最晚 `2029-01-19`。
- 同日期第二次运行约 6.4 秒完成，明确输出“请求范围内所有 symbol/date 均已同步，不创建 ThetaData client”，没有重复采集或写入。
- 测试后 `config.toml` 的 symbols 已恢复为 `NVDA, AAPL, CBRS, NBIS`。
- 新增 `max_concurrent_requests` 配置，范围 1–8；缺失时默认为 1，正式 PRO 配置为 8，示例配置为 4。
- 并发分为日期发现和历史批次两阶段；不同到期日并发，每个 `expiration + data_date` 内 OHLC/Quote/Greeks 仍串行，避免嵌套超限。
- 有界任务调度器最多保留 workers 个运行中或待消费结果；主线程按完成顺序立即写 Arrow，CSV/ClickHouse 完整性语义保持不变。
- `RefreshingThetaClient` 并发 session 失效测试通过：多个旧 session 请求同时失败时只创建一个新 client。
- 并发失败测试通过：任一历史任务最终失败时，ClickHouse 不写数据和进度；CSV 并发写入仍生成完整、排序正确的日文件和 marker。
- CSV 合并已改为显式按 `timestamp, expiration, strike, right` 排序，消除并发完成顺序对文件行序的影响。
- 真实并发测试 `NVDA / 2026-09-16`：26 个日期发现任务 15.396 秒全部成功；25 个历史批次 545.007 秒全部成功；ClickHouse 写入 1,599,972 行耗时 85.239 秒；总耗时 648.189 秒。
- 并发真实数据核验通过：`FINAL` 行数与唯一键均为 1,599,972，25 个有数据到期日、279 个行权价、CALL/PUT，美东时间严格为 09:30–16:00，进度为 `complete`。
- 真实并发期间没有 `UNAUTHENTICATED` 或订阅限流；首次 expirations 请求的一次本机 SOCKS 握手中断由原有重试恢复。
- 同日期第二次运行直接显示“所有 symbol/date 均已同步，不创建 ThetaData client”。
- `option_list_dates()` 返回 `NoDataFoundError` 时不再执行五次重试或发送 Lark 告警，而是将对应 `trade`/`quote` 类型视为空集合并继续查询另一类型；两类均无数据时视为该到期日已成功检查。
- 新增日期发现回归测试，覆盖单侧无数据仍使用另一侧日期，以及 trade/quote 均无数据的正常空结果；非 live 测试现为 `21 passed, 1 deselected`。
- ClickHouse 长日期范围改为逐日同步：日期发现仍只执行一次，历史任务按 `data_date` 分组，每天内部最多 8 个 expiration 并发；当天成功写入数据和进度后立即删除当天 Arrow 分片，再开始下一天。
- 历史任务失败的日期不写 ClickHouse 或进度，已暂存的当日分片也会清理；处理结束后按现有容错语义继续下一天。
- 新增跨日调度回归测试，确认下一天请求开始前上一天已经写入且临时分片已删除；非 live 测试现为 `22 passed, 1 deselected`，`compileall` 通过。
- OHLC、Quote、Greeks 历史接口返回 `NoDataFoundError` 时不再重试或发送 Lark，而是将该接口视为空 DataFrame 并继续请求及全外连接另外两个接口。
- 单个 `expiration + date` 三个历史接口均为空时，该批次正常产生 0 行；只要同日其他 expiration 有数据，仍会写入 ClickHouse 并在整日成功后写完成进度。
- 新增混合空数据回归测试，覆盖“OHLC 为空但 Quote/Greeks 有数据”及“一个 expiration 全空、其他 expiration 有数据”；非 live 测试现为 `23 passed, 1 deselected`，`compileall` 通过。

## In Progress

无。

## Known Issues

- 如果另一台机器持续使用同一 ThetaData API key，多个独立登录仍会互相使 session 失效；本轮已确认测试期间没有其他任务占用。
- `ReplacingMergeTree()` 只替换相同排序键；新版彻底缺少的旧唯一键不会自动删除，需要人工清理对应 symbol/date 后重同步。
- 后台 merge 完成前查询可能看到同键多行；需要即时去重时使用 `FINAL`。
- ClickHouse 数据写入成功但进度写入失败时，下次会重写整日；这是预期的至少一次写入语义，最终由 ReplacingMergeTree 合并。
- 未完成日期按日完整重抓，不实现单 expiration/API 的跨进程断点。
- 当前 Windows 环境的 WSL Bash 启动被系统拒绝，因此本轮未重新执行 `bash -n`；脚本改动仅为参数透传，需在 AWS 部署前复核。
- 本机真实测试经过 `127.0.0.1:10809` SOCKS 代理，8 workers 会共享代理/下行带宽；本机总耗时较此前本机串行约 13 分钟改善约 20%，未达到理论 2–3 分钟，AWS 直连效果需单独测量。

## Next Steps

- 在 Amazon Linux 上运行 `bash -n scripts/theta-harvest.sh scripts/start_aws_linux.sh`，再使用服务脚本启动 ClickHouse 模式。
- 在 AWS 直连环境记录 8 workers 的阶段耗时；若服务端或网络争用导致收益有限，只需将 `max_concurrent_requests` 调整为 4。
- 根据实际业务日期运行其余配置 symbols；NVDA 2026-09-15 和 2026-09-16 已有完成进度，将自动跳过。
- 如需验证人工重同步，可删除目标 symbol/date 的进度后再运行，并分别用普通查询和 `FINAL` 检查物理重复及合并结果。
- 将本次 `NoDataFoundError` 分类修复部署到 AWS 后，复跑此前失败日期，确认日志只输出一次“按空结果处理”且继续完成其他到期日。
- 在 AWS 使用长日期范围观察新的“开始历史日批次/历史日批次完成”日志，确认 `/tmp` 只保留当前日期分片并监控内存峰值。
- 部署历史接口空结果修复后重跑 `NVDA / 2026-02-10`，确认不再发送 `NoDataFoundError` Lark 告警，并能为同日其他 expiration 写入数据及进度。
