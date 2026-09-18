# Project Progress

## Current Status

ThetaData 全量期权链采集器现已实现按 `symbol + data_date` 的持久化完成标记和增量跳过。只有到期日/日期发现、三类历史接口、CSV 合并及原子落盘全部成功的日期才会标记完成；普通重复运行跳过有效标记，`--force` 会在创建 ThetaData client 前移除请求范围内的标记并完整重抓。Python 编译、AWS Bash 脚本语法检查和 5 个非 live 测试均通过，当前无代码阻塞。为避免与 AWS 后台任务争抢同一 API session，本次没有执行真实 API 全链测试。

## Completed

- 支持配置多个 symbols、命令行日期范围和 1m OHLC/Quote/Greeks 获取。
- 使用 expirations 和 trade/quote dates 接口发现全部有效到期日及数据日期。
- 三个历史接口不传 `start_time/end_time`，使用 ThetaData 默认美东 `09:30:00–16:00:00`。
- timestamp 不转换时区；CSV 保留 API 返回的本地时间与 offset。
- 实现 5 次指数退避、完整 traceback、失败批次隔离和非零退出码。
- 新增 `RefreshingThetaClient`：仅在 gRPC `UNAUTHENTICATED` 或 `Invalid session ID` 时重新认证，普通网络异常不重建 client。
- 实现三表全外连接、公共字段合并及按 `data/YYYY/MM/DD/SYMBOL.csv` 的日分区写入。
- 写入使用无压缩 Arrow 临时分片、Polars 流式 CSV 合并和原子替换。
- 新增 `data/YYYY/MM/DD/.SYMBOL.complete.json`，记录 marker/数据版本、请求签名、状态、完成时间、CSV 文件名/大小/行数、已检查及已写入到期日。
- 完成标记验证覆盖 JSON 损坏、版本不符、CSV 缺失和文件大小变化；无效时重新抓取。
- 全部列表请求成功但当天无数据时写 `no_data` 标记，不要求 CSV 存在。
- 完整日期使用本次抓取结果重建日 CSV；不完整日期可保留本次成功部分和旧的未刷新到期日，但不生成完成标记。
- 普通重复运行按 symbol/date 跳过；当请求范围全部完成时，在创建 ThetaData client 前直接成功退出。
- 新增 CLI `--force`；强制刷新在客户端鉴权前移除范围内所有标记，失败后旧 CSV 保留但日期保持未完成。
- `scripts/theta-harvest.sh start START_DATE END_DATE [--force]` 已支持强制刷新；兼容脚本继续透传参数。
- 真实 API live 测试已扩展为首次抓取、零写入跳过、文件大小/mtime 不变及 `--force` 第二次真实抓取。
- 非 live 文件系统测试通过：`5 passed, 1 deselected`；Python `compileall` 通过；两个 AWS shell 脚本通过 Git Bash `bash -n`。
- 既有真实 API 验证：NVDA 单到期日全部 strike/right 共 53,958 行；timestamp 为 `America/New_York`，范围严格为 09:30–16:00、391 分钟。
- 既有写入性能验证：198,720 行真实历史数据首次约 0.30 秒，重复覆盖约 0.69 秒。

## In Progress

无。

## Known Issues

- 如果另一台机器持续使用同一 API key，多个独立登录会不断互相使 session 失效；自动重建最多随单请求尝试 5 次，不会无限抢占会话。
- 本次未运行真实 API 全链 live 测试，以免影响 AWS 上可能正在使用同一 API key 的后台进程；live 验收状态仍待服务器侧确认。
- 未完成日期按日完整重抓，不实现单个 expiration/API 请求级别的跨进程断点。
- 旧版 `data/<year>/<symbol>.csv` 不会自动迁移到日目录，也不会参与新日文件的完成判定。
- 四个 symbols 的完整单日强制双跑可能持续数小时并产生数 GB 临时文件。
- ThetaData gRPC 经本机 SOCKS 代理偶发流中断，重试机制已在既有真实调用中验证可恢复。

## Next Steps

- 将新版部署到 AWS，停止旧后台任务后再启动新版，使完成标记、增量跳过和 `--force` 生效。
- 选择一个已结束历史日期执行首次真实抓取，核对每日 CSV 与 `.SYMBOL.complete.json` 的状态、文件大小和行数。
- 对同一日期普通重复运行，确认日志显示跳过且 CSV 大小和修改时间不变。
- 在确保没有其他 ThetaData session 后执行一次 `--force` 或 `python -m pytest -m live -s`，完成四 symbols 真实 API 验收。
