# ThetaData 全量美股期权链采集器

本项目通过官方 `thetadata` Python SDK 获取多个美股标的的历史期权链，将 OHLC、Quote 和 Greeks 按合约分钟拼接，可选择按日保存 CSV，或实时批量写入 ClickHouse。

## 安装

要求 Python 3.12 或更高版本。

```powershell
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

复制 `config.example.toml` 为 `config.toml`，填写 ThetaData API key、ClickHouse 和 Lark 配置。真实配置已被 `.gitignore` 忽略。

```toml
api_key = "your_api_key"
symbols = ["NVDA", "AAPL", "CBRS", "NBIS"]
output_dir = "data"
```

## 运行

日期范围首尾均包含，格式必须是 `YYYY-MM-DD`：

```powershell
.\venv\Scripts\python.exe main.py --start-date 2026-09-15 --end-date 2026-09-15 --storage csv
```

`--storage` 默认为 `csv`。CSV 模式下，有有效完成标记的 `symbol + data_date` 会在调用数据接口前直接跳过。需要重新获取上游修订数据时使用 `--force`：

```powershell
.\venv\Scripts\python.exe main.py --start-date 2026-09-15 --end-date 2026-09-15 --force
```

ClickHouse 模式同样实时调用 ThetaData，不读取或生成 CSV。`--force` 不允许用于此模式：

```powershell
.\venv\Scripts\python.exe main.py --start-date 2026-09-15 --end-date 2026-09-15 --storage clickhouse
```

程序通过 `option_list_expirations()` 获取全部到期日，再用 trade/quote 两类 `option_list_dates()` 判断哪些到期日在指定范围内有数据。三个历史接口固定传入：

- `interval="1m"`
- `strike="*"`
- `right="both"`

程序不传 `start_time` 和 `end_time`，由 ThetaData SDK 使用默认美东时间 `09:30:00–16:00:00`。API 返回的 timezone 不做转换；CSV timestamp 会保留原始本地时间及 UTC offset，例如 `2026-09-15T09:30:00-0400`。

### AWS Linux 服务管理

服务器需要预先安装 Python 3.12 或更高版本，并在项目根目录准备好 `config.toml`。首次启动会自动创建 `.venv` 并安装依赖；任务通过 `nohup` 在后台运行，SSH 断开不会终止采集。

```bash
chmod +x scripts/theta-harvest.sh scripts/start_aws_linux.sh

./scripts/theta-harvest.sh start 2026-09-15 2026-09-15
./scripts/theta-harvest.sh start 2026-09-15 2026-09-15 --storage csv --force
./scripts/theta-harvest.sh start 2026-09-15 2026-09-15 --storage clickhouse
./scripts/theta-harvest.sh status
./scripts/theta-harvest.sh stop
```

旧启动命令仍然可用，并会透传所有参数：

```bash
./scripts/start_aws_linux.sh 2026-09-15 2026-09-15
./scripts/start_aws_linux.sh 2026-09-15 2026-09-15 --force
```

日志保存在 `logs/`，当前任务 PID 保存在 `run/theta-harvest.pid`。`start` 会拒绝重复启动；`stop` 在确认 PID 属于本项目后发送 `SIGTERM`，最多等待 30 秒，不会自动强制终止其他进程。

## CSV 存储与完成标记

每个 symbol 每个数据日期保存一个文件，目录格式为 `data/YYYY/MM/DD/SYMBOL.csv`：

```text
data/
  2026/
    09/
      15/
        .AAPL.complete.json
        .NVDA.complete.json
        AAPL.csv
        NVDA.csv
      16/
        .AAPL.complete.json
        AAPL.csv
```

CSV 唯一键是 `symbol, expiration, strike, right, timestamp`，`data_date` 保存该文件对应的数据日期。只有该日的到期日发现、三个历史接口、CSV 合并和原子落盘全部成功后，才会原子写入 `.SYMBOL.complete.json`。

完成标记记录格式版本、请求签名、symbol、日期、状态、完成时间、CSV 文件名、文件大小、行数，以及已检查和实际写入的到期日。普通运行会验证标记版本及 CSV 文件大小：标记损坏、版本不符、CSV 缺失或大小变化时，该日会重新完整抓取。所有列表请求均成功且确认无数据时写入 `no_data` 标记，不创建 CSV。

完整成功的日期使用本次结果重建当日 CSV，不保留上游已经移除的数据。请求或写入失败的日期不会生成完成标记；本次成功的部分数据可以落盘，并保留未成功刷新的旧到期日数据，下次普通运行仍会完整重抓该日。已有 CSV 但没有完成标记时，也会重新抓取一次。

写入过程使用无压缩 Arrow 临时分片和 Polars 流式 CSV 输出，不再通过 Python 逐行解析及写入。正式 CSV 和完成标记都先写同目录临时文件，再原子替换目标文件。

旧版 `data/<year>/<symbol>.csv` 文件不会自动迁移或参与新日文件去重。修复时间范围和时区后，应重新抓取所需日期生成新的日文件。

## ClickHouse 存储与进度

先执行 [clickhouse_schema.sql](clickhouse_schema.sql) 创建并核对正式表。程序启动时也会执行相同的 `CREATE TABLE IF NOT EXISTS`，随后严格检查列类型、引擎、分区键和排序键；不兼容时立即退出，不会自动修改表。

ClickHouse 模式将每个完整日期的 Arrow 临时分片通过 Native TCP、LZ4 和 columnar insert 分批写入 `thetadata_options_chain_1m`，成功后才写 `thetadata_options_chain_1m_progress`。已有进度的 `symbol + data_date` 在创建 ThetaData client 前跳过。确认无数据时只写 `no_data` 进度。

数据表使用无版本列的 `ReplacingMergeTree()`。人工删除某日进度后再次同步，不会删除旧数据；相同排序键在后台 merge 后保留后写入记录。merge 前要求立即去重的查询应使用 `FINAL`。如果新版彻底缺少旧唯一键，旧行不会自动消失，这类修复需要人工删除对应数据分区行后再同步。

ClickHouse 只保存键、OHLC、Quote，以及 `delta, gamma, theta, vega, rho, underlying_time, underlying_price`。NaN 和正负无穷写为 `NULL`，合法零值保留。时间列使用 `America/New_York`。

## 错误和重试

每个真实 API 请求以及客户端鉴权最多尝试 5 次，使用指数退避。日志包含请求上下文、异常类型、源文件和代码行号。单个批次最终失败后会跳过该完整批次并继续处理，程序结束时汇总失败并返回非零退出码。

错误通过配置的 Lark webhook 即时发送。有重试的操作只在第 5 次仍失败时告警；拼接、暂存、CSV/ClickHouse 写入和未处理异常发生时立即告警。Lark 自身失败只记本地日志，不递归告警。Webhook、API key 和 ClickHouse 密码不会写入日志或消息。

当 gRPC 返回 `UNAUTHENTICATED` 或 `Invalid session ID` 时，后台进程会使用配置中的 API key 重新认证并替换底层 `ThetaClient`，随后由原重试周期继续刚才失败的请求。该过程不会重置 symbols、到期日循环或已经生成的临时分片。若另一台机器持续使用同一 API key，多个独立会话仍可能相互使 session 失效；程序最多尝试 5 次，不会无限重连。

官方 SDK 和 HTTP 客户端的 INFO 日志已关闭，避免日志输出账户资料；API key 不写入日志、CSV 或完成标记。

## 真实 API 测试

测试不使用 mock，会读取 `config.toml` 并调用生产 API。测试数据写入 pytest 临时目录，不会修改正式 `data/`：

```powershell
.\venv\Scripts\python.exe -m pytest -m live -s
```

真实测试会完整抓取配置中四个 symbols 在 `2026-09-15` 的全部可用期权链，验证完成标记；随后普通重复运行验证零写入和文件修改时间不变，再使用 `--force` 进行第二次真实抓取。全量数据很大，且运行时间受 ThetaData 服务和网络代理稳定性影响，测试可能持续数小时并产生数 GB 临时文件。
