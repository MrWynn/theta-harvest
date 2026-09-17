# ThetaData 全量美股期权链采集器

本项目通过官方 `thetadata` Python SDK 获取多个美股标的的历史期权链，将 OHLC、Quote 和全部 Greeks 按合约分钟拼接，并按数据年份和 symbol 保存为可重复运行、无重复行的 CSV。

## 安装

要求 Python 3.12 或更高版本。

```powershell
python -m venv venv
.\venv\Scripts\python.exe -m pip install -r requirements.txt
```

复制 `config.example.toml` 为 `config.toml`，填写 ThetaData API key。真实配置已被 `.gitignore` 忽略。

```toml
api_key = "your_api_key"
symbols = ["NVDA", "AAPL", "CBRS", "NBIS"]
output_dir = "data"
```

## 运行

日期范围首尾均包含，格式必须是 `YYYY-MM-DD`：

```powershell
.\venv\Scripts\python.exe main.py --start-date 2026-09-15 --end-date 2026-09-15
```

程序通过 `option_list_expirations()` 获取全部到期日，再用 trade/quote 两类 `option_list_dates()` 判断哪些到期日在指定范围内有数据。历史请求固定为：

- `interval="1m"`
- `strike="*"`
- `right="both"`
- `start_time="00:00:00"`
- `end_time="23:59:59.999"`

输出目录按请求的数据日期年份组织，例如：

```text
data/
  2025/
    AAPL.csv
    NVDA.csv
  2026/
    AAPL.csv
    CBRS.csv
    NBIS.csv
    NVDA.csv
```

CSV 唯一键是 `symbol, expiration, strike, right, timestamp`。`data_date` 保存该行所属的请求日期；ThetaData 返回的 timestamp 保持原值，不用于决定年度目录。重复运行时只替换本次成功获取的 `(data_date, expiration)` 分区，失败批次的旧数据不会被删除。

年度文件采用常量内存流式重写和同目录原子替换，适合数百万行以上的完整期权链。行按抓取批次写入，不保证整个年度文件全局排序。

## 错误和重试

每个真实 API 请求以及客户端鉴权最多尝试 5 次，使用指数退避。日志包含请求上下文、异常类型、源文件和代码行号。单个批次最终失败后会跳过该完整批次并继续处理，程序结束时汇总失败并返回非零退出码。

官方 SDK 和 HTTP 客户端的 INFO 日志已关闭，避免日志输出账户资料；API key 不写入日志或 CSV。

## 真实 API 测试

测试不使用 mock，会读取 `config.toml` 并调用生产 API。测试数据写入 pytest 临时目录，不会修改正式 `data/`：

```powershell
.\venv\Scripts\python.exe -m pytest -m live -s
```

真实测试会完整抓取配置中四个 symbols 在 `2026-09-15` 的全部可用期权链，并连续运行两次验证年度分区和幂等性。全量 Quote/Greeks 数据很大，且运行时间受 ThetaData 服务和网络代理稳定性影响，测试可能持续数小时并产生数 GB 文件。
