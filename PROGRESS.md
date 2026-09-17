# Project Progress

## Current Status

ThetaData 全量期权链采集器已实现。静态检查通过；真实 API 已验证四个 symbols 的到期日发现、NVDA 单批次拼接与幂等，以及 CBRS 单日完整期权链写入。当前无代码阻塞。

## Completed

- 支持配置多个 symbols、命令行日期范围、全天 1m OHLC/Quote/Greeks 获取。
- 使用 expirations 和 trade/quote dates 接口发现全部有效到期日及数据日期。
- 实现 5 次指数退避、完整 traceback、失败批次隔离和非零退出码。
- 实现三表全外连接、公共字段合并及按 `data/<year>/<symbol>.csv` 流式幂等写入。
- 真实 CBRS `2026-09-15` 文件为 2,476,800 行、16 个到期日、101 个行权价，复合键无重复。

## In Progress

无。

## Known Issues

- 四个 symbols 的完整单日双跑测试可能持续数小时；本次有界验证未等待 NVDA、AAPL、NBIS 全量测试完成。
- ThetaData gRPC 经本机 SOCKS 代理偶发流中断，重试机制已在真实调用中验证可恢复。

## Next Steps

- 需要完整验收时运行 `.\venv\Scripts\python.exe -m pytest -m live -s`，并预留数小时和数 GB 磁盘空间。
