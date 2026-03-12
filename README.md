# polymarket_backtest_dome

这是一个基于 Dome API 的 Polymarket 研究型回测项目。当前先聚焦三个方向：

- 尾盘高概率价格区间的反转频率统计，比如 `0.95+`
- 加密市场的波动率与到达率研究
- BTC 五分钟 `Yes/No` 市场的价格到达率统计

目录先保持简单，按“配置、数据、公共能力、研究主题”来分，不按接口拆得太细。

## 目录结构

```text
polymarket_backtest/
├── README.md
├── dome_docs/
├── configs/
├── data/
│   ├── raw/
│   └── processed/
├── src/
│   ├── api/
│   ├── data/
│   ├── research/
│   │   ├── tail_reversal/
│   │   ├── btc_5m_arrival/
│   │   └── vol_arrival/
│   └── utils/
├── notebooks/
├── reports/
│   ├── figures/
│   └── tables/
├── tests/
└── result/
```

## 怎么分

`configs/`

- 放实验配置。
- 比如市场筛选条件、时间范围、阈值参数、输出路径。

`data/raw/`

- 放从 Dome API 拉回来的原始数据。
- 尽量不改字段，方便回溯和重复清洗。

`data/processed/`

- 放清洗后的标准化数据和研究样本表。
- 比如统一时间单位后的 trades、orderbook panel、尾盘反转样本。

`src/api/`

- 放 Dome API 的请求封装。
- 只负责“怎么拿数据”，不负责研究逻辑。

`src/data/`

- 放数据清洗、标准化、拼表逻辑。
- 负责把原始接口结果整理成可分析的数据集。

`src/research/tail_reversal/`

- 放尾盘反转课题相关逻辑。
- 例如尾盘窗口定义、阈值筛选、反转标签、统计汇总。

`src/research/vol_arrival/`

- 放波动率与到达率课题相关逻辑。
- 例如收益率、波动率、成交到达率、盘口特征计算。

`src/research/btc_5m_arrival/`

- 放 BTC 五分钟 `Yes/No` 市场的价格到达率课题。
- 目前用 `markets + candlesticks`，统计 `Yes` 和 `No` 在 `0.52-0.58` 各档价格的到达频率。

`src/utils/`

- 放公共工具。
- 比如时间处理、分页处理、日志、通用 helpers。

`notebooks/`

- 放探索性分析。
- 主要用于快速验证假设，不放正式生产逻辑。

`reports/`

- 放图表和表格输出。
- `figures/` 放图片，`tables/` 放统计结果。

`tests/`

- 放测试。
- 先统一放一个目录，后面代码量变大了再细分。

`result/`

- 保留现有目录，不动。
- 如果后面确认用途稳定，再决定是否并入 `reports/` 或 `data/processed/`。

## 当前可运行脚本

目前已经实现了第一版 `0.95` 反转统计脚本，主数据源改为 `markets + candlesticks`：

```bash
export DOME_API_KEY=your_api_key
python3 -m src.research.tail_reversal.analyze_threshold --threshold 0.95
```

脚本会：

- 拉取所有 `closed` 市场
- 对每个市场拉对应时间范围内的 `candlesticks`
- 找出最终输掉的那一边
- 扫输掉一边的 `price.high`
- 只要任一 candle 的 `price.high >= 0.95`，就记为一次反转
- 按市场更新进度，支持断点续跑
- 只保存进度统计和反转市场明细

输出文件默认在：

- `data/processed/tail_reversal_095_reversals.jsonl`
- `data/processed/tail_reversal_095_progress.json`

如果中途中断，可以继续跑：

```bash
export DOME_API_KEY=your_api_key
python3 -m src.research.tail_reversal.analyze_threshold --threshold 0.95 --resume
```

这会：

- 读取已有的反转明细
- 读取 `progress.json` 里的市场分页位置
- 从上次断点继续处理后续市场

## BTC 五分钟到达率脚本

新增了一套独立的 BTC 五分钟到达率脚本，输出全部放在单独目录下，不会和 `tail_reversal` 冲突：

```bash
export DOME_API_KEY=your_api_key
python3 -m src.research.btc_5m_arrival.analyze_arrival --min-threshold 0.52 --max-threshold 0.58 --step 0.01
```

脚本会：

- 扫描所有 `closed` 市场
- 只筛选 BTC 五分钟、且 outcome 为 `Yes/No` 的市场
- 对每个匹配市场拉 `1m candlesticks`
- 统计 `Yes` 和 `No` 历史最高价分别到达过哪些价格档位
- 汇总 `0.52-0.58` 每一档的到达次数和到达频率
- 支持断点续跑

输出默认在：

- `data/raw/btc_5m_arrival/markets/selected_markets.jsonl`
- `data/processed/btc_5m_arrival/progress.json`
- `data/processed/btc_5m_arrival/summary.json`
- `data/processed/btc_5m_arrival/hits.jsonl`
- `data/processed/btc_5m_arrival/failed_markets.jsonl`

继续跑的命令：

```bash
export DOME_API_KEY=your_api_key
python3 -m src.research.btc_5m_arrival.analyze_arrival --resume
```

## 这样分的原因

- 你现在的目标是研究型回测，不是做一个完整交易系统。
- 目前只有两个研究主题，直接按主题拆最清晰。
- 公共部分只保留 `api`、`data`、`utils` 三层，避免一开始抽象过度。
- 等后面研究主题变多，再考虑把 `research/` 下的共性逻辑继续拆分。
