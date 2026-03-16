# Tail Buy 0.95 Sequence

这部分研究要回答的问题是：

- 如果在历史回测范围内，只要某个盘口某一侧到过 `0.95`，就买入这一侧
- 并且固定按 `0.95` 成本买入，持有到结算
- 如果最后没有反转，就按 `0.95 -> 1.00` 赚钱
- 如果最后发生反转，这一笔就亏光
- 再按时间顺序串起来，看 `1000u` 本金、`100` 份分仓、每笔 `10u` 时的资金曲线和回撤

## 数据口径

- 市场全集沿用旧的 `tail_reversal_095` 研究范围，也就是 `17391` 个市场
- `tail_reversal_095_reversals.jsonl` 里的 `437` 个样本视为亏损交易
- 其余触发到 `0.95` 的样本视为盈利交易
- 实际重建出的触发交易总数是 `3839`
- 其中：
  - 盈利 `3402`
  - 亏损 `437`

## 时间清洗

本次图表只保留 **截至 2026-03-16 UTC 已经结算的样本**。

原因是原始交易序列里有一部分市场虽然已经触发过 `0.95`，但 `market_end_time` 晚于 `2026-03-16`，如果直接放进资金曲线，会把未来才结算的仓位也算进去，导致图里出现 `2027` 时间。

这次已经做了清洗：

- `analysis_cutoff_utc = 2026-03-16T00:00:00+00:00`
- `excluded_future_settlement_markets = 188`

也就是说，图表展示的是 **清洗后的已结算样本**，不是把未来市场混进来后的结果。

## 回测假设

- 初始本金：`1000u`
- 分仓数：`100`
- 单笔投入：`10u`
- 入场价：固定 `0.95`
- 盈利结算：回收到 `1.00`
- 亏损结算：该笔归零
- 如果同一时间段仓位已满，则新信号跳过

## 结果摘要

当前图表对应的关键结果在 [chart_summary.json](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/tail_buy_095_sequence/chart_summary.json)。

这版清洗后的资金管理结果是：

- 实际成交 `620` 笔
- 因仓位满跳过 `3031` 笔
- 最终权益 `510.0`
- 总收益 `-490.0`
- 最大回撤 `50.0261%`
- 最长连续亏损 `4`

## 图表说明

### [capital_curve.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/tail_buy_095_sequence/capital_curve.png)

资金曲线。

- 横轴：时间（UTC）
- 纵轴：账户价值
- 含义：按时间顺序执行策略后，账户净值如何变化

### [drawdown_curve.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/tail_buy_095_sequence/drawdown_curve.png)

回撤曲线。

- 横轴：时间（UTC）
- 纵轴：相对历史高点的回撤比例
- 含义：策略在历史过程中曾经从峰值回撤了多少

### [outcome_count.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/tail_buy_095_sequence/outcome_count.png)

盈亏笔数分布图。

- `Win`：买入 `0.95` 高价侧后，最终没有发生反转
- `Loss`：买入 `0.95` 高价侧后，最终发生反转

### [monthly_loss_rate.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/tail_buy_095_sequence/monthly_loss_rate.png)

按月聚合的亏损率。

- 横轴：触发月份
- 纵轴：该月触发交易中的亏损占比
- 含义：看不同时间段里，反转亏损是否明显抬升

## 相关数据文件

- [summary.json](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/tail_buy_095_sequence/summary.json)
  - 交易序列整体统计
- [all_entries.jsonl](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/tail_buy_095_sequence/all_entries.jsonl)
  - 全部重建交易样本
- [successful_entries.jsonl](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/tail_buy_095_sequence/successful_entries.jsonl)
  - 盈利样本
- [failed_entries.jsonl](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/tail_buy_095_sequence/failed_entries.jsonl)
  - 亏损样本
- [missing_trigger_markets.jsonl](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/tail_buy_095_sequence/missing_trigger_markets.jsonl)
  - 在当前重建逻辑里没找到 `0.95` 触发时点的市场
- [failed_markets.jsonl](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/tail_buy_095_sequence/failed_markets.jsonl)
  - 技术上处理失败的市场，不代表交易亏损
