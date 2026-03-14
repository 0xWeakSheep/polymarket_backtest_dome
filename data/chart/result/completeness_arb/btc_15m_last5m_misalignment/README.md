# BTC 15m Last5m Misalignment Charts

本目录存放 `btc_15m_last5m_misalignment` 回测结果的图表与摘要文件。

## 图表说明

### `overall_funnel.png`

这张图展示整体漏斗：

- `Processed`：已经实际处理过的 15 分钟 BTC 盘口数量
- `Eligible`：满足研究前置条件的窗口数量
- `Opportunity`：最终出现过 `15m_target_side + last5m_opposite_side < 1.0` 机会的窗口数量

这张图适合快速看：

- 总样本有多大
- 前置条件筛掉了多少窗口
- 最终真正出现机会的比例有多高

### `hit_rate_by_pattern.png`

这张图按两种路径拆分：

- `up_up`：前两个 5 分钟子盘口方向都为 Up
- `down_down`：前两个 5 分钟子盘口方向都为 Down

每根柱子分成两部分：

- `Opportunity`：该路径下出现机会的窗口数
- `No Opportunity`：该路径下未出现机会的窗口数

柱内的百分比表示：

- 在该路径的 `eligible` 样本里，出现机会的比例

这张图适合比较：

- `up_up` 和 `down_down` 哪条路径更容易出现机会
- 哪条路径样本更多

### `edge_distribution.png`

这张图展示机会的 `edge` 分布。

这里的定义是：

- `edge = 1.0 - price_sum`
- `price_sum = 15m目标方向价格 + 最后5m反方向价格`

`edge` 越大，说明价格和 1 的偏离越大，也就是机会越“厚”。

这张图适合看：

- 大多数机会是轻微错价，还是明显错价
- `up_up` 和 `down_down` 两条路径的机会厚度是否不同

### `daily_opportunity_windows.png`

这张图按 UTC 天聚合。

包含两层信息：

- 柱状图：每天出现机会的窗口数
- 折线图：每天在 `eligible` 样本中的机会占比

这张图适合看：

- 哪些日期机会明显更多
- 某些日期是不是机会密度也更高，而不只是样本更多

## 摘要文件

### `chart_summary.json`

这是图表配套的结构化摘要，包含：

- 总 processed / eligible / opportunity 数量
- 机会占全部 processed 的比例
- 机会占 eligible 的比例
- `up_up` / `down_down` 的拆分统计
- 机会最多的 UTC 日期
- 当前 `edge` 最大的一条机会记录

如果后面要做进一步分析或写结论，可以优先看这个文件。
