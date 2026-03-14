# BTC 5m Volatility vs Arrival

本目录存放“前 5 分钟历史波动率”与“当前 5 分钟盘口到达率”之间关系的分析图表。

## 研究口径

- 样本单位：单个 `btc-updown-5m-*` 盘口
- 特征：该盘口开始前 5 分钟的 BTC 现货 1 分钟价格特征
- 标签：该盘口在不同阈值下，`up` / `down` 是否到达
- 到达结果读取自现有 `data/processed/btc_5m_arrival/*` 中的 miss 文件
- 波动率数据写入独立目录 `data/processed/btc_5m_volatility/`

本研究没有修改原有 `btc_5m_arrival` 数据，只是读取其现有结果进行关联分析。

## 当前样本规模

来自 [summary.json](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/btc_5m_volatility/summary.json)：

- 总样本数：`8060`
- 成功处理：`8060`
- 失败：`0`

## 主要指标

本轮先计算了以下几个前 5 分钟特征：

- `return_std_1m_5m`：前 5 分钟 1 分钟收益率标准差
- `abs_return_sum_1m_5m`：前 5 分钟 1 分钟绝对收益之和
- `realized_vol_1m_5m`：前 5 分钟实现波动率
- `range_pct_5m`：前 5 分钟最高最低振幅
- `net_move_pct_5m`：前 5 分钟净涨跌幅

## 当前结论

### 1. 纯波动率和到达率的正相关不强

最接近“历史波动率”定义的 `return_std_1m_5m`，目前没有表现出明显正相关。

表现为：

- 对 `up` 到达，Spearman 相关大致在 `-0.001 ~ 0.012`
- 对 `down` 到达，Spearman 相关大致在 `-0.007 ~ -0.016`

这说明：

- 单看“前 5 分钟抖动是否更剧烈”，暂时看不出它能明显提升后续 5 分钟到达率

### 2. 最强的正相关来自 `net_move_pct_5m`

在本轮结果里，排在最前面的正相关组合是：

- `net_move_pct_5m` + `down` + `0.52`
- Pearson：`0.107946`
- Spearman：`0.119354`

而且在 `0.52 ~ 0.58` 各个阈值上，`net_move_pct_5m` 对 `down` 到达都排在前列。

这说明：

- 前 5 分钟的**净移动方向/幅度**
- 比“波动率强弱本身”
- 更可能和后续 5 分钟的 `down` 到达率有关

换句话说，当前更像是：

- **方向性信号 stronger than 波动率信号**

### 3. `up` 方向只有很弱的正相关

`up` 方向当前最好的组合大致是：

- `range_pct_5m` + `up` + `0.56`
- Spearman：`0.022746`

这个量级很弱，说明：

- 即使前 5 分钟振幅变大
- 对 `up` 到达率也只有非常弱的提升

### 4. 当前最重要的研究结论

如果只问：

- “哪些波动率和到达率呈现正相关？”

那当前最准确的回答是：

- 强正相关没有出现
- `return_std_1m_5m` 这类标准波动率指标基本没有明显信号
- `range_pct_5m` 对 `up` 只有很弱正相关
- `net_move_pct_5m` 对 `down` 有相对更明显的正相关，但它已经偏方向性，不是纯波动率

所以当前更合理的表述是：

- **前 5 分钟的方向性变化，可能比前 5 分钟的波动率大小更能解释到达率变化**

## 图表说明

### `up_arrival_rate_by_return_std_1m_5m_quantile.png`

- 横轴：`return_std_1m_5m` 分位组
- 纵轴：不同阈值下的 `up` 到达率

这张图主要看：

- 波动率从低到高时，`up` 到达率是否单调抬升

当前解读：

- 没有看到特别强的单调上升趋势

### `down_arrival_rate_by_return_std_1m_5m_quantile.png`

- 横轴：`return_std_1m_5m` 分位组
- 纵轴：不同阈值下的 `down` 到达率

当前解读：

- 同样没有看到明显的“波动率越高，到达率越高”

### `up_correlation_heatmap.png`

- 横轴：阈值 `0.52 ~ 0.58`
- 纵轴：不同前 5 分钟特征
- 颜色和格内数字：Spearman 相关系数

当前解读：

- `up` 方向整体相关性偏弱
- 最好的也只是轻微正相关

### `down_correlation_heatmap.png`

- 横轴：阈值 `0.52 ~ 0.58`
- 纵轴：不同前 5 分钟特征
- 颜色和格内数字：Spearman 相关系数

当前解读：

- `down` 方向里，`net_move_pct_5m` 的正相关更明显
- 但整体也还没有达到特别强的量级

## 配套数据文件

### `data/processed/btc_5m_volatility/volatility_arrival_analysis.csv`

每个市场一行，包含：

- 波动率特征
- 不同阈值下的 `up_arrived_*`
- 不同阈值下的 `down_arrived_*`

这是后续做进一步统计分析的主表。

### `data/processed/btc_5m_volatility/correlation_summary.csv`

包含：

- 每个指标
- 每个方向
- 每个阈值
- Pearson 相关
- Spearman 相关

适合快速筛查信号强弱。

### `data/processed/btc_5m_volatility/top_positive_correlations.csv`

按正相关强度排序后的前几项结果，方便快速看当前最强信号。

## 下一步建议

### 1. 不要只盯“波动率”，要重点看“方向性”

当前结果说明：

- `net_move_pct_5m` 比纯波动率更值得深入

建议下一步补图：

- `net_move_pct_5m` 分位组 vs 到达率
- 区分 `up` / `down`
- 看是否存在更明显的单调关系

### 2. 把 `range_pct_5m` 单独拿出来复核

虽然它的信号弱，但比 `return_std_1m_5m` 仍然更有信息量。

建议补：

- `range_pct_5m` 分位组图
- 与 `return_std_1m_5m` 做对比

### 3. 做分时段分析

很可能不是所有时间段都一样。

建议按：

- UTC 小时
- 亚洲时段 / 欧洲时段 / 美盘时段
- 周内不同日期

去拆开看相关性，避免整体均值把局部信号冲掉。

### 4. 做多变量分析

当前是一维相关性，容易把不同效应混在一起。

建议下一步增加：

- 波动率 + 净移动幅度
- 波动率 + 时段
- 波动率 + 前一窗口方向

做简单逻辑回归或分层统计。

### 5. 重新审视策略目标

如果你的目标是“提升到达率”，这批结果已经给出一个重要提示：

- 单纯找“高波动率窗口”可能不是最有效方向
- 更可能要转向“方向性 + 波动 + 时段”联合筛选

## 当前一句话总结

这轮结果更像是在说明：

- **前 5 分钟的方向性变化，比前 5 分钟的波动率强弱，更可能和下一段 5 分钟盘口到达率相关。**
