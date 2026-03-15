# BTC 5m 波动率精细关联分析

本目录记录的是一轮更细粒度的研究工作：  
不是再去问“历史到达率高不高”，而是进一步追问：

- 前 5 分钟的波动率，是否和当前 5 分钟盘口的到达率有关系
- 如果有关系，这种关系是不是藏在很小的小数位变化里
- 这种关系能不能作为后续筛选条件，帮助提升策略质量

## 这部分工作是在做什么

这轮工作不是替代原来的 `btc_5m_arrival` 研究，而是在它基础上继续往前推进。

前面的纯到达率研究已经回答了一个问题：

- 某些阈值下，到达率本身是存在的

但仅仅有到达率并不等于有交易价值。  
当把价格、赔率和成本一起带入后，会出现一个更关键的问题：

- **即使到达率不错，策略 EV 也可能仍然是负的**

这意味着：

- 不能只看“会不会到达”
- 还要继续找“哪些市场更值得做”

于是这轮工作把重点转向了：

- 能不能用市场开始前的客观特征，去筛出更高质量的 5 分钟盘口

当前选择的第一个特征就是：

- **前 5 分钟历史波动率**

## 这部分具体干了什么

### 1. 新建了独立的波动率研究链路

新增研究模块：

- [compute_pre_5m_volatility.py](/Users/caoxiangrui/Desktop/external/polymarket_backtest/src/research/btc_5m_volatility/compute_pre_5m_volatility.py)

它做的事情是：

- 对每个 `btc-updown-5m-*` 盘口
- 取该盘口开始前 5 分钟的 BTC 现货 1 分钟数据
- 计算几个波动率相关指标

这些结果单独写到：

- [data/processed/btc_5m_volatility](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/btc_5m_volatility)

不会污染原有的 `btc_5m_arrival` 旧数据。

### 2. 把波动率和已有到达结果做关联

后续分析不是重新跑到达率，而是读取已有结果：

- [data/processed/btc_5m_arrival](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/btc_5m_arrival)

再和新生成的波动率表做拼接，得到：

- [volatility_arrival_analysis.csv](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/btc_5m_volatility/volatility_arrival_analysis.csv)

这一步的目标是：

- 对每个 5 分钟盘口
- 同时拥有“前 5 分钟波动率”和“当前盘口是否到达”

### 3. 第一轮先做了整体相关性

最早那版分析主要用：

- Pearson
- Spearman

去看：

- 前 5 分钟波动率
- 和 `0.52 ~ 0.58` 不同阈值到达率

有没有正相关

结论是：

- 线性或单调相关整体不强
- 纯波动率信号不明显

### 4. 第二轮改成更精细的小数位分析

因为 BTC 的前 5 分钟波动率本身数值很小，不能仅凭“相关系数不大”就直接下结论。

所以这轮又专门做了精细扫描：

- 看波动率本身的分布是不是很窄
- 看 arrived / missed 两组在小数位上差多少
- 做更细的分位组
- 做更高精度的 cutoff 扫描

这部分对应脚本：

- [plot_volatility_precision_scan.py](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/script/btc_5m_volatility_precision_scan/plot_volatility_precision_scan.py)

## 当前研究的意义

这部分工作的意义，不是在于“已经找到了一个特别强的波动率因子”，而在于：

### 1. 把研究从“单纯看到达率”推进到了“尝试找可筛选因子”

前面如果只看到达率，很容易停留在：

- 某阈值历史命中不错

但一旦发现：

- 单纯按原始到达率去做，EV 仍可能是负的

那就必须继续寻找：

- 哪些样本更优
- 哪些市场更值得做

波动率关联分析就是在做这个事情。

### 2. 排除一个常见误区

现在至少可以更有把握地说：

- 不是“所有高波动率窗口都会显著提高到达率”

也就是说：

- 不能简单把“高波动率”直接当成强筛选条件

### 3. 给后续因子研究提供方向

当前结果更像在说明：

- 如果信号存在，它不在粗粒度线性相关里
- 它可能藏在更细的小数位变化里
- 或者需要和其他变量联合才会显现

所以这部分工作并不是无效，而是在帮后续研究缩小搜索空间。

## 这轮重点看的两个波动率指标

### `range_pct_5m`

英文解释：

- `5m High-Low Range (%)`

含义：

- 前 5 分钟最高价和最低价之间的振幅
- 再除以起始价格，得到百分比形式

它更像是在衡量：

- **这 5 分钟里价格区间有多宽**

### `return_std_1m_5m`

英文解释：

- `1m Return Std Dev over Prior 5m`

含义：

- 用前 5 分钟里每个 1 分钟收益率
- 计算标准差

它更像是在衡量：

- **这 5 分钟里价格抖动有多剧烈**

## 当前图表怎么读

## 总览图

### `metric_distributions.png`

路径：

- [metric_distributions.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/metric_distributions.png)

这张图用来回答：

- 波动率是不是确实都很小
- 小数点后几位是不是才有主要区别

当前结论是：

- 是的，分布整体很小
- 主要差异确实在很细的小数位上

## 按阈值拆开的子目录

本目录下又按阈值拆成了：

- [0.52](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.52)
- [0.53](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.53)
- [0.54](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.54)
- [0.55](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.55)
- [0.56](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.56)
- [0.57](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.57)
- [0.58](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.58)

每个阈值目录里都有 8 张图，分别是：

- `up/down`
- `range_pct_5m / return_std_1m_5m`
- `quantile / threshold_sweep`

### `*_quantile.png`

例如：

- [0.53/up_range_pct_5m_quantile.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.53/up_range_pct_5m_quantile.png)

含义：

- 把某个波动率指标从低到高切成很多分位组
- 看每一组里的到达率

这里横轴的 `Q1 ~ Q30` 表示：

- `Q1`：该波动率最低的一组
- `Q30`：该波动率最高的一组

也就是说：

- 横轴越往右，代表前 5 分钟波动率越高

这类图适合看：

- 到达率是不是随着波动率升高而单调上升

现在图里还额外标了：

- **峰值点的具体到达率**

方便直接读最大值，而不是只看相对高低。

### `*_threshold_sweep.png`

例如：

- [0.53/up_range_pct_5m_threshold_sweep.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.53/up_range_pct_5m_threshold_sweep.png)

含义：

- 不是把样本切桶
- 而是直接设波动率 cutoff
- 只保留高于某个 cutoff 的样本
- 再看这些样本的到达率

这类图适合看：

- 如果只挑“高波动率”的那部分市场
- 到达率有没有真的变高

## 如果你只看某个阈值，比如 `0.53`

推荐阅读顺序：

1. 先看 [0.53/up_range_pct_5m_quantile.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.53/up_range_pct_5m_quantile.png)
2. 再看 [0.53/up_return_std_1m_5m_quantile.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.53/up_return_std_1m_5m_quantile.png)
3. 然后对照 [0.53/up_range_pct_5m_threshold_sweep.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.53/up_range_pct_5m_threshold_sweep.png)
4. 再看 [0.53/up_return_std_1m_5m_threshold_sweep.png](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/chart/result/btc_5m_volatility_precision_scan/0.53/up_return_std_1m_5m_threshold_sweep.png)

如果 `range_pct_5m` 的图更明显抬升，就说明：

- 对这个阈值来说
- `range_pct_5m` 比 `return_std_1m_5m` 更值得关注

## 配套数据文件

这部分的结构化数据在：

- [data/processed/btc_5m_volatility_precision_scan](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/btc_5m_volatility_precision_scan)

重点看这些：

### `metric_distribution_summary.csv`

- 波动率总体分布
- 适合确认数值到底有多小

### `up_arrived_vs_missed_summary.csv`
- [up_arrived_vs_missed_summary.csv](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/btc_5m_volatility_precision_scan/up_arrived_vs_missed_summary.csv)

### `down_arrived_vs_missed_summary.csv`
- [down_arrived_vs_missed_summary.csv](/Users/caoxiangrui/Desktop/external/polymarket_backtest/data/processed/btc_5m_volatility_precision_scan/down_arrived_vs_missed_summary.csv)

这两张表适合看：

- arrived 和 missed 两组
- 均值/中位数到底差了多少

### `up_fine_quantile_rates.csv` / `down_fine_quantile_rates.csv`

适合做：

- 更细的定量比较
- 自己筛某几个 quantile 看具体到达率

### `up_threshold_sweep.csv` / `down_threshold_sweep.csv`

适合看：

- 提高 cutoff 后
- 到达率到底抬升了多少

## 当前阶段的结论

当前这部分工作给出的结论，不是：

- “已经找到了很强的波动率因子”

而是：

- 波动率数值确实很小
- 差异主要体现在小数点后几位
- 即便放大到更细粒度，信号也仍然偏弱

所以更稳妥的结论是：

- **前 5 分钟波动率本身，不足以单独构成一个很强的筛选条件**

但这部分工作依然有价值，因为它已经把研究往前推进了一步：

- 从“到达率本身”  
- 推进到“为什么 EV 仍可能是负的”  
- 再推进到“能不能用前置信号筛出更好的样本”

这一步说明：

- 仅靠纯波动率，筛选能力有限
- 后续更值得尝试的是：
  - 波动率 + 方向性
  - 波动率 + 时段
  - 波动率 + 前一窗口状态

## 下一步建议

### 1. 做 uplift 分析

直接比较：

- 全样本到达率
- 前 10% 高波动率组到达率
- 前 5% 高波动率组到达率

这种方式会比相关系数更直观。

### 2. 叠加方向性变量

例如：

- `range_pct_5m` + `net_move_pct_5m`
- 高波动率且前 5 分钟单边上涨/下跌

这种组合通常比单独看波动率更可能出现信号。

### 3. 做分时段拆分

有可能：

- 整体信号不强
- 但某些固定时段信号更明显

可以继续按：

- UTC 小时
- 亚洲 / 欧洲 / 美盘

去拆开做。
