# 休市信息文件 `futures_halt_periods.json`

本文件记录**不可交易时段**（休市、下架冻结期、交易所维护/宕机导致的成交 K 线缺失），
供量化回测引擎在逐 bar 模拟时跳过这些时段，避免在「根本没有成交、无法下单」的时间点上
凭空成交，从而产生失真的回测结果。

---

## 一、为什么需要它

「数据完整」不等于「数据可交易」。两类时段在数据里看起来正常、实则不可交易：

| 现象 | 数据表现 | 回测风险 |
|---|---|---|
| **下架冻结期** | `volume=0` 且价格冻结（`high==low`），但**有占位记录、不是断档** | 在冻结价上反复成交，虚增交易次数 |
| **成交 K 线缺失** | 完全没有记录（gap） | 无数据却被线性插值/前向填充，凭空造出可交易的价格 |

这两类时段无法用「完整性」检查识别（它们不是重复、不是 NaN、不是缺文件），
必须单独检测并交给回测引擎显式跳过。

---

## 二、生成

```bash
# 生成休市文件（读取 data/ 下每个 symbol 的 1d K 线检测冻结期，
# 并从忽略清单提取成交 K 线缺口）
python futures_validate_data.py --validate=halt

# 只生成指定 symbol
python futures_validate_data.py --validate=halt --symbols BTCUSDT,ETHUSDT

# 调整下架冻结期的最小天数阈值（默认 2 天，过滤单日偶然零成交的噪声）
python futures_validate_data.py --validate=halt --min-days 2
```

输出文件：`output/futures_halt_periods.json`。

> **依赖说明**：`missing`（维护期）类型从忽略清单 `output/futures_ignore_continuity.json`
> 提取成交 K 线（`ohlcv` kind）的 gap。若该清单不存在或为空，`missing` 时段为空，
> `frozen`（下架冻结期）不受影响。建议先跑一次
> `python futures_validate_data.py --validate=continuity` 再 `--gen-ignore`，
> 得到完整的忽略清单后重新生成休市文件。

---

## 三、文件格式

```json
{
  "version": 1,
  "generated_at": "2026-08-20T12:00:00+00:00",
  "description": "休市/不可交易时段清单，供回测引擎跳过。……",
  "data_dir": "/abs/path/to/data",
  "symbols_filter": null,
  "periods": [
    {
      "symbol": "CVCUSDT",
      "type": "frozen",
      "start": "2022-11-30T00:00:00",
      "end": "2025-05-15T00:00:00",
      "reason": "下架冻结期（连续零成交、价格冻结）"
    },
    {
      "symbol": "ICPUSDT",
      "type": "missing",
      "start": "2022-09-01T00:00:00",
      "end": "2022-09-26T12:00:00",
      "reason": "成交 K 线缺失（交易所维护/宕机）"
    }
  ],
  "summary": {
    "frozen_period_count": 1,
    "missing_period_count": 1,
    "total_period_count": 2,
    "symbols_with_frozen": 1,
    "symbols_with_missing": 1,
    "min_days": 2
  }
}
```

### 字段说明

| 字段 | 说明 |
|---|---|
| `periods` | 扁平时段列表，每条对应一个 symbol 的一段休市期 |
| `type` | 时段类型：`frozen`（下架冻结期）或 `missing`（成交 K 线缺失） |
| `start` / `end` | ISO 时间字符串，**闭区间**（见下） |
| `reason` | 人类可读的原因说明 |
| `summary` | 计数汇总，便于快速核对 |

### 区间语义

`start` 和 `end` 构成**闭区间 `[start, end]`**，两端都视为不可交易。
回测引擎判断规则：某根 bar 的 `open_time` 落在 `[start, end]` 内即视为休市，跳过该 bar。

- `frozen`：`start`/`end` 均为冻结日的 `open_time`（1d 粒度，`end` 是最后一个冻结日）。
- `missing`：沿用 gap 检测的区间（含缺失起点与终点）。

> 注意：`periods` 里同一 symbol 可能有多条**重叠**时段（例如 ICPUSDT 不同周期的
> K 线缺口被并排记录）。回测引擎无需做区间合并——逐条判断即可，重叠不影响正确性。

---

## 四、回测引擎使用方法

### 1. 加载 + 查询

```python
import json
from pathlib import Path

import pandas as pd


def load_halt_periods(path="output/futures_halt_periods.json"):
    """加载休市文件，返回 {symbol: [(start_ts, end_ts), ...]}。"""
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    by_symbol = {}
    for p in data["periods"]:
        by_symbol.setdefault(p["symbol"], []).append(
            (pd.Timestamp(p["start"]), pd.Timestamp(p["end"]))
        )
    return by_symbol


HALT = load_halt_periods()


def is_halted(symbol, bar_time):
    """判断某 symbol 的某根 bar 是否处于休市/不可交易时段。

    闭区间 [start, end]，两端都视为不可交易。
    """
    for start, end in HALT.get(symbol, []):
        if start <= bar_time <= end:
            return True
    return False
```

### 2. 在回测循环中跳过

```python
for bar in bars(symbol):           # bars() 依次产出各周期 K 线
    if is_halted(symbol, bar["open_time"]):
        continue                   # 休市：不平仓、不开仓、不结算，直接跳过
    # ... 正常信号生成、成交撮合、资金费结算 ...
```

### 3. 与其它数据就绪检查的组合

休市文件只解决「**能不能交易**」，与「数据对不对」是两回事。完整回测就绪通常还需：

| 维度 | 来源 | 作用 |
|---|---|---|
| 完整性 / 重复 / 连续性 | `--validate=continuity` 等 | 数据本身是否完整 |
| **异常值 / 尖峰** | `--validate=outliers` | 闪崩、插针等极端 bar，决定是否过滤 |
| **休市 / 不可交易** | `--validate=halt` | 跳过冻结期与缺失期 |
| 前视偏差 | 回测框架自身 | 只用「当时可得」的数据，不提前泄露 |

---

## 五、注意事项与局限

1. **`frozen` 的判定阈值**：默认要求连续 `volume==0` 且 `high==low` **≥ 2 天**才判定为
   下架冻结期（`--min-days` 可调）。单日偶然零成交的小币不会被误报，但也意味着
   **短于 2 天的下架/停牌不会被捕捉**。

2. **只覆盖成交 K 线**：`missing` 类型仅从忽略清单提取 `ohlcv`（成交 K 线）的缺口，
   因为只有成交 K 线缺失才意味着「无法交易」。`indexPrice`/`markPrice`/`premiumIndex`
   的中断（指数/标记价格当日全局停发）**不包含在内**——它们不影响成交 K 线回测，
   但若你的策略依赖标记价/指数价（如计算基差、强平），需另行处理这些中断。

3. **`onboardDate` 陷阱**：下架后重新上线的币，其 `onboardDate` 是「重新上线日」而非
   「首次上线日」。冻结期检测**不依赖** `onboardDate`，而是用「首个真实成交之后的
   零成交段」来判定，因此能正确识别这些币的下架期（详见 `docs/known-issues.md` #5）。

4. **上市前占位**：数据起点的零成交段（上市前的指数价格占位）**不会**被记为休市——
   回测引擎应自行用 `info.json` 的 `onboardDate` 确定每个 symbol 的上市起点。

5. **文件为快照**：`generated_at` 记录生成时刻。数据更新后（新币上线、旧币下架、
   补了历史缺口），应重新运行 `--validate=halt` 刷新，避免用过期的休市清单。
