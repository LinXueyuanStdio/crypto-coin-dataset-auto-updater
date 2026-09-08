# Known Issues & 数据源坑

记录 Binance USDT-M 永续数据抓取/校验过程中踩过的坑，避免后续重复踩。

---

## 1. funding 检查 `calc_time not in index`

**现象**：`futures_validate_data.py` 对所有 funding 文件统一报错
`"['calc_time'] not in index`，导致 527 个币的 funding 完整性检查完全失效。

**根因**：`missing_ranges(frame, time_col, required_columns, ...)` 里先
`work.set_index(time_col)` 把 `time_col` 变成索引，再用 `aligned[required_columns]`
取列——而 `FUNDING_COLUMNS` 包含 `calc_time`（即 `time_col`），此时它已不在列里。

**修复**：在 `missing_ranges` 内部剔除 `time_col`（`value_cols = [c for c in required_columns if c != time_col]`），
做防御性处理。同类常量 `KLINE_REQUIRED_COLUMNS` / `METRICS_NUMERIC_COLUMNS` 本就不含时间列，所以只有 funding 触发。

**教训**：传给 `missing_ranges` 的 `required_columns` 必须是**纯数值列**，不能包含时间列。

---

## 2. funding interval 会动态变化，不能用单一众数网格

**现象**：`infer_funding_interval_hours` 用众数推断单一间隔（如 4h），对全程套固定网格检测 gap，
把 interval 切换前的整段误判成缺口（0GUSDT 误报 1267 个）。

**事实**：币安 funding interval 会**非单调变化**（8h→4h→1h→4h），随市场波动动态调整。
`funding_interval_hours` 是「向前看」的（当前点 → 下一点的期望间隔）。

**修复**：`funding_missing_ranges` 改用**局部异常检测**——相邻 `calc_time` 间隔明显大于
左右邻居（`max(neighbors) * 1.5`）才算缺口。这能同时：
- 正确处理切换（切换点相邻差是标准值，不误报）
- 正确报缺一个点（4h 段缺 04:00 → 相邻差 8h > 4h*1.5）

**教训**：不能用 `min(neighbors)`（切换点会取到新段的 1h 误判 4h 间隔）；用 `max(neighbors)`。

---

## 3. `funding_interval_hours` 字段抓取时被写错

**现象**：`funding_interval_hours` 字段对 interval 变化过的币全是错的
（403 个币、9034 条记录，如 API3 早期 8h 段被标成 4h）。

**根因**：`fetch_funding_range` 两处错误——
1. 抓取时硬编码 `"funding_interval_hours": 8`（币安 `/fapi/v1/fundingRate` **不返回 interval 字段**）
2. 抓取后又 `infer_funding_interval_hours` 取众数**统一覆盖**全部记录，抹掉 8h→4h→1h 的切换

**修复**：新增 `fill_funding_interval_hours`，按相邻 `calc_time` 间隔归一化到标准值
`{1,2,4,8}`，`>8.5h` 视为数据缺口置空并前向回填（避免把缺口时长误当 interval）。

**教训**：币安 fundingRate 接口只返回 `fundingTime` + `fundingRate`，**没有 interval 字段**，
interval 只能从时间戳差推断，且必须逐条推断、不能取众数统一。

---

## 4. kline 接口保留期其实很长（"超保留期"是误判）

**现象**：曾错误地把 14387 个 kline gap 归为「超保留期无法在线补」。

**事实**：只有 metrics（`/futures/data/*`）真正只保留 **30 天**。kline 类接口
（`/fapi/v1/klines`、`indexPriceKlines`、`markPriceKlines`、`premiumIndexKlines`）
保留期很长（能查 2020-2023 的历史）。实测 ALGOUSDT 2022 年能返回 13 万行。

**修复**：重新修复历史 kline gap，88.2% 可在线补（14387 → 1696）。

**教训**：判断「能否在线补」必须实测接口，不能臆断保留期。

---

## 5. `onboardDate` 对「下架后重新上线」的币不可靠

**现象**：11 个币的 `onboardDate` 晚于 kline 起始数年（CVCUSDT onboard=2025-05-16，但 kline 从 2020-11-11 连续）。

**事实**：这些币经历了**下架 → 重新上线**，币安 API 的 `onboardDate` 返回的是
「最近一次上线日期」而非「首次上线日期」。例如：
- CVXUSDT：2022-09 首上 → 2025-05 下架 → 2025-07-23 重新上线
- SLPUSDT：2023-10 首上 → 2025-07-23 重新上线

**数据表现**：下架期在数据里是
- kline：`volume=0`、价格冻结（零成交占位），**不是断档**
- funding：真实缺失（合约下架无资金费率结算）

**结论**：下架期**不算数据缺失**。当前 validate 用 onboardDate 过滤「上市前空档」，
对重新上线的币恰好把旧合约期 + 下架期都排除，语义正确，**无需修复 onboardDate**。

**教训**：不要用 `onboardDate` 推断「首次上线日期」；对重新上线的币，onboardDate 是「当前合约」的上线日期。

---

## 6. 指数/标记价格中断是全局性的（不只 meme/小币）

**现象**：剩余 1696 个不可补的 kline gap，之前误判为「meme/小币 indexPrice 当日中断」。

**事实**：这些 gap 是**币安全局中断**——同一时刻影响所有在线币。集中在固定日期：
- `2023-11-10 03:45`（indexPrice/markPrice）
- `2022-07-12 13:15~14:25`（markPrice/premiumIndex）
- `2020-12-01 22:40~23:55`（premiumIndex）
- `2020-12-17 07:35~07:50`（indexPrice/markPrice/premiumIndex）

ATOM/AAVE/ADA/ALGO 等主流币的 gap 列表几乎一模一样。币安源头就没有这些数据，
线上接口返回空，不可补。

**处理**：进忽略清单（`output/futures_ignore_continuity.json`），reason 标注全局中断。

---

## 7. `requiredMarginPercent` / `maintMarginPercent` 是静态默认值，不能算杠杆

**现象**：info.json 里 `requiredMarginPercent` 几乎全是 `"5.0000"`（→ 20x），
`maintMarginPercent` 全是 `"2.5000"`。但币安后来把 SCRUSDT 最高杠杆从 20x 降到 10x，
这个字段没变，策略据此算 `max_lev = 100/5 = 20x` 会高估杠杆。

**根因**：这两个字段来自 `/fapi/v1/exchangeInfo`，而币安在这个接口里对几乎所有
USDT-M 永续都**硬编码静态默认值**（`requiredMarginPercent="5.0000"`、
`maintMarginPercent="2.5000"`），**从不随杠杆调整更新**。实测 527 个 TRADING 永续里
473 个（约 90%）用 `100/requiredMarginPercent` 推算的杠杆与真实杠杆不符：
BTC/ETH 真实 150x、XRP 100x、BCH/LTC/ADA 等 75x、SCR 10x，但 info.json 全是 20x。

**事实**：真实杠杆在 `/fapi/v1/leverageBracket`（需 API key + HMAC 签名），且是
**逐仓分层**的 brackets（如 SCR：10x/5x/4x/3x/2x/1x 六档），不是单一数字。维持保证金率
（`maintMarginRatio`）同样分层，exchangeInfo 的 `maintMarginPercent` 也是静态默认值。

**修复**：`futures_updater.py` 新增 `fetch_leverage_brackets()`，签名调用
`leverageBracket` 并在 master batch 里把结果写进每个 symbol 的 info.json：
- `maxLeverage`：第一档 `initialLeverage`（最高杠杆）
- `leverageBrackets`：完整分层数组（含 `initialLeverage` / `maintMarginRatio`）

旧的 `requiredMarginPercent` / `maintMarginPercent` 字段保留（兼容），但**不可靠**，
策略层应改用 `maxLeverage`（或从 `leverageBrackets` 取分层保证金率）。

**教训**：exchangeInfo 的 `requiredMarginPercent` / `maintMarginPercent` 是静态默认值，
和 `onboardDate`（#5）一样属于「快照/默认值」而非实时数据；真实的杠杆/保证金率
要用签名接口 `leverageBracket` 获取。

---

## 8. funding 数值列 string 类型残留导致 merge 冲突

**现象**：`GRVTUSDT_fundingRate` 抓取失败，报
`"Expected bytes, got a 'int' object" / Conversion failed for column funding_interval_hours with type object`。

**根因**：磁盘上旧的 funding parquet 里 `funding_interval_hours`（及 `last_funding_rate`）
是 string（历史遗留，所有数值列曾用 `dtype=str` 读入并落盘）。后来抓取改成了数值
（`numeric_cols`），但**只归一化了新抓的数据**，没迁移磁盘上已有的旧列。
`merge_frames` 里 `pd.concat(existing(string), new(int))` 混成 object，`to_parquet`
时 pyarrow 推断 schema 失败。只有个别「迁移时还没上线」的新币（如 GRVTUSDT）触发，
因为它们的 funding 文件全程是 string。

**修复**：`merge_frames` 增加 `numeric_cols` 参数，对数值列做 `pd.to_numeric` 归一化
（与时间列归一化同套做法）；`process_job` 传入 `dt.numeric_cols`。merge 前两侧都转数值，
concat 后类型一致，落盘即修复。

**教训**：磁盘旧数据与在线抓取的类型一致性要在 **merge 边界**统一归一化，
不能只靠上游抓取侧修正——历史遗留的 string 列在 merge 时仍需显式 `to_numeric`。

---

## 附：忽略清单机制

`output/futures_ignore_continuity.json` 记录所有「已知不可在线补」的 gap（带 reason）。
`futures_validate_data.py --validate=continuity --ignore-file ...` 读取后跳过，不再视为缺失。

- metrics 历史缺失 → 30 天保留期
- funding 缺失 → 2026-06-24 全局中断 + 下架期
- kline 缺失 → 币安全局中断日
