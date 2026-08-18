#!/usr/bin/env python
"""从忽略清单生成「缺失且无法补充」的 HTML 审计报告。"""
import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

IGNORE = Path("output/futures_ignore_continuity.json")
OUT = Path("output/futures_gap_report.html")

KIND_LABEL = {
    "metrics": "metrics",
    "indexPrice": "指数价格",
    "markPrice": "标记价格",
    "premiumIndex": "溢价指数",
    "ohlcv": "K线 OHLCV",
}
KIND_COLOR = {
    "metrics": "#5eb6d9",
    "indexPrice": "#a78bea",
    "markPrice": "#6b96e8",
    "premiumIndex": "#52c4a6",
    "ohlcv": "#e38a5c",
}


def load_data():
    data = json.loads(IGNORE.read_text(encoding="utf-8"))
    entries = data["entries"]
    total = sum(len(e["gaps"]) for e in entries)

    by_kind = Counter()
    by_symbol = defaultdict(Counter)
    kline_year = Counter()
    metrics_year = Counter()
    for e in entries:
        kind = e["kind"]
        n = len(e["gaps"])
        by_kind[kind] += n
        by_symbol[e["symbol"]][kind] += n
        for g in e["gaps"]:
            y = pd.Timestamp(g["start"]).year
            if kind == "metrics":
                metrics_year[y] += 1
            else:
                kline_year[y] += 1

    symbols = [
        {"symbol": s, "gaps": sum(k.values()), "kinds": dict(k)}
        for s, k in by_symbol.items()
    ]
    symbols.sort(key=lambda x: -x["gaps"])

    return {
        "total": total,
        "entry_count": len(entries),
        "symbol_count": len(symbols),
        "kline_total": sum(kline_year.values()),
        "metrics_total": sum(metrics_year.values()),
        "by_kind": dict(by_kind),
        "kline_year": dict(sorted(kline_year.items())),
        "metrics_year": dict(sorted(metrics_year.items())),
        "symbols": symbols,
    }


HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>缺失数据审计</title>
<style>
:root{
  --bg:#0c0f14; --surface:#141922; --surface2:#1b2230;
  --text:#e7eaf0; --muted:#8a93a5; --faint:#5a6472;
  --border:#232b3a; --accent:#e0a458; --accent-dim:#8f6a35;
  --metrics:#5eb6d9; --index:#a78bea; --mark:#6b96e8; --premium:#52c4a6; --ohlcv:#e38a5c;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme="light"]){}
}
:root[data-theme="light"]{
  --bg:#f6f7f9; --surface:#ffffff; --surface2:#eef1f5;
  --text:#1a1e26; --muted:#5a6472; --faint:#8a93a5;
  --border:#e2e6ec; --accent:#b8791f; --accent-dim:#d9a441;
  --metrics:#2d7fa0; --index:#7c5fc4; --mark:#3f6fbf; --premium:#2d8f78; --ohlcv:#c0652f;
}
@media (prefers-color-scheme: light){
  :root:not([data-theme="dark"]){
    --bg:#f6f7f9; --surface:#ffffff; --surface2:#eef1f5;
    --text:#1a1e26; --muted:#5a6472; --faint:#8a93a5;
    --border:#e2e6ec; --accent:#b8791f; --accent-dim:#d9a441;
    --metrics:#2d7fa0; --index:#7c5fc4; --mark:#3f6fbf; --premium:#2d8f78; --ohlcv:#c0652f;
  }
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{
  margin:0; background:var(--bg); color:var(--text);
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,"PingFang SC","Microsoft YaHei",sans-serif;
  line-height:1.6; -webkit-font-smoothing:antialiased;
}
.num{font-family:ui-monospace,"SF Mono","Cascadia Mono",Consolas,Menlo,monospace;font-variant-numeric:tabular-nums}
.wrap{max-width:1080px;margin:0 auto;padding:48px 28px 96px}

/* header */
header{border-bottom:1px solid var(--border);padding-bottom:36px;margin-bottom:40px}
.eyebrow{font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:var(--accent);font-weight:600;margin-bottom:14px}
h1{font-size:40px;line-height:1.12;margin:0 0 12px;font-weight:700;letter-spacing:-.02em;text-wrap:balance}
.lede{font-size:17px;color:var(--muted);max-width:62ch;margin:0}

/* metric cards */
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:44px}
.card{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:20px 18px}
.card .v{font-size:32px;font-weight:700;letter-spacing:-.02em;line-height:1}
.card .v small{font-size:15px;font-weight:500;color:var(--muted)}
.card .l{font-size:12px;color:var(--muted);letter-spacing:.05em;margin-top:8px;text-transform:uppercase}
.card.primary{border-color:var(--accent-dim)}
.card.primary .v{color:var(--accent)}

/* sections */
section{margin-bottom:48px}
h2{font-size:20px;font-weight:650;letter-spacing:-.01em;margin:0 0 6px;display:flex;align-items:baseline;gap:10px}
h2 .tag{font-size:12px;font-weight:600;color:var(--muted);letter-spacing:.04em}
.sub{color:var(--muted);font-size:14px;margin:0 0 20px;max-width:72ch}

/* reason blocks */
.reasons{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.reason{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:22px}
.reason .top{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:10px}
.reason .name{font-weight:650;font-size:15px}
.reason .cnt{font-size:26px;font-weight:700}
.reason p{color:var(--muted);font-size:13.5px;margin:0}
.reason.metrics .cnt{color:var(--metrics)}
.reason.kline .cnt{color:var(--accent)}

/* bars */
.bars{display:flex;flex-direction:column;gap:9px}
.bar-row{display:grid;grid-template-columns:150px 1fr 84px;align-items:center;gap:14px}
.bar-row .lab{font-size:13px;color:var(--text);display:flex;align-items:center;gap:8px}
.bar-row .lab .dot{width:9px;height:9px;border-radius:50%;flex:0 0 auto}
.bar-row .track{background:var(--surface2);border-radius:5px;height:14px;overflow:hidden}
.bar-row .fill{height:100%;border-radius:5px;min-width:2px}
.bar-row .val{font-size:13px;text-align:right;color:var(--muted)}

/* year split */
.years{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.year-panel{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:22px}
.year-panel h3{font-size:14px;margin:0 0 16px;font-weight:650;color:var(--muted)}
.year-panel.metrics h3{color:var(--metrics)}
.year-panel.kline h3{color:var(--accent)}

/* table */
.table-wrap{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden}
table{width:100%;border-collapse:collapse;font-size:13.5px}
thead th{position:sticky;top:0;background:var(--surface2);color:var(--muted);font-weight:600;text-align:left;padding:12px 16px;font-size:12px;letter-spacing:.04em;text-transform:uppercase;border-bottom:1px solid var(--border);cursor:pointer;user-select:none;white-space:nowrap}
thead th:hover{color:var(--text)}
thead th.sorted{color:var(--accent)}
tbody td{padding:10px 16px;border-bottom:1px solid var(--border);white-space:nowrap}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:var(--surface2)}
td.rank{color:var(--faint)}
td.sym{font-weight:600}
td.gap{text-align:right}
.kindcell{display:inline-flex;gap:4px}
.kchip{font-size:11px;padding:1px 7px;border-radius:20px;font-weight:600}
.controls{padding:14px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.controls input{background:var(--surface2);border:1px solid var(--border);border-radius:8px;color:var(--text);padding:7px 12px;font-size:13px;min-width:220px}
.controls input:focus{outline:none;border-color:var(--accent)}
.controls .note{font-size:12px;color:var(--faint);margin-left:auto}
a,button{color:var(--accent)}
@media (max-width:760px){
  .grid{grid-template-columns:repeat(2,1fr)}
  .reasons,.years{grid-template-columns:1fr}
  .bar-row{grid-template-columns:118px 1fr 64px}
  h1{font-size:30px}
}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <div class="eyebrow">Binance USDT-M 永续期货 · 数据完整性审计</div>
    <h1>缺失数据审计</h1>
    <p class="lede">这些历史缺口无法通过在线接口补充——metrics 接口仅保留 30 天，价格类 K 线超出接口保留期。它们已记录到忽略清单，validate 不再将其视为缺失。</p>
  </header>

  <section>
    <div class="grid">
      <div class="card primary"><div class="v num" id="total">—</div><div class="l">总缺口</div></div>
      <div class="card"><div class="v num" id="metrics">—</div><div class="l">metrics 缺口</div></div>
      <div class="card"><div class="v num" id="kline">—</div><div class="l">K线缺口</div></div>
      <div class="card"><div class="v num" id="syms">—</div><div class="l">涉及交易对</div></div>
    </div>
  </section>

  <section>
    <h2>为什么无法补充<span class="tag">两类原因</span></h2>
    <p class="sub">缺口本身是真实的历史缺失，但两类数据源的在线接口都已查不到对应记录，因此无法通过补拉修复。</p>
    <div class="reasons">
      <div class="reason metrics">
        <div class="top"><span class="name">metrics · 持仓量 / 多空比</span><span class="cnt num" id="rc-metrics">—</span></div>
        <p>币安 <code>/futures/data/*</code> 接口只保留最近 30 天。30 天前的 openInterest、多空比、taker 买卖比等无法再查询。</p>
      </div>
      <div class="reason kline">
        <div class="top"><span class="name">K线 · 指数 / 标记 / 溢价</span><span class="cnt num" id="rc-kline">—</span></div>
        <p>indexPrice / markPrice / premiumIndex 接口的历史保留期有限，2020–2023 年及 2026 上半年的缺口已超出可查范围。</p>
      </div>
    </div>
  </section>

  <section>
    <h2>按数据类型分布<span class="tag">缺口数量</span></h2>
    <div class="bars" id="kind-bars"></div>
  </section>

  <section>
    <h2>按年份分布<span class="tag">缺口起始时间</span></h2>
    <div class="years">
      <div class="year-panel metrics"><h3>metrics 缺口按年</h3><div class="bars" id="metrics-year-bars"></div></div>
      <div class="year-panel kline"><h3>K线缺口按年</h3><div class="bars" id="kline-year-bars"></div></div>
    </div>
  </section>

  <section>
    <h2>缺口最多的交易对<span class="tag">完整清单可排序</span></h2>
    <p class="sub">下表列出全部 <span class="num" id="sym-count">—</span> 个有缺口的交易对，点击列头排序，输入框过滤 symbol。</p>
    <div class="table-wrap">
      <div class="controls">
        <input id="filter" type="text" placeholder="过滤 symbol，如 USDC">
        <span class="note">显示 <span class="num" id="shown">—</span> / <span class="num" id="total-rows">—</span></span>
      </div>
      <div style="overflow-x:auto;max-height:560px;overflow-y:auto">
        <table>
          <thead>
            <tr>
              <th data-k="rank">#</th>
              <th data-k="symbol">交易对</th>
              <th data-k="metrics">metrics</th>
              <th data-k="indexPrice">指数价格</th>
              <th data-k="markPrice">标记价格</th>
              <th data-k="premiumIndex">溢价指数</th>
              <th data-k="ohlcv">K线</th>
              <th data-k="gaps" class="sorted">总缺口</th>
            </tr>
          </thead>
          <tbody id="tbody"></tbody>
        </table>
      </div>
    </div>
  </section>

  <footer style="margin-top:56px;color:var(--faint);font-size:12px">
    由 <span class="num">--gen-ignore</span> 生成的忽略清单分析 · 数据快照 <span class="num" id="gen-at">—</span>
  </footer>
</div>

<script>
const DATA = __DATA__;
const $ = s => document.querySelector(s);
const fmt = n => n.toLocaleString('en-US');

function barRow(label, color, value, max){
  const pct = max > 0 ? (value / max * 100) : 0;
  return `<div class="bar-row">
    <span class="lab"><span class="dot" style="background:${color}"></span>${label}</span>
    <span class="track"><span class="fill" style="width:${pct}%;background:${color}"></span></span>
    <span class="val num">${fmt(value)}</span>
  </div>`;
}

function renderTop(){
  $('#total').textContent = fmt(DATA.total);
  $('#metrics').innerHTML = fmt(DATA.metrics_total) + ' <small>(' + (DATA.metrics_total/DATA.total*100).toFixed(0) + '%)</small>';
  $('#kline').innerHTML = fmt(DATA.kline_total) + ' <small>(' + (DATA.kline_total/DATA.total*100).toFixed(0) + '%)</small>';
  $('#syms').textContent = fmt(DATA.symbol_count);
  $('#rc-metrics').textContent = fmt(DATA.metrics_total);
  $('#rc-kline').textContent = fmt(DATA.kline_total);
  $('#sym-count').textContent = fmt(DATA.symbol_count);
  $('#gen-at').textContent = DATA.generated_at.slice(0, 16).replace('T', ' ');
}

function renderKind(){
  const kinds = [['metrics','metrics',DATA.by_kind.metrics],['indexPrice','指数价格',DATA.by_kind.indexPrice],
    ['markPrice','标记价格',DATA.by_kind.markPrice],['premiumIndex','溢价指数',DATA.by_kind.premiumIndex],
    ['ohlcv','K线 OHLCV',DATA.by_kind.ohlcv]];
  const max = Math.max(...kinds.map(k=>k[2]));
  $('#kind-bars').innerHTML = kinds.map(k=>barRow(k[1], KIND_COLOR[k[0]], k[2], max)).join('');
}

function renderYears(){
  const mk = Object.entries(DATA.metrics_year); const ky = Object.entries(DATA.kline_year);
  const mkm = Math.max(...mk.map(x=>x[1])); const kym = Math.max(...ky.map(x=>x[1]));
  $('#metrics-year-bars').innerHTML = mk.map(([y,v])=>barRow(y, KIND_COLOR.metrics, v, mkm)).join('');
  $('#kline-year-bars').innerHTML = ky.map(([y,v])=>barRow(y, KIND_COLOR.ohlcv, v, kym)).join('');
}

const KIND_COLOR = DATA.kind_colors;
const KIND_SHORT = {metrics:'metrics', indexPrice:'指数', markPrice:'标记', premiumIndex:'溢价', ohlcv:'K线'};
let rows = DATA.symbols;
let sortK = 'gaps', sortAsc = false;

function renderTable(){
  const q = $('#filter').value.trim().toUpperCase();
  const shown = rows.filter(r => !q || r.symbol.includes(q));
  $('#shown').textContent = fmt(shown.length);
  $('#total-rows').textContent = fmt(rows.length);
  $('#tbody').innerHTML = shown.map((r,i)=>{
    const cells = ['metrics','indexPrice','markPrice','premiumIndex','ohlcv'].map(k=>{
      const v = r.kinds[k] || 0;
      return v ? `<span class="kchip" style="background:${KIND_COLOR[k]}22;color:${KIND_COLOR[k]}">${fmt(v)}</span>` : '<span class="num" style="color:var(--faint)">·</span>';
    });
    return `<tr>
      <td class="rank num">${i+1}</td>
      <td class="sym">${r.symbol}</td>
      ${cells.map(c=>`<td class="gap">${c}</td>`).join('')}
      <td class="gap num" style="font-weight:650">${fmt(r.gaps)}</td>
    </tr>`;
  }).join('');
}

document.querySelectorAll('thead th').forEach(th=>{
  th.addEventListener('click', ()=>{
    const k = th.dataset.k;
    if(k === 'rank') return;
    if(sortK === k){ sortAsc = !sortAsc; } else { sortK = k; sortAsc = false; }
    document.querySelectorAll('thead th').forEach(x=>x.classList.remove('sorted'));
    th.classList.add('sorted');
    rows.sort((a,b)=>{
      const av = k === 'symbol' ? a.symbol : (k === 'gaps' ? a.gaps : (a.kinds[k]||0));
      const bv = k === 'symbol' ? b.symbol : (k === 'gaps' ? b.gaps : (b.kinds[k]||0));
      if(typeof av === 'string') return sortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
      return sortAsc ? av - bv : bv - av;
    });
    renderTable();
  });
});
$('#filter').addEventListener('input', renderTable);

renderTop(); renderKind(); renderYears(); renderTable();
</script>
</body>
</html>
"""


def main():
    d = load_data()
    d["generated_at"] = json.loads(IGNORE.read_text(encoding="utf-8")).get("generated_at", "")
    d["kind_colors"] = KIND_COLOR
    html = HTML.replace("__DATA__", json.dumps(d, ensure_ascii=False))
    OUT.write_text(html, encoding="utf-8")
    print(f"生成 {OUT} ({OUT.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
