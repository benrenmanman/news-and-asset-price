# 隔夜市场监控系统

欧美市场隔夜新闻与资产价格异动自动归因工具。

## 功能

- **新闻采集**：从 Polygon.io 采集欧美交易时段（北京时间 20:00–06:00）分钟级新闻流
- **价格采集**：同步采集 SPY/QQQ/TLT/GLD/EURUSD/DXY 分钟级 OHLCV K 线
- **时间戳对齐**：检测价格异动（±1.5σ 阈值），为每个异动匹配 ±5 分钟内的候选新闻
- **AI 归因**：调用 Claude API，输出主驱动新闻、逻辑链条、置信度
- **WeCom 推送**：每日北京时间 07:30 自动推送晨报

## 项目结构

```
overnight_monitor/
├── src/
│   ├── config.py          # 全局配置（资产清单、阈值参数）
│   ├── db.py              # SQLite 数据库初始化与操作
│   ├── news_collector.py  # Polygon.io 新闻采集
│   ├── price_collector.py # Polygon.io 分钟K线采集
│   ├── aligner.py         # 时间戳对齐引擎（核心）
│   └── main.py            # 主调度入口
├── data/                  # 运行时数据（SQLite、JSON快照、日志）
├── .github/workflows/
│   └── overnight_monitor.yml
└── requirements.txt
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 设置环境变量

```bash
export POLYGON_API_KEY="your_polygon_key"       # 必须，Starter $29/月
export ANTHROPIC_API_KEY="your_anthropic_key"   # 可选，用于 AI 归因
export WECOM_WEBHOOK_URL="https://qyapi..."     # 可选，用于 WeCom 推送
```

### 3. 本地运行

```bash
cd src
python main.py
```

### 4. 配置 GitHub Actions

在仓库 `Settings → Secrets and variables → Actions` 中添加：

**Secrets**（加密，用于 Key）：

| Secret 名称         | 说明                                     |
|--------------------|------------------------------------------|
| `POLYGON_API_KEY`  | Polygon/Massive API Key（必须）           |
| `AI_API_KEY`       | AI 服务 Key，对应 config.AI_CONFIG（可选）|
| `WECOM_WEBHOOK_URL`| 企业微信 Webhook URL（可选）              |

**Variables**（明文，用于非敏感配置）：

| Variable 名称  | 默认值                          | 说明              |
|--------------|--------------------------------|-------------------|
| `AI_BASE_URL` | `https://api.openai.com/v1`   | AI 服务 base URL  |
| `AI_MODEL`    | `gpt-4o`                       | 使用的模型名称    |

> 切换到 DeepSeek 示例：`AI_BASE_URL=https://api.deepseek.com/v1`，`AI_MODEL=deepseek-chat`，`AI_API_KEY` 填 DeepSeek Key。

## 监控资产

默认监控以下资产（可在 `config.py` 中修改）：

| ID          | 说明           | Polygon Ticker |
|-------------|---------------|----------------|
| SPY         | S&P 500 ETF   | SPY            |
| QQQ         | Nasdaq ETF    | QQQ            |
| TLT         | 美债20Y ETF   | TLT            |
| GLD         | 黄金 ETF      | GLD            |
| EURUSD      | EUR/USD       | C:EURUSD       |
| DXY_PROXY   | DXY近似(UUP)  | UUP            |

## 关键参数

```python
# config.py
PRICE_MOVE_WINDOW_MIN  = 15    # 滚动检测窗口（分钟）
PRICE_SIGMA_THRESHOLD  = 1.5   # 触发阈值（标准差倍数）
NEWS_PRE_WINDOW_MIN    = 5     # 新闻前置窗口（分钟）
NEWS_POST_WINDOW_MIN   = 2     # 新闻后置窗口（分钟，信息扩散滞后）
```

## 数据库表结构

```
news          → 新闻条目（id, published_ts, title, tickers, ...）
price_bars    → 分钟K线（asset_id, bar_ts, open, high, low, close, ...）
price_events  → 价格异动事件（asset_id, event_ts, sigma_multiple, ...）
alignments    → 新闻-价格配对（event_id, news_id, time_delta_sec, ai_attribution, ...）
```

## 输出示例

```
▶ SPY  ↓ -0.84%  (1.9σ)  @ 2025-10-15T02:15:00 UTC
   [  -3.0min] [Reuters      ] Fed official signals rate pause may extend through Q1
   [  -1.0min] [Bloomberg    ] US 10Y yield hits 4.85%, highest since 2007
   [  +0.5min] [WSJ          ] Markets price in hawkish Fed pivot after inflation surprise

归因[高]: Fed official signals rate pause may extend through Q1
逻辑: 美联储官员暗示暂停加息时间延长，触发市场对利率长期偏高的重定价，
      美债收益率上行拖累权益估值，SPY下行符合利率风险逻辑。
```

## 已知限制

1. **Polygon Starter 限速**：15 req/min，完整采集约需 5–10 分钟
2. **FX 数据**：Polygon 提供 EUR/USD 等主要货币对，但 DXY 指数本身无直接 ticker，
   以 UUP ETF 近似；如需精确 DXY，需接入 Refinitiv 或 Bloomberg
3. **新闻覆盖**：Polygon 新闻以英文财经媒体为主（Reuters、Bloomberg、WSJ），
   中文媒体及 CCTV/新华社暂未覆盖
4. **数据库持久化**：GitHub Actions 使用 cache 持久化 SQLite，存在缓存失效风险，
   生产环境建议迁移至 TimescaleDB 或 Supabase
