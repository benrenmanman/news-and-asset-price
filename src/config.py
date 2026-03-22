"""
config.py
全局配置，所有 API Key 从环境变量读取，不硬编码。

【切换 AI 服务商】
只需修改本文件末尾的 AI_CONFIG，或设置对应环境变量，无需改动 main.py。

支持任何兼容 OpenAI Chat Completions 接口的服务，包括：
  - OpenAI          (默认)
  - Azure OpenAI
  - DeepSeek
  - 国内中转/代理节点（如 api2d、302ai 等）
  - Ollama 本地部署（base_url 改为 http://localhost:11434/v1，api_key 填 "ollama"）
"""
import os
from zoneinfo import ZoneInfo

# ── 时区 ─────────────────────────────────────────────────────────────────────
UTC = ZoneInfo("UTC")
BJT = ZoneInfo("Asia/Shanghai")  # UTC+8

# ── 监控时间窗口（北京时间） ──────────────────────────────────────────────────
# 默认：前一日 20:00 → 当日 06:00（覆盖欧美主要交易时段）
OVERNIGHT_START_HOUR_BJT = 20
OVERNIGHT_END_HOUR_BJT   = 6

# ── 数据 API Keys ─────────────────────────────────────────────────────────────
POLYGON_API_KEY   = os.environ.get("POLYGON_API_KEY", "")
TWELVE_DATA_KEY   = os.environ.get("TWELVE_DATA_KEY", "")
WECOM_WEBHOOK_URL = os.environ.get("WECOM_WEBHOOK_URL", "")

# ── 数据库路径 ────────────────────────────────────────────────────────────────
DB_PATH = os.environ.get("DB_PATH", "data/overnight.db")

# ── 监控资产清单 ──────────────────────────────────────────────────────────────
# Polygon ticker 格式：股票/ETF 直接用 symbol；FX 用 C:EURUSD；商品用 X:GOLD
ASSETS = [
    {"id": "SPY",       "label": "S&P 500 ETF",  "source": "polygon", "ticker": "SPY"},
    {"id": "QQQ",       "label": "Nasdaq ETF",    "source": "polygon", "ticker": "QQQ"},
    {"id": "TLT",       "label": "美债20Y ETF",   "source": "polygon", "ticker": "TLT"},
    {"id": "GLD",       "label": "黄金ETF",       "source": "polygon", "ticker": "GLD"},
    {"id": "EURUSD",    "label": "EUR/USD",       "source": "polygon", "ticker": "C:EURUSD"},
    {"id": "DXY_PROXY", "label": "DXY近似(UUP)", "source": "polygon", "ticker": "UUP"},
]

# ── 价格异动检测参数 ───────────────────────────────────────────────────────────
PRICE_MOVE_WINDOW_MIN = 15   # 滚动窗口（分钟）
PRICE_SIGMA_THRESHOLD = 1.5  # 触发归因的标准差倍数

# ── 新闻对齐窗口 ──────────────────────────────────────────────────────────────
NEWS_PRE_WINDOW_MIN  = 5  # 价格异动前 N 分钟的新闻
NEWS_POST_WINDOW_MIN = 2  # 价格异动后 N 分钟的新闻（信息扩散滞后）

# ── Polygon 新闻采集参数 ───────────────────────────────────────────────────────
NEWS_FETCH_LIMIT = 1000  # 每次 API 请求最大条数（Polygon 上限 1000）
NEWS_CATEGORIES  = [
    "earnings", "dividends", "analyst_ratings",
    "mergers_acquisitions", "economic", "fx", "crypto",
]

# ── AI 服务配置 ───────────────────────────────────────────────────────────────
# 三个字段均优先读取环境变量，方便 GitHub Actions Secrets / Variables 注入；
# 未设置环境变量时回落到字典内的默认值。
#
# 快速切换方式（只改环境变量，不动代码）：
#   export AI_BASE_URL="https://api.deepseek.com/v1"
#   export AI_API_KEY="sk-..."
#   export AI_MODEL="deepseek-chat"
#
AI_CONFIG = {
    # ── 当前激活配置 ────────────────────────────────────────────────────────
    "base_url": os.environ.get("AI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
    "api_key":  os.environ.get("AI_API_KEY",  os.environ.get("OPENAI_API_KEY", "")),
    "model":    os.environ.get("AI_MODEL",    "qwen-plus"),

    # ── 其他服务商预设（取消注释即可切换，同时注释掉上方三行） ─────────────
    #
    # DeepSeek
    # "base_url": "https://api.deepseek.com/v1",
    # "api_key":  os.environ.get("AI_API_KEY", ""),
    # "model":    "deepseek-chat",
    #
    # Azure OpenAI
    # "base_url": (
    #     f"https://{os.environ.get('AZURE_RESOURCE', '')}.openai.azure.com"
    #     f"/openai/deployments/{os.environ.get('AZURE_DEPLOYMENT', '')}"
    # ),
    # "api_key":  os.environ.get("AZURE_OPENAI_KEY", ""),
    # "model":    os.environ.get("AI_MODEL", "gpt-4o"),
    #
    # 302ai / api2d 等国内中转
    # "base_url": "https://api.302.ai/v1",
    # "api_key":  os.environ.get("AI_API_KEY", ""),
    # "model":    "gpt-4o",
    #
    # Ollama 本地部署
    # "base_url": "http://localhost:11434/v1",
    # "api_key":  "ollama",
    # "model":    "qwen2.5:14b",
}
