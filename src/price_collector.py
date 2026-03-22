"""
price_collector.py
从 Polygon.io /v2/aggs/ticker/{ticker}/range/1/minute/{from}/{to}
采集分钟级 OHLCV K 线，写入 SQLite。

设计决策：使用 REST（而非 WebSocket），适合 GitHub Actions 无状态批量拉取。
FX 行情同样走 Polygon（C:EURUSD 等），不需要额外 API。
"""
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from config import POLYGON_API_KEY, ASSETS, DB_PATH
from db import get_conn, transaction, init_db

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

POLYGON_AGGS_URL   = "https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{from_date}/{to_date}"
MIN_REQUEST_INTERVAL = 5.0   # 12 req/min 安全边际


class PriceCollector:
    def __init__(self, api_key: str = POLYGON_API_KEY):
        if not api_key:
            raise ValueError("POLYGON_API_KEY 未设置")
        self.api_key = api_key
        self._last_request_time = 0.0
        self.conn = get_conn()

    def _rate_limit(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    def _fetch_bars(
        self,
        ticker: str,
        from_date: str,  # YYYY-MM-DD 或 Unix ms timestamp
        to_date: str,
        cursor: Optional[str] = None,
    ) -> dict:
        """
        Polygon /aggs 接口，支持 cursor 分页（超过 50000 条时）。
        """
        self._rate_limit()
        url = POLYGON_AGGS_URL.format(
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
        )
        params = {
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
            "apiKey": self.api_key,
        }
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _normalize_bar(asset_id: str, result: dict) -> dict:
        """
        Polygon aggs result 字段：
          t = Unix ms timestamp（bar 开始时间）
          o, h, l, c, v, vw
        """
        bar_ts_ms = result.get("t", 0)
        bar_ts    = bar_ts_ms // 1000
        bar_utc   = datetime.fromtimestamp(bar_ts, tz=timezone.utc).isoformat()
        return {
            "asset_id":    asset_id,
            "bar_time_utc": bar_utc,
            "bar_ts":       bar_ts,
            "open":         result.get("o"),
            "high":         result.get("h"),
            "low":          result.get("l"),
            "close":        result.get("c"),
            "volume":       result.get("v"),
            "vwap":         result.get("vw"),
        }

    def _upsert_bars(self, bars: list[dict]):
        if not bars:
            return
        with transaction(self.conn):
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO price_bars (
                    asset_id, bar_time_utc, bar_ts,
                    open, high, low, close, volume, vwap
                ) VALUES (
                    :asset_id, :bar_time_utc, :bar_ts,
                    :open, :high, :low, :close, :volume, :vwap
                )
                """,
                bars,
            )

    def collect_asset(
        self,
        asset: dict,
        start_utc: datetime,
        end_utc: datetime,
    ) -> int:
        """
        采集单个资产的分钟K线。返回本次写入条数。
        """
        ticker   = asset["ticker"]
        asset_id = asset["id"]
        # Polygon aggs from/to 接受 YYYY-MM-DD 或 Unix 毫秒
        from_ms = int(start_utc.timestamp() * 1000)
        to_ms   = int(end_utc.timestamp() * 1000)

        logger.info(f"[price] {asset_id} ({ticker}) {start_utc.isoformat()} → {end_utc.isoformat()}")

        total = 0
        cursor = None
        page = 0

        while True:
            page += 1
            try:
                data = self._fetch_bars(ticker, from_ms, to_ms, cursor)
            except requests.HTTPError as e:
                logger.error(f"[price] {asset_id} HTTP错误: {e}")
                break
            except Exception as e:
                logger.error(f"[price] {asset_id} 请求异常: {e}")
                break

            results = data.get("results", [])
            if not results:
                break

            bars = [self._normalize_bar(asset_id, r) for r in results]
            self._upsert_bars(bars)
            total += len(bars)

            next_url = data.get("next_url", "")
            if not next_url:
                break
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(next_url).query)
            cursor = qs.get("cursor", [None])[0]
            if not cursor:
                break

        logger.info(f"[price] {asset_id} 写入 {total} 根K线")
        return total

    def collect_all(self, start_utc: datetime, end_utc: datetime) -> dict:
        """采集 config.ASSETS 中所有资产。返回 {asset_id: count} 字典。"""
        results = {}
        for asset in ASSETS:
            count = self.collect_asset(asset, start_utc, end_utc)
            results[asset["id"]] = count
        return results

    def close(self):
        self.conn.close()


# ── 便捷函数：采集昨夜时段 ───────────────────────────────────────────────
def collect_overnight_prices(api_key: str = POLYGON_API_KEY) -> dict:
    from zoneinfo import ZoneInfo
    from config import OVERNIGHT_START_HOUR_BJT, OVERNIGHT_END_HOUR_BJT
    BJT = ZoneInfo("Asia/Shanghai")
    now_bjt = datetime.now(BJT)

    if now_bjt.hour >= OVERNIGHT_END_HOUR_BJT:
        end_bjt   = now_bjt.replace(hour=OVERNIGHT_END_HOUR_BJT,   minute=0, second=0, microsecond=0)
        start_bjt = now_bjt.replace(hour=OVERNIGHT_START_HOUR_BJT, minute=0, second=0, microsecond=0)
        if now_bjt.hour < OVERNIGHT_START_HOUR_BJT:
            start_bjt -= timedelta(days=1)
    else:
        end_bjt   = now_bjt.replace(hour=OVERNIGHT_END_HOUR_BJT,   minute=0, second=0, microsecond=0)
        start_bjt = (now_bjt - timedelta(days=1)).replace(
            hour=OVERNIGHT_START_HOUR_BJT, minute=0, second=0, microsecond=0
        )

    start_utc = start_bjt.astimezone(ZoneInfo("UTC"))
    end_utc   = end_bjt.astimezone(ZoneInfo("UTC"))

    collector = PriceCollector(api_key)
    try:
        return collector.collect_all(start_utc, end_utc)
    finally:
        collector.close()


if __name__ == "__main__":
    init_db()
    result = collect_overnight_prices()
    for asset_id, count in result.items():
        print(f"  {asset_id}: {count} 根K线")
