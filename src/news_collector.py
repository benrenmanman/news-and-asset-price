"""
news_collector.py
从 Polygon.io /v2/reference/news 采集新闻，写入 SQLite。

Polygon 新闻 API：
  GET https://api.polygon.io/v2/reference/news
  参数：published_utc.gte, published_utc.lte, limit, sort, order, apiKey

限速：Starter 15 req/min。本模块做 token-bucket 限速，安全边际 12 req/min。
"""
import json
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

import requests

from config import POLYGON_API_KEY, NEWS_FETCH_LIMIT, DB_PATH
from db import get_conn, transaction, init_db

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

POLYGON_NEWS_URL = "https://api.polygon.io/v2/reference/news"
MIN_REQUEST_INTERVAL = 5.0  # 秒，12 req/min 安全边际


class NewsCollector:
    def __init__(self, api_key: str = POLYGON_API_KEY):
        if not api_key:
            raise ValueError("POLYGON_API_KEY 未设置，请检查环境变量。")
        self.api_key = api_key
        self._last_request_time = 0.0
        self.conn = get_conn()

    # ── 限速 ─────────────────────────────────────────────────────────────
    def _rate_limit(self):
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    # ── 单次 API 请求 ──────────────────────────────────────────────────
    def _fetch_page(
        self,
        published_gte: str,
        published_lte: str,
        cursor: Optional[str] = None,
    ) -> dict:
        """
        返回 Polygon API 原始响应 dict。
        cursor 是 Polygon 分页 next_url 中的 cursor 参数。
        """
        self._rate_limit()
        params = {
            "published_utc.gte": published_gte,
            "published_utc.lte": published_lte,
            "limit": NEWS_FETCH_LIMIT,
            "sort": "published_utc",
            "order": "asc",
            "apiKey": self.api_key,
        }
        if cursor:
            params["cursor"] = cursor

        resp = requests.get(POLYGON_NEWS_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    # ── 标准化单条新闻 ────────────────────────────────────────────────
    @staticmethod
    def _normalize(article: dict, fetched_at: str) -> dict:
        pub_utc = article.get("published_utc", "")
        # 将 ISO 8601 转换为 Unix timestamp（秒）
        try:
            dt = datetime.fromisoformat(pub_utc.replace("Z", "+00:00"))
            pub_ts = int(dt.timestamp())
        except Exception:
            pub_ts = 0

        tickers = article.get("tickers", [])
        keywords = article.get("keywords", [])

        # 情绪评分：Polygon 提供 insights[].sentiment，取第一个
        sentiment = None
        insights = article.get("insights", [])
        if insights:
            s = insights[0].get("sentiment")
            if s == "positive":
                sentiment = 0.6
            elif s == "negative":
                sentiment = -0.6
            elif s == "neutral":
                sentiment = 0.0

        return {
            "id":            article.get("id", ""),
            "published_utc": pub_utc,
            "published_ts":  pub_ts,
            "title":         article.get("title", ""),
            "description":   article.get("description", ""),
            "tickers":       json.dumps(tickers),
            "keywords":      json.dumps(keywords),
            "sentiment":     sentiment,
            "source_name":   article.get("publisher", {}).get("name", ""),
            "article_url":   article.get("article_url", ""),
            "raw_json":      json.dumps(article),
            "fetched_at":    fetched_at,
        }

    # ── 写入数据库（批量 upsert） ─────────────────────────────────────
    def _upsert_articles(self, articles: list[dict]):
        if not articles:
            return
        with transaction(self.conn):
            self.conn.executemany(
                """
                INSERT OR IGNORE INTO news (
                    id, published_utc, published_ts, title, description,
                    tickers, keywords, sentiment, source_name, article_url,
                    raw_json, fetched_at
                ) VALUES (
                    :id, :published_utc, :published_ts, :title, :description,
                    :tickers, :keywords, :sentiment, :source_name, :article_url,
                    :raw_json, :fetched_at
                )
                """,
                articles,
            )

    # ── 主采集方法 ────────────────────────────────────────────────────
    def collect(
        self,
        start_utc: datetime,
        end_utc: datetime,
    ) -> int:
        """
        采集 [start_utc, end_utc] 区间内的所有新闻，写入数据库。
        返回本次新增条数。
        """
        published_gte = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        published_lte = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        fetched_at    = datetime.now(timezone.utc).isoformat()

        logger.info(f"[news] 开始采集: {published_gte} → {published_lte}")

        total_inserted = 0
        cursor = None
        page = 0

        while True:
            page += 1
            try:
                data = self._fetch_page(published_gte, published_lte, cursor)
            except requests.HTTPError as e:
                logger.error(f"[news] HTTP错误 (page={page}): {e}")
                break
            except Exception as e:
                logger.error(f"[news] 请求异常 (page={page}): {e}")
                break

            results = data.get("results", [])
            if not results:
                logger.info(f"[news] 第{page}页无结果，采集完毕")
                break

            normalized = [self._normalize(a, fetched_at) for a in results]
            self._upsert_articles(normalized)
            total_inserted += len(normalized)

            logger.info(f"[news] 第{page}页: 获取 {len(results)} 条，累计 {total_inserted} 条")

            # 翻页：从 next_url 中提取 cursor
            next_url = data.get("next_url", "")
            if not next_url:
                break
            # next_url 格式: https://api.polygon.io/...?cursor=XXX&...
            from urllib.parse import urlparse, parse_qs
            qs = parse_qs(urlparse(next_url).query)
            cursor = qs.get("cursor", [None])[0]
            if not cursor:
                break

        logger.info(f"[news] 采集完成，共处理 {total_inserted} 条")
        return total_inserted

    def close(self):
        self.conn.close()


# ── 便捷函数：采集昨夜时段 ───────────────────────────────────────────────
def collect_overnight(
    api_key: str = POLYGON_API_KEY,
    start_hour_bjt: int = 20,
    end_hour_bjt: int = 6,
) -> int:
    """
    采集北京时间前一日 start_hour → 当日 end_hour 的新闻。
    自动转换为 UTC 时间窗口。
    """
    from zoneinfo import ZoneInfo
    BJT = ZoneInfo("Asia/Shanghai")
    now_bjt = datetime.now(BJT)

    # 构造昨夜窗口
    if now_bjt.hour < end_hour_bjt:
        # 当前北京时间还在 06:00 之前：窗口是 昨天20:00 → 今天06:00
        end_bjt   = now_bjt.replace(hour=end_hour_bjt, minute=0, second=0, microsecond=0)
        start_bjt = (now_bjt - timedelta(days=1)).replace(
            hour=start_hour_bjt, minute=0, second=0, microsecond=0
        )
    else:
        # 当前北京时间已过 06:00：窗口是 今天20:00(昨夜) → 今天06:00
        start_bjt = now_bjt.replace(hour=start_hour_bjt, minute=0, second=0, microsecond=0)
        end_bjt   = now_bjt.replace(hour=end_hour_bjt, minute=0, second=0, microsecond=0)
        # 如果现在时间还没到今晚 start_hour，则 start 是昨天
        if now_bjt.hour < start_hour_bjt:
            start_bjt = (now_bjt - timedelta(days=1)).replace(
                hour=start_hour_bjt, minute=0, second=0, microsecond=0
            )

    start_utc = start_bjt.astimezone(ZoneInfo("UTC"))
    end_utc   = end_bjt.astimezone(ZoneInfo("UTC"))

    collector = NewsCollector(api_key)
    try:
        return collector.collect(start_utc, end_utc)
    finally:
        collector.close()


if __name__ == "__main__":
    init_db()
    n = collect_overnight()
    print(f"本次采集新闻 {n} 条")
