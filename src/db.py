"""
db.py
SQLite 数据库初始化与基础操作。
使用 WAL 模式，支持并发读写（GitHub Actions 单进程也适用）。
"""
import sqlite3
import contextlib
from pathlib import Path
from config import DB_PATH


def get_conn() -> sqlite3.Connection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextlib.contextmanager
def transaction(conn: sqlite3.Connection):
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def init_db():
    """建表（幂等，已存在则跳过）。"""
    conn = get_conn()
    with transaction(conn):
        # ── 新闻表 ────────────────────────────────────────────────────────
        conn.execute("""
        CREATE TABLE IF NOT EXISTS news (
            id             TEXT PRIMARY KEY,          -- Polygon article_id
            published_utc  TEXT NOT NULL,             -- ISO 8601 UTC
            published_ts   INTEGER NOT NULL,          -- Unix timestamp（秒），便于范围查询
            title          TEXT NOT NULL,
            description    TEXT,
            tickers        TEXT,                      -- JSON 数组，如 '["AAPL","SPY"]'
            keywords       TEXT,                      -- JSON 数组
            sentiment      REAL,                      -- 情绪评分，-1~1（若来源提供）
            source_name    TEXT,
            article_url    TEXT,
            raw_json       TEXT,                      -- 完整原始JSON备份
            fetched_at     TEXT NOT NULL              -- 本地采集时间 ISO UTC
        )
        """)
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_news_ts ON news(published_ts)
        """)

        # ── 分钟价格表 ────────────────────────────────────────────────────
        conn.execute("""
        CREATE TABLE IF NOT EXISTS price_bars (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id       TEXT NOT NULL,             -- 对应 config.ASSETS[*].id
            bar_time_utc   TEXT NOT NULL,             -- 分钟K线开始时间 ISO 8601
            bar_ts         INTEGER NOT NULL,          -- Unix timestamp（秒）
            open           REAL,
            high           REAL,
            low            REAL,
            close          REAL NOT NULL,
            volume         REAL,
            vwap           REAL,
            UNIQUE(asset_id, bar_ts)
        )
        """)
        conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_price_asset_ts ON price_bars(asset_id, bar_ts)
        """)

        # ── 价格异动事件表 ───────────────────────────────────────────────
        conn.execute("""
        CREATE TABLE IF NOT EXISTS price_events (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id       TEXT NOT NULL,
            event_ts       INTEGER NOT NULL,          -- 异动发生时刻 Unix ts
            event_utc      TEXT NOT NULL,
            window_return  REAL NOT NULL,             -- 窗口期收益率（小数，如 -0.008）
            sigma_multiple REAL NOT NULL,             -- 偏离标准差倍数
            direction      TEXT NOT NULL,             -- 'up' / 'down'
            created_at     TEXT NOT NULL
        )
        """)

        # ── 新闻-价格对齐结果表 ──────────────────────────────────────────
        conn.execute("""
        CREATE TABLE IF NOT EXISTS alignments (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id       INTEGER NOT NULL REFERENCES price_events(id),
            news_id        TEXT NOT NULL REFERENCES news(id),
            time_delta_sec INTEGER NOT NULL,          -- 新闻时间 - 事件时间（负=新闻在前）
            proximity_rank INTEGER NOT NULL,          -- 1=最近
            ai_attribution TEXT,                      -- AI归因叙事（JSON）
            ai_confidence  TEXT,                      -- '高'/'中'/'低'
            created_at     TEXT NOT NULL
        )
        """)
    conn.close()
    print(f"[db] 数据库初始化完成: {DB_PATH}")


if __name__ == "__main__":
    init_db()
