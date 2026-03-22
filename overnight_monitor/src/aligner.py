"""
aligner.py
时间戳对齐引擎：核心模块

功能：
1. 从 price_bars 中检测价格异动事件（滚动窗口 + σ 阈值）
2. 为每个异动事件，在 ±(PRE, POST) 分钟窗口内查找候选新闻
3. 按时间接近度排序，写入 alignments 表
"""
import json
import logging
import statistics
from datetime import datetime, timezone
from typing import Optional

from config import (
    PRICE_MOVE_WINDOW_MIN,
    PRICE_SIGMA_THRESHOLD,
    NEWS_PRE_WINDOW_MIN,
    NEWS_POST_WINDOW_MIN,
    ASSETS,
)
from db import get_conn, transaction, init_db

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class Aligner:
    def __init__(self):
        self.conn = get_conn()

    # ── 1. 价格异动检测 ──────────────────────────────────────────────
    def detect_price_events(
        self,
        start_ts: int,
        end_ts: int,
        asset_id: Optional[str] = None,
    ) -> list[dict]:
        """
        对指定时间范围内的分钟K线做滚动窗口检测。
        返回所有触发阈值的异动事件列表（未写库，先返回供检查）。
        """
        asset_ids = [asset_id] if asset_id else [a["id"] for a in ASSETS]
        events = []

        for aid in asset_ids:
            # 拉取比窗口多一点的历史K线，确保第一根有足够的前置数据
            bars = self.conn.execute(
                """
                SELECT bar_ts, close
                FROM price_bars
                WHERE asset_id = ? AND bar_ts >= ? AND bar_ts <= ?
                ORDER BY bar_ts ASC
                """,
                (aid, start_ts - PRICE_MOVE_WINDOW_MIN * 60, end_ts),
            ).fetchall()

            if len(bars) < PRICE_MOVE_WINDOW_MIN + 1:
                logger.warning(f"[aligner] {aid} 数据不足 ({len(bars)} 根)，跳过")
                continue

            closes = [row["close"] for row in bars]
            timestamps = [row["bar_ts"] for row in bars]

            # 计算所有分钟收益率（用于全局 σ 估计）
            all_returns = []
            for i in range(1, len(closes)):
                if closes[i - 1] > 0:
                    all_returns.append((closes[i] - closes[i - 1]) / closes[i - 1])

            if len(all_returns) < PRICE_MOVE_WINDOW_MIN:
                continue

            global_std = statistics.stdev(all_returns)
            if global_std == 0:
                continue

            # 滚动窗口：计算 PRICE_MOVE_WINDOW_MIN 分钟内的累积收益率
            W = PRICE_MOVE_WINDOW_MIN
            for i in range(W, len(closes)):
                price_start = closes[i - W]
                price_end   = closes[i]
                if price_start == 0:
                    continue
                window_ret = (price_end - price_start) / price_start
                sigma_mult = abs(window_ret) / global_std

                if sigma_mult >= PRICE_SIGMA_THRESHOLD:
                    event_ts  = timestamps[i]
                    event_utc = datetime.fromtimestamp(event_ts, tz=timezone.utc).isoformat()

                    # 去重：如果前一根K线已经触发同方向事件，合并（避免连续触发刷库）
                    if events and events[-1]["asset_id"] == aid:
                        prev_ts   = events[-1]["event_ts"]
                        prev_dir  = events[-1]["direction"]
                        curr_dir  = "up" if window_ret > 0 else "down"
                        if event_ts - prev_ts <= PRICE_MOVE_WINDOW_MIN * 60 and prev_dir == curr_dir:
                            # 更新为更强的那个
                            if sigma_mult > events[-1]["sigma_multiple"]:
                                events[-1].update({
                                    "event_ts":      event_ts,
                                    "event_utc":     event_utc,
                                    "window_return": window_ret,
                                    "sigma_multiple": sigma_mult,
                                })
                            continue

                    events.append({
                        "asset_id":      aid,
                        "event_ts":      event_ts,
                        "event_utc":     event_utc,
                        "window_return": window_ret,
                        "sigma_multiple": round(sigma_mult, 2),
                        "direction":     "up" if window_ret > 0 else "down",
                    })

        logger.info(f"[aligner] 检测到 {len(events)} 个价格异动事件")
        return events

    # ── 2. 写入价格事件 ──────────────────────────────────────────────
    def save_price_events(self, events: list[dict]) -> list[int]:
        """批量写入 price_events，返回插入的 rowid 列表。"""
        now_utc = datetime.now(timezone.utc).isoformat()
        ids = []
        with transaction(self.conn):
            for e in events:
                cur = self.conn.execute(
                    """
                    INSERT OR IGNORE INTO price_events
                        (asset_id, event_ts, event_utc, window_return, sigma_multiple, direction, created_at)
                    VALUES (:asset_id, :event_ts, :event_utc, :window_return, :sigma_multiple, :direction, :created_at)
                    """,
                    {**e, "created_at": now_utc},
                )
                if cur.lastrowid:
                    ids.append(cur.lastrowid)
        return ids

    # ── 3. 查找候选新闻 ──────────────────────────────────────────────
    def find_candidate_news(
        self,
        event_ts: int,
        pre_min: int  = NEWS_PRE_WINDOW_MIN,
        post_min: int = NEWS_POST_WINDOW_MIN,
        max_candidates: int = 20,
    ) -> list[dict]:
        """
        在 [event_ts - pre_min*60, event_ts + post_min*60] 窗口内
        查找新闻，按时间接近度升序排列。
        """
        window_start = event_ts - pre_min * 60
        window_end   = event_ts + post_min * 60

        rows = self.conn.execute(
            """
            SELECT id, published_utc, published_ts, title, description,
                   tickers, keywords, sentiment, source_name
            FROM news
            WHERE published_ts >= ? AND published_ts <= ?
            ORDER BY ABS(published_ts - ?) ASC
            LIMIT ?
            """,
            (window_start, window_end, event_ts, max_candidates),
        ).fetchall()

        candidates = []
        for rank, row in enumerate(rows, start=1):
            delta_sec = row["published_ts"] - event_ts
            candidates.append({
                "news_id":         row["id"],
                "published_utc":   row["published_utc"],
                "published_ts":    row["published_ts"],
                "title":           row["title"],
                "description":     row["description"],
                "tickers":         json.loads(row["tickers"] or "[]"),
                "keywords":        json.loads(row["keywords"] or "[]"),
                "sentiment":       row["sentiment"],
                "source_name":     row["source_name"],
                "time_delta_sec":  delta_sec,
                "time_delta_min":  round(delta_sec / 60, 1),
                "proximity_rank":  rank,
            })

        return candidates

    # ── 4. 保存对齐结果（AI 归因前的候选配对） ───────────────────────
    def save_alignments(self, event_id: int, candidates: list[dict]):
        """将候选新闻写入 alignments 表（ai_attribution 暂为空，等 AI 模块填充）。"""
        now_utc = datetime.now(timezone.utc).isoformat()
        with transaction(self.conn):
            for c in candidates:
                self.conn.execute(
                    """
                    INSERT OR IGNORE INTO alignments
                        (event_id, news_id, time_delta_sec, proximity_rank, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (event_id, c["news_id"], c["time_delta_sec"], c["proximity_rank"], now_utc),
                )

    # ── 5. 主流程：运行对齐 ──────────────────────────────────────────
    def run(self, start_ts: int, end_ts: int) -> list[dict]:
        """
        完整对齐流程：检测事件 → 查找新闻 → 保存候选配对。
        返回结构化的事件+候选新闻列表，供 AI 归因模块消费。

        返回结构：
        [
          {
            "event": {...},
            "event_id": int,
            "candidates": [
              {"news_id": ..., "title": ..., "time_delta_min": ..., ...},
              ...
            ]
          },
          ...
        ]
        """
        events = self.detect_price_events(start_ts, end_ts)
        if not events:
            logger.info("[aligner] 无价格异动事件，流程结束")
            return []

        event_ids = self.save_price_events(events)
        # 对于已存在的事件（IGNORE），重新查询 id
        if not event_ids:
            event_ids = []
            for e in events:
                row = self.conn.execute(
                    "SELECT id FROM price_events WHERE asset_id=? AND event_ts=?",
                    (e["asset_id"], e["event_ts"]),
                ).fetchone()
                if row:
                    event_ids.append(row["id"])

        aligned_results = []
        for event, event_id in zip(events, event_ids):
            candidates = self.find_candidate_news(event["event_ts"])
            if candidates:
                self.save_alignments(event_id, candidates)
            aligned_results.append({
                "event":      event,
                "event_id":   event_id,
                "candidates": candidates,
            })
            logger.info(
                f"[aligner] {event['asset_id']} {event['event_utc']} "
                f"σ={event['sigma_multiple']} "
                f"ret={event['window_return']:.4%} "
                f"候选新闻={len(candidates)}条"
            )

        return aligned_results

    def close(self):
        self.conn.close()


# ── 便捷函数 ──────────────────────────────────────────────────────────────
def run_alignment(start_ts: int, end_ts: int) -> list[dict]:
    aligner = Aligner()
    try:
        return aligner.run(start_ts, end_ts)
    finally:
        aligner.close()


# ── 调试打印：格式化对齐结果 ─────────────────────────────────────────────
def print_alignment_report(results: list[dict]):
    if not results:
        print("无价格异动事件。")
        return

    print(f"\n{'='*70}")
    print(f"价格异动事件 & 候选新闻归因报告  共 {len(results)} 个事件")
    print(f"{'='*70}")

    for r in results:
        e = r["event"]
        direction_arrow = "↑" if e["direction"] == "up" else "↓"
        ret_pct = e["window_return"] * 100

        print(f"\n▶ {e['asset_id']}  {direction_arrow} {ret_pct:+.2f}%  "
              f"({e['sigma_multiple']}σ)  @ {e['event_utc'][:19]} UTC")

        if not r["candidates"]:
            print("   └─ 未找到同期新闻")
            continue

        for c in r["candidates"][:5]:  # 展示前5条
            delta_str = f"{c['time_delta_min']:+.1f}min"
            tickers_str = ",".join(c["tickers"][:3]) if c["tickers"] else "—"
            print(f"   [{delta_str:>8}] [{tickers_str:<12}] {c['title'][:65]}")


if __name__ == "__main__":
    from datetime import timedelta
    init_db()

    # 测试：对过去10小时做对齐
    now = datetime.now(timezone.utc)
    end_ts   = int(now.timestamp())
    start_ts = int((now - timedelta(hours=10)).timestamp())

    results = run_alignment(start_ts, end_ts)
    print_alignment_report(results)
