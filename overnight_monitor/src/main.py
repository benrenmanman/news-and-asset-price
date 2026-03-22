"""
main.py
主调度入口。被 GitHub Actions 或手动调用。

流程：
1. 计算昨夜时间窗口（北京时间）
2. 采集新闻
3. 采集价格K线
4. 运行对齐引擎
5. 调用 AI 归因（config.AI_CONFIG 配置，支持多服务商）
6. 发送 WeCom 通知
"""
import json
import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))

from config import (
    POLYGON_API_KEY, WECOM_WEBHOOK_URL,
    OVERNIGHT_START_HOUR_BJT, OVERNIGHT_END_HOUR_BJT,
    DB_PATH,
)
from db import init_db
from news_collector import NewsCollector
from price_collector import PriceCollector
from aligner import Aligner, print_alignment_report

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("data/run.log", encoding="utf-8"),
    ],
)

BJT = ZoneInfo("Asia/Shanghai")
UTC = ZoneInfo("UTC")


# ── 时间窗口计算 ──────────────────────────────────────────────────────────
def get_overnight_window() -> tuple[datetime, datetime]:
    """返回 (start_utc, end_utc)"""
    now_bjt = datetime.now(BJT)

    if now_bjt.hour < OVERNIGHT_END_HOUR_BJT:
        end_bjt   = now_bjt.replace(hour=OVERNIGHT_END_HOUR_BJT,   minute=0, second=0, microsecond=0)
        start_bjt = (now_bjt - timedelta(days=1)).replace(
            hour=OVERNIGHT_START_HOUR_BJT, minute=0, second=0, microsecond=0
        )
    else:
        end_bjt   = now_bjt.replace(hour=OVERNIGHT_END_HOUR_BJT,   minute=0, second=0, microsecond=0)
        start_bjt = now_bjt.replace(hour=OVERNIGHT_START_HOUR_BJT, minute=0, second=0, microsecond=0)
        if now_bjt.hour >= OVERNIGHT_START_HOUR_BJT:
            start_bjt -= timedelta(days=1)

    return start_bjt.astimezone(UTC), end_bjt.astimezone(UTC)


# ── AI 归因 ───────────────────────────────────────────────────────────────
def run_ai_attribution(aligned_results: list[dict]) -> list[dict]:
    """
    通过 OpenAI-compatible 接口做归因分析。
    接口地址、模型、Key 全部从 config.AI_CONFIG 读取，支持任意兼容后端：
    OpenAI、DeepSeek、Azure OpenAI、302ai、Ollama 等。

    切换服务商只需修改 config.py 的 AI_CONFIG，或设置环境变量：
      AI_BASE_URL / AI_API_KEY / AI_MODEL
    """
    from config import AI_CONFIG
    api_key  = AI_CONFIG.get("api_key", "")
    base_url = AI_CONFIG.get("base_url", "https://api.openai.com/v1")
    model    = AI_CONFIG.get("model", "gpt-4o")

    if not api_key:
        logger.warning("[ai] AI_CONFIG.api_key 未设置，跳过 AI 归因")
        logger.warning("[ai] 请设置环境变量 AI_API_KEY 或在 config.py 中配置")
        return aligned_results

    try:
        from openai import OpenAI
    except ImportError:
        logger.warning("[ai] openai 包未安装，请 pip install openai")
        return aligned_results

    client = OpenAI(api_key=api_key, base_url=base_url)
    logger.info(f"[ai] base_url={base_url}  model={model}")

    from db import get_conn, transaction
    conn = get_conn()

    system_prompt = (
        "你是一位专注于全球宏观和外汇市场的卖方研究员。"
        "用户会给你价格异动数据和同期新闻，你需要做归因分析。"
        "只输出合法的 JSON，不要输出任何其他内容。"
    )

    for r in aligned_results:
        e          = r["event"]
        candidates = r["candidates"]
        if not candidates:
            r["ai_result"] = None
            continue

        news_block = "\n".join([
            f"[{c['time_delta_min']:+.1f}min] [{c['source_name']}] {c['title']}"
            + (f"\n  摘要: {c['description'][:150]}" if c["description"] else "")
            for c in candidates[:10]
        ])

        ret_pct       = e["window_return"] * 100
        direction_str = "上涨" if e["direction"] == "up" else "下跌"

        user_prompt = f"""请分析以下价格异动事件并进行新闻归因。

【价格异动】
资产: {e["asset_id"]}
时间: {e["event_utc"][:19]} UTC
方向: {direction_str}
幅度: {ret_pct:+.2f}%（{e["sigma_multiple"]}个标准差）

【同期新闻（按时间接近度排列，负值=新闻早于异动）】
{news_block}

请以JSON格式输出以下字段（不要输出其他内容）：
{{
  "primary_driver": "最可能的主驱动新闻标题（直接引用上方标题）",
  "logic": "用2-3句话解释价格异动与该新闻的逻辑链条",
  "asset_reaction_consistent": true,
  "confidence": "高/中/低",
  "residual": "是否有无法被新闻解释的残差异动，如有请说明",
  "secondary_drivers": ["次要驱动新闻标题（若有）"]
}}"""

        # response_format=json_object 不是所有后端都支持，按 base_url 判断
        create_kwargs = dict(
            model=model,
            max_tokens=600,
            temperature=0.2,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
        )
        if any(domain in base_url for domain in ("openai.com", "deepseek.com", "302.ai", "api2d")):
            create_kwargs["response_format"] = {"type": "json_object"}

        try:
            response = client.chat.completions.create(**create_kwargs)
            raw_text = response.choices[0].message.content.strip()
            # 兼容部分后端返回 markdown 代码块
            if raw_text.startswith("```"):
                raw_text = raw_text.split("```")[1]
                if raw_text.startswith("json"):
                    raw_text = raw_text[4:]
            ai_result = json.loads(raw_text)
        except Exception as ex:
            logger.error(f"[ai] {e['asset_id']} 归因失败: {ex}")
            ai_result = {"error": str(ex)}

        r["ai_result"] = ai_result

        if "confidence" in ai_result and r.get("event_id"):
            with transaction(conn):
                conn.execute(
                    """
                    UPDATE alignments
                    SET ai_attribution = ?, ai_confidence = ?
                    WHERE event_id = ? AND proximity_rank = 1
                    """,
                    (json.dumps(ai_result, ensure_ascii=False),
                     ai_result.get("confidence", ""),
                     r["event_id"]),
                )

        logger.info(
            f"[ai] {e['asset_id']} 归因完成: "
            f"{ai_result.get('confidence', '?')} / "
            f"{str(ai_result.get('primary_driver', '?'))[:50]}"
        )

    conn.close()
    return aligned_results


# ── WeCom 通知 ────────────────────────────────────────────────────────────
def send_wecom_report(aligned_results: list[dict], start_utc: datetime, end_utc: datetime):
    if not WECOM_WEBHOOK_URL:
        logger.info("[wecom] WECOM_WEBHOOK_URL 未设置，跳过推送")
        return

    import requests

    lines = [
        "## 🌙 隔夜市场异动报告",
        f"**时间段**: {start_utc.strftime('%m/%d %H:%M')} - {end_utc.strftime('%m/%d %H:%M')} UTC",
        f"**检测事件**: {len(aligned_results)} 个\n",
    ]

    for r in aligned_results[:8]:
        e       = r["event"]
        ai      = r.get("ai_result") or {}
        ret_pct = e["window_return"] * 100
        arrow   = "↑" if e["direction"] == "up" else "↓"

        lines.append(
            f"**{e['asset_id']}** {arrow} {ret_pct:+.2f}% ({e['sigma_multiple']}σ)"
            f"  `{e['event_utc'][11:16]} UTC`"
        )
        if ai.get("primary_driver"):
            lines.append(f"> 归因[{ai.get('confidence','?')}]: {ai['primary_driver'][:60]}")
            if ai.get("logic"):
                lines.append(f"> {ai['logic'][:80]}")
        elif r["candidates"]:
            lines.append(f"> 候选新闻: {r['candidates'][0]['title'][:60]}")
        lines.append("")

    payload = {"msgtype": "markdown", "markdown": {"content": "\n".join(lines)}}
    try:
        resp = requests.post(WECOM_WEBHOOK_URL, json=payload, timeout=10)
        resp.raise_for_status()
        logger.info("[wecom] 报告推送成功")
    except Exception as ex:
        logger.error(f"[wecom] 推送失败: {ex}")


# ── 主入口 ────────────────────────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("隔夜市场监控启动")
    logger.info("=" * 60)

    init_db()

    start_utc, end_utc = get_overnight_window()
    start_ts = int(start_utc.timestamp())
    end_ts   = int(end_utc.timestamp())
    logger.info(f"监控窗口: {start_utc.isoformat()} → {end_utc.isoformat()}")

    logger.info("── 步骤1: 采集新闻 ──────────────")
    news_collector = NewsCollector()
    news_count = news_collector.collect(start_utc, end_utc)
    news_collector.close()
    logger.info(f"新闻采集完成: {news_count} 条")

    logger.info("── 步骤2: 采集价格K线 ────────────")
    price_collector = PriceCollector()
    price_counts = price_collector.collect_all(start_utc, end_utc)
    price_collector.close()
    for aid, cnt in price_counts.items():
        logger.info(f"  {aid}: {cnt} 根K线")

    logger.info("── 步骤3: 时间戳对齐 ──────────────")
    aligner = Aligner()
    aligned = aligner.run(start_ts, end_ts)
    aligner.close()

    logger.info("── 步骤4: AI 归因 ─────────────────")
    aligned = run_ai_attribution(aligned)

    print_alignment_report(aligned)

    logger.info("── 步骤5: 推送报告 ─────────────────")
    send_wecom_report(aligned, start_utc, end_utc)

    snapshot_path = f"data/report_{start_utc.strftime('%Y%m%d_%H%M')}.json"
    with open(snapshot_path, "w", encoding="utf-8") as f:
        serializable = [
            {
                "event":      r["event"],
                "event_id":   r.get("event_id"),
                "candidates": [{k: v for k, v in c.items() if k != "raw_json"}
                               for c in r["candidates"][:10]],
                "ai_result":  r.get("ai_result"),
            }
            for r in aligned
        ]
        json.dump(serializable, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON快照已保存: {snapshot_path}")
    logger.info("隔夜市场监控完成")
    return aligned


if __name__ == "__main__":
    main()
