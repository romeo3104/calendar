#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import html
import json
import logging
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import requests
import yfinance as yf


LOGGER = logging.getLogger(__name__)

JST = ZoneInfo("Asia/Tokyo")
NY = ZoneInfo("America/New_York")

ROOT_DIR = Path(__file__).resolve().parent.parent
DIST_DIR = ROOT_DIR / "dist"
SUMMARY_DIR = DIST_DIR / "summary"

MARKET_GROUPS = {
    "株価": [
        {"label": "S&P 500", "symbol": "^GSPC", "suffix": ""},
        {"label": "NASDAQ", "symbol": "^IXIC", "suffix": ""},
        {"label": "ダウ", "symbol": "^DJI", "suffix": ""},
        {"label": "日経225", "symbol": "^N225", "suffix": ""},
    ],
    "先物": [
        {"label": "S&P 500先物", "symbol": "ES=F", "suffix": ""},
        {"label": "NASDAQ100先物", "symbol": "NQ=F", "suffix": ""},
        {"label": "NYダウ先物", "symbol": "YM=F", "suffix": ""},
    ],
    "債券・金利": [
        {"label": "米3か月", "symbol": "^IRX", "suffix": "%"},
        {"label": "米5年", "symbol": "^FVX", "suffix": "%"},
        {"label": "米10年", "symbol": "^TNX", "suffix": "%"},
        {"label": "米30年", "symbol": "^TYX", "suffix": "%"},
    ],
    "為替": [
        {"label": "ドル円", "symbol": "JPY=X", "suffix": ""},
        {"label": "ユーロドル", "symbol": "EURUSD=X", "suffix": ""},
        {"label": "ドル指数", "symbol": "DX-Y.NYB", "suffix": ""},
    ],
    "コモディティ": [
        {"label": "WTI原油", "symbol": "CL=F", "suffix": ""},
        {"label": "ブレント原油", "symbol": "BZ=F", "suffix": ""},
        {"label": "金", "symbol": "GC=F", "suffix": ""},
        {"label": "銀", "symbol": "SI=F", "suffix": ""},
        {"label": "銅", "symbol": "HG=F", "suffix": ""},
    ],
    "暗号資産": [
        {"label": "Bitcoin", "symbol": "BTC-USD", "suffix": ""},
        {"label": "Ethereum", "symbol": "ETH-USD", "suffix": ""},
    ],
}

NEWS_SOURCES = {
    "Reuters": "https://news.google.com/rss/search?q=site:reuters.com%20(markets%20OR%20economy%20OR%20stocks%20OR%20bonds%20OR%20oil%20OR%20currencies)%20when:1d&hl=ja&gl=JP&ceid=JP:ja",
    "Bloomberg": "https://news.google.com/rss/search?q=site:bloomberg.com%20(markets%20OR%20economy%20OR%20stocks%20OR%20bonds%20OR%20oil%20OR%20currencies)%20when:1d&hl=ja&gl=JP&ceid=JP:ja",
}


@dataclass
class MarketItem:
    label: str
    symbol: str
    value: Optional[float]
    previous: Optional[float]
    change: Optional[float]
    change_pct: Optional[float]
    suffix: str = ""
    error: Optional[str] = None


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def should_run_now(force: bool) -> bool:
    if force:
        return True

    now_ny = datetime.now(NY)
    return now_ny.weekday() < 5 and now_ny.hour == 16 and 10 <= now_ny.minute < 20


def format_number(item: MarketItem) -> str:
    if item.value is None:
        return "取得失敗"

    decimals = 2
    if item.symbol == "JPY=X":
        decimals = 3
    if item.symbol in {"EURUSD=X", "DX-Y.NYB"}:
        decimals = 4
    if item.symbol in {"^IRX", "^FVX", "^TNX", "^TYX"}:
        decimals = 3

    return f"{item.value:,.{decimals}f}{item.suffix}"


def format_change(item: MarketItem) -> str:
    if item.change is None or item.change_pct is None:
        return "N/A"
    sign = "+" if item.change >= 0 else ""
    return f"{sign}{item.change:,.2f} / {sign}{item.change_pct:.2f}%"


def fetch_market_item(label: str, symbol: str, suffix: str) -> MarketItem:
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="7d", interval="1d", auto_adjust=False, actions=False)
        hist = hist.dropna(subset=["Close"])

        if hist.empty:
            raise ValueError("価格履歴が取得できませんでした。")

        if len(hist) >= 2:
            previous = float(hist["Close"].iloc[-2])
            value = float(hist["Close"].iloc[-1])
        else:
            value = float(hist["Close"].iloc[-1])
            previous = None

        change = None if previous is None else value - previous
        change_pct = None if previous in (None, 0) else (change / previous) * 100

        return MarketItem(
            label=label,
            symbol=symbol,
            value=value,
            previous=previous,
            change=change,
            change_pct=change_pct,
            suffix=suffix,
        )
    except Exception as exc:
        LOGGER.exception("市場データ取得失敗: %s", symbol)
        return MarketItem(
            label=label,
            symbol=symbol,
            value=None,
            previous=None,
            change=None,
            change_pct=None,
            suffix=suffix,
            error=str(exc),
        )


def fetch_all_markets() -> Dict[str, List[MarketItem]]:
    results: Dict[str, List[MarketItem]] = {}
    for group_name, entries in MARKET_GROUPS.items():
        items: List[MarketItem] = []
        for entry in entries:
            items.append(fetch_market_item(entry["label"], entry["symbol"], entry["suffix"]))
        results[group_name] = items
    return results


def fetch_news_items(url: str, limit: int = 8) -> List[dict]:
    try:
        response = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        response.raise_for_status()
        root = ET.fromstring(response.content)
        items: List[dict] = []

        for item in root.findall(".//item"):
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            if not title or not link:
                continue
            items.append({"title": title, "link": link, "pub_date": pub_date})
            if len(items) >= limit:
                break

        return items
    except Exception as exc:
        LOGGER.exception("ニュース取得失敗: %s", url)
        return [{"title": f"取得失敗: {exc}", "link": "", "pub_date": ""}]


def build_overview_bullets(markets: Dict[str, List[MarketItem]]) -> List[str]:
    bullets: List[str] = []

    def pick(symbol: str) -> Optional[MarketItem]:
        for group_items in markets.values():
            for item in group_items:
                if item.symbol == symbol:
                    return item
        return None

    spx = pick("^GSPC")
    ndx = pick("^IXIC")
    dxy = pick("DX-Y.NYB")
    us10y = pick("^TNX")
    oil = pick("CL=F")
    gold = pick("GC=F")
    btc = pick("BTC-USD")

    def mood(item: Optional[MarketItem], up_text: str, down_text: str, flat_text: str) -> str:
        if item is None or item.change_pct is None:
            return flat_text
        if item.change_pct > 0.15:
            return up_text
        if item.change_pct < -0.15:
            return down_text
        return flat_text

    anchor_equity = spx if (spx and ndx and (spx.change_pct or 0) >= (ndx.change_pct or 0)) else ndx

    bullets.append(
        f"株式は、S&P500とNASDAQを基準にみると {mood(anchor_equity, 'リスク選好', 'リスク回避', '方向感限定')} の地合いです。"
    )
    bullets.append(
        f"金利・為替は、米10年債利回りとドル指数を基準にみると {mood(us10y, '金利上昇圧力', '金利低下', '金利横ばい')} / {mood(dxy, 'ドル高', 'ドル安', 'ドル横ばい')} です。"
    )
    bullets.append(
        f"コモディティは、WTIと金を基準にみると {mood(oil, '景気・供給懸念で原油高', '原油安', '原油横ばい')}、{mood(gold, '安全資産買い', '安全資産売り', '金は横ばい')} です。"
    )
    bullets.append(
        f"暗号資産は、Bitcoin を基準にみると {mood(btc, '強含み', '弱含み', 'もみ合い')} です。"
    )
    return bullets


def build_market_cards(markets: Dict[str, List[MarketItem]]) -> str:
    priority = ["^GSPC", "^IXIC", "^DJI", "^TNX", "JPY=X", "DX-Y.NYB", "CL=F", "GC=F", "BTC-USD"]
    cards_html = []
    all_items = {item.symbol: item for group in markets.values() for item in group}
    for symbol in priority:
        item = all_items.get(symbol)
        if item is None:
            continue
        cards_html.append(
            f"""
            <article class="summary-card">
              <div class="summary-card-title">{html.escape(item.label)}</div>
              <div class="summary-card-value">{html.escape(format_number(item))}</div>
              <div class="summary-card-change">{html.escape(format_change(item))}</div>
            </article>
            """
        )
    return "\n".join(cards_html)


def build_market_tables(markets: Dict[str, List[MarketItem]]) -> str:
    sections = []
    for group_name, items in markets.items():
        rows = []
        for item in items:
            rows.append(
                f"""
                <tr>
                  <td>{html.escape(item.label)}</td>
                  <td>{html.escape(format_number(item))}</td>
                  <td>{html.escape(format_change(item))}</td>
                </tr>
                """
            )
        sections.append(
            f"""
            <section class="summary-section">
              <h2>{html.escape(group_name)}</h2>
              <div class="summary-table-wrap">
                <table class="summary-table">
                  <thead>
                    <tr>
                      <th>項目</th>
                      <th>終値 / 指標値</th>
                      <th>前日比</th>
                    </tr>
                  </thead>
                  <tbody>
                    {''.join(rows)}
                  </tbody>
                </table>
              </div>
            </section>
            """
        )
    return "\n".join(sections)


def build_news_sections(news_map: Dict[str, List[dict]]) -> str:
    sections = []
    for publisher, items in news_map.items():
        lis = []
        for item in items:
            title = html.escape(item["title"])
            link = item["link"]
            if link:
                safe_link = html.escape(link, quote=True)
                lis.append(f'<li><a href="{safe_link}" target="_blank" rel="noopener noreferrer">{title}</a></li>')
            else:
                lis.append(f"<li>{title}</li>")
        sections.append(
            f"""
            <section class="summary-section">
              <h2>{html.escape(publisher)} ニュース</h2>
              <ul class="summary-news-list">
                {''.join(lis)}
              </ul>
            </section>
            """
        )
    return "\n".join(sections)


def build_summary_html(markets: Dict[str, List[MarketItem]], news_map: Dict[str, List[dict]], generated_at_jst: datetime, generated_at_ny: datetime) -> str:
    bullets = build_overview_bullets(markets)
    bullets_html = "".join(f"<li>{html.escape(line)}</li>" for line in bullets)
    cards_html = build_market_cards(markets)
    tables_html = build_market_tables(markets)
    news_html = build_news_sections(news_map)

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>世界経済サマリー</title>
  <link rel="stylesheet" href="../style.css">
</head>
<body class="summary-page">
  <header class="page-header">
    <div class="page-header-top">
      <div class="year-nav">
        <a class="year-nav-button" href="../index.html">カレンダーへ戻る</a>
      </div>
      <div class="year-range-note">NY市場クローズ10分後の自動生成を想定</div>
    </div>
    <h1>世界経済サマリー</h1>
    <p class="summary-meta">生成時刻 JST: {html.escape(generated_at_jst.strftime("%Y-%m-%d %H:%M:%S"))} / New York: {html.escape(generated_at_ny.strftime("%Y-%m-%d %H:%M:%S"))}</p>
  </header>

  <section class="summary-section">
    <h2>概況</h2>
    <ul class="summary-bullets">
      {bullets_html}
    </ul>
  </section>

  <section class="summary-cards-grid">
    {cards_html}
  </section>

  {tables_html}

  {news_html}
</body>
</html>"""


def write_outputs(html_text: str, data: dict, current_date: str) -> None:
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)
    latest_html = SUMMARY_DIR / "latest.html"
    latest_json = SUMMARY_DIR / "latest.json"
    dated_html = SUMMARY_DIR / f"{current_date}.html"
    dated_json = SUMMARY_DIR / f"{current_date}.json"

    latest_html.write_text(html_text, encoding="utf-8")
    latest_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    dated_html.write_text(html_text, encoding="utf-8")
    dated_json.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    setup_logging()
    force = "--force" in sys.argv

    if not should_run_now(force):
        LOGGER.info("NY 16:10 条件外のためスキップします。")
        return 0

    now_jst = datetime.now(JST)
    now_ny = now_jst.astimezone(NY)

    markets = fetch_all_markets()
    news_map = {publisher: fetch_news_items(url) for publisher, url in NEWS_SOURCES.items()}

    payload = {
        "generated_at_jst": now_jst.isoformat(),
        "generated_at_ny": now_ny.isoformat(),
        "markets": {
            group: [
                {
                    "label": item.label,
                    "symbol": item.symbol,
                    "value": item.value,
                    "previous": item.previous,
                    "change": item.change,
                    "change_pct": item.change_pct,
                    "suffix": item.suffix,
                    "error": item.error,
                }
                for item in items
            ]
            for group, items in markets.items()
        },
        "news": news_map,
    }

    html_text = build_summary_html(markets, news_map, now_jst, now_ny)
    write_outputs(html_text, payload, now_jst.strftime("%Y-%m-%d"))
    LOGGER.info("世界経済サマリー生成完了: %s", SUMMARY_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
