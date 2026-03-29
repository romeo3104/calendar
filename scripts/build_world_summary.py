#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import html
import json
import logging
import re
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

NEWS_SOURCES = {
    "Reuters日本語": (
        "https://news.google.com/rss/search?"
        "q=site:jp.reuters.com%20"
        "(市場%20OR%20経済%20OR%20株式%20OR%20債券%20OR%20原油%20OR%20為替%20OR%20金)%20when:1d"
        "&hl=ja&gl=JP&ceid=JP:ja"
    ),
    "Bloomberg日本語": (
        "https://news.google.com/rss/search?"
        "q=(site:bloomberg.co.jp%20OR%20site:bloomberg.com)%20"
        "(市場%20OR%20経済%20OR%20株式%20OR%20債券%20OR%20原油%20OR%20為替%20OR%20金)%20when:1d"
        "&hl=ja&gl=JP&ceid=JP:ja"
    ),
}

JAPANESE_CHAR_PATTERN = re.compile(r"[ぁ-んァ-ヶ一-龠々ー]")
NOISE_SUFFIX_PATTERN = re.compile(r"\s*[-|｜]\s*(Reuters|Bloomberg|ロイター|ブルームバーグ).*$", re.IGNORECASE)

INVESTING_JGB_URLS = {
    "日本国債5年利回り": "https://www.investing.com/rates-bonds/japan-5-year-bond-yield",
    "日本国債10年利回り": "https://www.investing.com/rates-bonds/japan-10-year-bond-yield",
    "日本国債30年利回り": "https://www.investing.com/rates-bonds/japan-30-year-bond-yield",
}

YF_ITEMS = {
    "株式": [
        {"name": "NYダウ", "symbol": "^DJI", "source": "Yahoo Finance"},
        {"name": "NASDAQ総合", "symbol": "^IXIC", "source": "Yahoo Finance"},
        {"name": "S&P500", "symbol": "^GSPC", "source": "Yahoo Finance"},
        {"name": "SOX", "symbol": "^SOX", "source": "Yahoo Finance"},
        {"name": "VIX", "symbol": "^VIX", "source": "Yahoo Finance"},
        {"name": "日経225", "symbol": "^N225", "source": "Yahoo Finance"},
        {"name": "TOPIX", "symbol": "998405.T", "source": "Yahoo!ファイナンス"},
        {
            "name": "東証REIT",
            "symbol": "1343.T",
            "source": "Yahoo!ファイナンス",
            "note": "代替取得: NEXT FUNDS 東証REIT指数連動型上場投信",
        },
    ],
    "為替": [
        {"name": "ドル円", "symbol": "JPY=X", "source": "Yahoo Finance"},
        {"name": "ユーロドル", "symbol": "EURUSD=X", "source": "Yahoo Finance"},
        {"name": "ユーロ円", "symbol": "EURJPY=X", "source": "Yahoo Finance"},
        {"name": "ポンド円", "symbol": "GBPJPY=X", "source": "Yahoo Finance"},
        {"name": "ドルインデックス", "symbol": "DX-Y.NYB", "source": "Yahoo Finance"},
        {"name": "豪ドル円", "symbol": "AUDJPY=X", "source": "Yahoo Finance"},
        {"name": "ニュージーランド円", "symbol": "NZDJPY=X", "source": "Yahoo Finance"},
        {"name": "スイスフラン円", "symbol": "CHFJPY=X", "source": "Yahoo Finance"},
        {"name": "韓国ウォン円", "symbol": "KRWJPY=X", "source": "Yahoo Finance"},
        {"name": "トルコリラ円", "symbol": "TRYJPY=X", "source": "Yahoo Finance"},
        {"name": "南アフリカランド円", "symbol": "ZARJPY=X", "source": "Yahoo Finance"},
        {"name": "メキシコペソ円", "symbol": "MXNJPY=X", "source": "Yahoo Finance"},
    ],
    "米国債": [
        {"name": "米国債5年利回り", "symbol": "^FVX", "source": "Yahoo Finance", "is_yield10x": True, "suffix": "%"},
        {"name": "米国債10年利回り", "symbol": "^TNX", "source": "Yahoo Finance", "is_yield10x": True, "suffix": "%"},
        {"name": "米国債30年利回り", "symbol": "^TYX", "source": "Yahoo Finance", "is_yield10x": True, "suffix": "%"},
    ],
    "商品": [
        {"name": "金", "symbol": "GC=F", "source": "Yahoo Finance"},
        {"name": "銀", "symbol": "SI=F", "source": "Yahoo Finance"},
        {"name": "WTI原油", "symbol": "CL=F", "source": "Yahoo Finance"},
        {"name": "Brent原油", "symbol": "BZ=F", "source": "Yahoo Finance"},
        {"name": "天然ガス", "symbol": "NG=F", "source": "Yahoo Finance"},
        {"name": "銅", "symbol": "HG=F", "source": "Yahoo Finance"},
        {"name": "プラチナ", "symbol": "PL=F", "source": "Yahoo Finance"},
        {"name": "パラジウム", "symbol": "PA=F", "source": "Yahoo Finance"},
    ],
    "暗号資産": [
        {"name": "BTC/USD", "symbol": "BTC-USD", "source": "Yahoo Finance"},
        {"name": "BTC/JPY", "symbol": "BTC-JPY", "source": "Yahoo Finance"},
        {"name": "ETH/USD", "symbol": "ETH-USD", "source": "Yahoo Finance"},
        {"name": "XRP/USD", "symbol": "XRP-USD", "source": "Yahoo Finance"},
    ],
}

CATEGORY_ORDER = ["株式", "為替", "米国債", "日本国債", "商品", "暗号資産"]


@dataclass
class MarketRow:
    category: str
    name: str
    value: Optional[float]
    previous: Optional[float]
    change: Optional[float]
    change_pct: Optional[float]
    source: str
    acquired_at: Optional[str]
    suffix: str = ""
    note: str = ""
    missing_reason: str = ""

    @property
    def is_missing(self) -> bool:
        return self.value is None

    @property
    def display_source(self) -> str:
        if self.note:
            return f"{self.source}（{self.note}）"
        return self.source


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def should_run_now(force: bool) -> bool:
    if force:
        return True
    now_ny = datetime.now(NY)
    return now_ny.weekday() < 5 and now_ny.hour == 16 and 10 <= now_ny.minute < 20


def requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session


def decode_response_content(response: requests.Response) -> str:
    for encoding in ("utf-8-sig", "cp932", response.encoding or "utf-8"):
        try:
            return response.content.decode(encoding)
        except Exception:
            continue
    return response.text


def fetch_yahoo_row(category: str, spec: dict) -> MarketRow:
    name = spec["name"]
    symbol = spec["symbol"]
    source = spec["source"]
    suffix = spec.get("suffix", "")
    note = spec.get("note", "")
    is_yield10x = spec.get("is_yield10x", False)

    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="7d", interval="1d", auto_adjust=False, actions=False)
        hist = hist.dropna(subset=["Close"])
        if hist.empty:
            raise ValueError("価格履歴が取得できませんでした。")

        current = float(hist["Close"].iloc[-1])
        previous = float(hist["Close"].iloc[-2]) if len(hist) >= 2 else None

        if is_yield10x:
            current /= 10.0
            if previous is not None:
                previous /= 10.0

        change = None if previous is None else current - previous
        change_pct = None if previous in (None, 0) else (change / previous) * 100

        acquired_at = None
        idx = hist.index[-1]
        if hasattr(idx, "tz_convert"):
            try:
                acquired_at = idx.tz_convert(JST).strftime("%Y-%m-%d %H:%M:%S JST")
            except Exception:
                acquired_at = idx.strftime("%Y-%m-%d")
        else:
            acquired_at = str(idx)

        return MarketRow(
            category=category,
            name=name,
            value=current,
            previous=previous,
            change=change,
            change_pct=change_pct,
            source=source,
            acquired_at=acquired_at,
            suffix=suffix,
            note=note,
        )
    except Exception as exc:
        LOGGER.exception("Yahoo取得失敗: %s", symbol)
        return MarketRow(
            category=category,
            name=name,
            value=None,
            previous=None,
            change=None,
            change_pct=None,
            source=source,
            acquired_at=None,
            suffix=suffix,
            note=note,
            missing_reason=f"Yahoo取得失敗: {exc}",
        )


def parse_mof_jgb_rows(session: requests.Session) -> Dict[str, MarketRow]:
    url = "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv"
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        text = decode_response_content(response)
        reader = csv.reader(text.splitlines())
        rows = [row for row in reader if row]

        header = None
        data_rows = []
        for row in rows:
            if header is None and any("年" in cell for cell in row):
                header = row
                continue
            if header is not None:
                data_rows.append(row)

        if header is None or not data_rows:
            raise ValueError("財務省CSVのヘッダー解析に失敗しました。")

        latest = None
        for row in reversed(data_rows):
            if len(row) >= 2 and any(cell.strip() for cell in row):
                latest = row
                break
        if latest is None:
            raise ValueError("財務省CSVにデータ行がありません。")

        header_map = {cell.strip(): idx for idx, cell in enumerate(header)}
        date_idx = header_map.get("基準日", 0)
        targets = {"5年": "日本国債5年利回り", "10年": "日本国債10年利回り", "30年": "日本国債30年利回り"}

        result: Dict[str, MarketRow] = {}
        for tenor, name in targets.items():
            if tenor not in header_map:
                raise ValueError(f"財務省CSVに {tenor} 列がありません。")

            idx = header_map[tenor]
            current_text = latest[idx].strip() if idx < len(latest) else ""
            date_text = latest[date_idx].strip() if date_idx < len(latest) else ""

            if not current_text:
                result[name] = MarketRow(
                    category="日本国債",
                    name=name,
                    value=None,
                    previous=None,
                    change=None,
                    change_pct=None,
                    source="財務省",
                    acquired_at=date_text or None,
                    suffix="%",
                    missing_reason="財務省CSVの最新行に値がありません。",
                )
                continue

            current = float(current_text)
            previous = None
            for row in reversed(data_rows[:-1]):
                if idx < len(row) and row[idx].strip():
                    previous = float(row[idx].strip())
                    break

            change = None if previous is None else current - previous
            change_pct = None if previous in (None, 0) else (change / previous) * 100

            result[name] = MarketRow(
                category="日本国債",
                name=name,
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="財務省",
                acquired_at=f"{date_text} 財務省公表値" if date_text else "財務省公表値",
                suffix="%",
            )

        return result

    except Exception as exc:
        LOGGER.exception("財務省JGB取得失敗")
        return {
            "日本国債5年利回り": MarketRow("日本国債", "日本国債5年利回り", None, None, None, None, "財務省", None, "%", missing_reason=f"財務省取得失敗: {exc}"),
            "日本国債10年利回り": MarketRow("日本国債", "日本国債10年利回り", None, None, None, None, "財務省", None, "%", missing_reason=f"財務省取得失敗: {exc}"),
            "日本国債30年利回り": MarketRow("日本国債", "日本国債30年利回り", None, None, None, None, "財務省", None, "%", missing_reason=f"財務省取得失敗: {exc}"),
        }


def parse_first_float(text: str, pattern: str) -> Optional[float]:
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    try:
        return float(match.group(1).replace(",", ""))
    except Exception:
        return None


def fetch_investing_jgb_row(session: requests.Session, name: str, url: str) -> MarketRow:
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        text = response.text

        current = parse_first_float(text, r'Add to Watchlist.*?([0-9]+\.[0-9]+)')
        if current is None:
            current = parse_first_float(text, r'Prev\. Close\s*([0-9]+\.[0-9]+)')
        previous = parse_first_float(text, r'Prev\. Close\s*([0-9]+\.[0-9]+)')

        if current is None:
            raise ValueError("Investing.com から数値を抽出できませんでした。")

        acquired_at = None
        time_match = re.search(r'(Real-time Data|Closed)·([^<\n]+)', text)
        if time_match:
            acquired_at = f"{time_match.group(2).strip()} Investing.com"

        change = None if previous is None else current - previous
        change_pct = None if previous in (None, 0) else (change / previous) * 100

        return MarketRow(
            category="日本国債",
            name=name,
            value=current,
            previous=previous,
            change=change,
            change_pct=change_pct,
            source="Investing.com",
            acquired_at=acquired_at,
            suffix="%",
            note="代替取得",
        )
    except Exception as exc:
        LOGGER.exception("Investing JGB取得失敗: %s", name)
        return MarketRow(
            category="日本国債",
            name=name,
            value=None,
            previous=None,
            change=None,
            change_pct=None,
            source="Investing.com",
            acquired_at=None,
            suffix="%",
            note="代替取得",
            missing_reason=f"Investing取得失敗: {exc}",
        )


def fetch_jgb_rows(session: requests.Session) -> List[MarketRow]:
    primary = parse_mof_jgb_rows(session)
    rows: List[MarketRow] = []

    for name in ("日本国債5年利回り", "日本国債10年利回り", "日本国債30年利回り"):
        row = primary[name]
        if not row.is_missing:
            rows.append(row)
            continue

        fallback = fetch_investing_jgb_row(session, name, INVESTING_JGB_URLS[name])
        if fallback.is_missing:
            combined_reason = row.missing_reason
            if fallback.missing_reason:
                combined_reason = f"{row.missing_reason} / {fallback.missing_reason}"
            row.missing_reason = combined_reason
            rows.append(row)
        else:
            rows.append(fallback)

    return rows


def normalize_news_title(title: str) -> str:
    cleaned = html.unescape(title).strip()
    cleaned = NOISE_SUFFIX_PATTERN.sub("", cleaned)
    return cleaned.strip()


def is_japanese_title(title: str) -> bool:
    return bool(title and JAPANESE_CHAR_PATTERN.search(title))


def fetch_news_items(session: requests.Session, url: str, limit: int = 10) -> List[dict]:
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        items: List[dict] = []
        seen_titles = set()

        for item in root.findall(".//item"):
            raw_title = (item.findtext("title") or "").strip()
            title = normalize_news_title(raw_title)
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()

            if not title or not link:
                continue
            if not is_japanese_title(title):
                continue
            if title in seen_titles:
                continue

            seen_titles.add(title)
            items.append({"title": title, "link": link, "pub_date": pub_date})

            if len(items) >= limit:
                break

        if items:
            return items

        return [{"title": "日本語ニュースを取得できませんでした。", "link": "", "pub_date": ""}]

    except Exception as exc:
        LOGGER.exception("ニュース取得失敗: %s", url)
        return [{"title": f"日本語ニュース取得失敗: {exc}", "link": "", "pub_date": ""}]


def fetch_all_data() -> Dict[str, List[MarketRow]]:
    session = requests_session()
    results: Dict[str, List[MarketRow]] = {category: [] for category in CATEGORY_ORDER}

    for category in ("株式", "為替", "米国債", "商品", "暗号資産"):
        for spec in YF_ITEMS[category]:
            results[category].append(fetch_yahoo_row(category, spec))

    results["日本国債"] = fetch_jgb_rows(session)
    return results


def unique_rows(results: Dict[str, List[MarketRow]]) -> Dict[str, List[MarketRow]]:
    deduped: Dict[str, List[MarketRow]] = {}
    seen_names = set()

    for category in CATEGORY_ORDER:
        deduped[category] = []
        for row in results.get(category, []):
            if row.name in seen_names:
                continue
            seen_names.add(row.name)
            deduped[category].append(row)

    return deduped


def format_value(row: MarketRow) -> str:
    if row.value is None:
        return "未取得"

    decimals = 2
    if row.suffix == "%":
        decimals = 3
    if "円" in row.name and "国債" not in row.name:
        decimals = 3
    if row.name in {"ユーロドル", "ドルインデックス"}:
        decimals = 4

    return f"{row.value:,.{decimals}f}{row.suffix}"


def format_change(row: MarketRow) -> str:
    if row.change is None:
        return "未確認"

    sign = "+" if row.change >= 0 else ""
    decimals = 2 if row.suffix != "%" else 3
    return f"{sign}{row.change:,.{decimals}f}{row.suffix}"


def format_change_pct(row: MarketRow) -> str:
    if row.change_pct is None:
        return "未確認"

    sign = "+" if row.change_pct >= 0 else ""
    return f"{sign}{row.change_pct:.2f}%"


def summarize_direction(row: Optional[MarketRow], threshold: float = 0.15) -> str:
    if row is None or row.change_pct is None:
        return "未取得"
    if row.change_pct > threshold:
        return "上昇"
    if row.change_pct < -threshold:
        return "下落"
    return "横ばい"


def pick_row(results: Dict[str, List[MarketRow]], name: str) -> Optional[MarketRow]:
    for rows in results.values():
        for row in rows:
            if row.name == name:
                return row
    return None


def build_overview(results: Dict[str, List[MarketRow]]) -> List[str]:
    spx = pick_row(results, "S&P500")
    nasdaq = pick_row(results, "NASDAQ総合")
    dow = pick_row(results, "NYダウ")
    sox = pick_row(results, "SOX")
    vix = pick_row(results, "VIX")
    nikkei = pick_row(results, "日経225")
    topix = pick_row(results, "TOPIX")
    reit = pick_row(results, "東証REIT")
    us10 = pick_row(results, "米国債10年利回り")
    jp10 = pick_row(results, "日本国債10年利回り")
    usd_jpy = pick_row(results, "ドル円")
    eur_usd = pick_row(results, "ユーロドル")
    dxy = pick_row(results, "ドルインデックス")
    aud_jpy = pick_row(results, "豪ドル円")
    mxn_jpy = pick_row(results, "メキシコペソ円")
    gold = pick_row(results, "金")
    oil = pick_row(results, "WTI原油")
    copper = pick_row(results, "銅")
    btc = pick_row(results, "BTC/USD")
    eth = pick_row(results, "ETH/USD")

    lines = []
    lines.append(f"米国株は、NYダウ {format_value(dow)}、S&P500 {format_value(spx)}、NASDAQ総合 {format_value(nasdaq)} をみると、全体として {summarize_direction(spx)} 基調です。")
    lines.append(f"半導体株の代表としてみる SOX は {format_value(sox)} で {summarize_direction(sox)}、ハイテク全体の強弱確認に使えます。")
    lines.append(f"VIX は {format_value(vix)} で {summarize_direction(vix)} です。数値が高いままなら、株式市場の不安心理が残っている可能性があります。")
    lines.append(f"日本株は、日経225 {format_value(nikkei)}、TOPIX {format_value(topix)} の並びでみると、主力株と広範な市場のどちらが強いかを切り分けやすい状態です。")
    lines.append(f"東証REITは {format_value(reit)} です。{reit.note if reit and reit.note else '元指数が未取得の場合は代替取得を表示します。'}")
    lines.append(f"為替は、ドル円 {format_value(usd_jpy)}、ユーロドル {format_value(eur_usd)}、ドルインデックス {format_value(dxy)} を並べると、ドル高 / ドル安の方向を確認しやすいです。")
    lines.append(f"高金利・資源国通貨の参考として、豪ドル円 {format_value(aud_jpy)}、メキシコペソ円 {format_value(mxn_jpy)} を入れています。取得時点差がある場合は表の取得時刻を確認してください。")
    lines.append(f"金利は、米10年 {format_value(us10)} と日本10年 {format_value(jp10)} を基準に、日米の長期金利差と為替の関係を確認できる構成です。")
    lines.append(f"商品は、金 {format_value(gold)}、WTI原油 {format_value(oil)}、銅 {format_value(copper)} を中心に、安全資産・エネルギー・景気敏感の3方向を見ます。")
    lines.append(f"暗号資産は、BTC/USD {format_value(btc)} と ETH/USD {format_value(eth)} を中心に、リスク選好の補助指標として扱います。")
    return lines


def build_category_sections(results: Dict[str, List[MarketRow]]) -> str:
    sections = []

    for category in CATEGORY_ORDER:
        rows = results.get(category, [])
        tr_list = []

        for row in rows:
            tr_list.append(
                f"""
                <tr>
                  <td>{html.escape(row.name)}</td>
                  <td>{html.escape(format_value(row))}</td>
                  <td>{html.escape(format_change(row))}</td>
                  <td>{html.escape(format_change_pct(row))}</td>
                  <td>{html.escape(row.display_source)}</td>
                  <td>{html.escape(row.acquired_at or '未確認')}</td>
                </tr>
                """
            )

        sections.append(
            f"""
            <section class="summary-section summary-category-section">
              <h2>{html.escape(category)}</h2>
              <div class="summary-table-wrap">
                <table class="summary-table">
                  <thead>
                    <tr>
                      <th>項目</th>
                      <th>数値</th>
                      <th>前日比</th>
                      <th>騰落率</th>
                      <th>取得元</th>
                      <th>取得時刻</th>
                    </tr>
                  </thead>
                  <tbody>
                    {''.join(tr_list)}
                  </tbody>
                </table>
              </div>
            </section>
            """
        )

    return "\n".join(sections)


def build_missing_section(results: Dict[str, List[MarketRow]]) -> str:
    rows = []

    for category in CATEGORY_ORDER:
        for row in results.get(category, []):
            if not row.is_missing and not row.note:
                continue

            reason = row.missing_reason or "代替取得"
            alt = "あり" if row.note else "なし"
            note = row.note or ""

            rows.append(
                f"""
                <tr>
                  <td>{html.escape(row.name)}</td>
                  <td>{html.escape(reason)}</td>
                  <td>{html.escape(alt)}</td>
                  <td>{html.escape(note)}</td>
                </tr>
                """
            )

    if not rows:
        rows.append(
            """
            <tr>
              <td>なし</td>
              <td>未取得項目はありません。</td>
              <td>なし</td>
              <td></td>
            </tr>
            """
        )

    return f"""
    <section class="summary-section">
      <h2>未取得項目</h2>
      <div class="summary-table-wrap">
        <table class="summary-table">
          <thead>
            <tr>
              <th>項目名</th>
              <th>未取得理由</th>
              <th>代替取得</th>
              <th>補足</th>
            </tr>
          </thead>
          <tbody>
            {''.join(rows)}
          </tbody>
        </table>
      </div>
    </section>
    """


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


def build_source_list() -> str:
    items = [
        "Yahoo Finance / Yahoo!ファイナンス",
        "JPX",
        "財務省",
        "Investing.com",
        "Reuters日本語",
        "Bloomberg日本語",
    ]
    return "".join(f"<li>{html.escape(item)}</li>" for item in items)


def build_summary_html(results: Dict[str, List[MarketRow]], news_map: Dict[str, List[dict]], generated_at_jst: datetime, generated_at_ny: datetime) -> str:
    overview_lines = build_overview(results)
    overview_html = "".join(f"<li>{html.escape(line)}</li>" for line in overview_lines)
    sections_html = build_category_sections(results)
    missing_html = build_missing_section(results)
    news_html = build_news_sections(news_map)
    sources_html = build_source_list()

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
      <div class="year-range-note">取得時点差あり。取得時刻列を確認してください。</div>
    </div>
    <h1>世界経済サマリー</h1>
    <p class="summary-meta">生成時刻 JST: {html.escape(generated_at_jst.strftime("%Y-%m-%d %H:%M:%S"))} / New York: {html.escape(generated_at_ny.strftime("%Y-%m-%d %H:%M:%S"))}</p>
  </header>

  <section class="summary-section">
    <h2>概況</h2>
    <ul class="summary-bullets">
      {overview_html}
    </ul>
  </section>

  <section class="summary-sections-grid">
    {sections_html}
  </section>

  {missing_html}

  {news_html}

  <section class="summary-section">
    <h2>出典</h2>
    <ul class="summary-news-list">
      {sources_html}
    </ul>
  </section>
</body>
</html>"""


def build_payload(results: Dict[str, List[MarketRow]], news_map: Dict[str, List[dict]], generated_at_jst: datetime, generated_at_ny: datetime) -> dict:
    return {
        "generated_at_jst": generated_at_jst.isoformat(),
        "generated_at_ny": generated_at_ny.isoformat(),
        "results": {
            category: [
                {
                    "name": row.name,
                    "value": row.value,
                    "previous": row.previous,
                    "change": row.change,
                    "change_pct": row.change_pct,
                    "source": row.source,
                    "display_source": row.display_source,
                    "acquired_at": row.acquired_at,
                    "suffix": row.suffix,
                    "note": row.note,
                    "missing_reason": row.missing_reason,
                }
                for row in rows
            ]
            for category, rows in results.items()
        },
        "news": news_map,
    }


def write_outputs(html_text: str, data: dict, current_date: str) -> None:
    SUMMARY_DIR.mkdir(parents=True, exist_ok=True)

    for path in (SUMMARY_DIR / "latest.html", SUMMARY_DIR / f"{current_date}.html"):
        path.write_text(html_text, encoding="utf-8")

    for path in (SUMMARY_DIR / "latest.json", SUMMARY_DIR / f"{current_date}.json"):
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    setup_logging()
    force = "--force" in sys.argv

    if not should_run_now(force):
        LOGGER.info("NY 16:10 条件外のためスキップします。")
        return 0

    session = requests_session()
    results = unique_rows(fetch_all_data())
    news_map = {publisher: fetch_news_items(session, url) for publisher, url in NEWS_SOURCES.items()}

    now_jst = datetime.now(JST)
    now_ny = now_jst.astimezone(NY)
    html_text = build_summary_html(results, news_map, now_jst, now_ny)
    payload = build_payload(results, news_map, now_jst, now_ny)
    write_outputs(html_text, payload, now_jst.strftime("%Y-%m-%d"))
    LOGGER.info("世界経済サマリー生成完了: %s", SUMMARY_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
