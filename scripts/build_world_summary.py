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
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import requests
import yfinance as yf


LOGGER = logging.getLogger(__name__)

JST = ZoneInfo("Asia/Tokyo")
NY = ZoneInfo("America/New_York")

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
DIST_DIR = ROOT_DIR / "dist"
SUMMARY_DIR = DIST_DIR / "summary"

FAVICON_CANDIDATES = [
    ("favicon.svg", "image/svg+xml"),
    ("favicon.ico", "image/x-icon"),
    ("favicon.png", "image/png"),
]

NEWS_SOURCES = {
    "Reuters日本語": [
        (
            "https://news.google.com/rss/search?"
            "q=site:jp.reuters.com%20"
            "(市場%20OR%20経済%20OR%20株式%20OR%20債券%20OR%20原油%20OR%20為替%20OR%20金)%20when:1d"
            "&hl=ja&gl=JP&ceid=JP:ja"
        ),
    ],
    "Bloomberg日本語": [
        (
            "https://news.google.com/rss/search?"
            "q=site:bloomberg.com/jp/news/articles%20when:3d"
            "&hl=ja&gl=JP&ceid=JP:ja"
        ),
        (
            "https://news.google.com/rss/search?"
            "q=site:bloomberg.co.jp/news/articles%20when:3d"
            "&hl=ja&gl=JP&ceid=JP:ja"
        ),
        (
            "https://news.google.com/rss/search?"
            "q=(site:bloomberg.com/jp/news/articles%20OR%20site:bloomberg.co.jp/news/articles)%20"
            "(市場%20OR%20経済%20OR%20株式%20OR%20債券%20OR%20原油%20OR%20為替%20OR%20金%20OR%20政策)%20when:7d"
            "&hl=ja&gl=JP&ceid=JP:ja"
        ),
        (
            "https://news.google.com/rss/search?"
            "q=site:bloomberg.com/jp/latest%20when:2d"
            "&hl=ja&gl=JP&ceid=JP:ja"
        ),
    ],
}

JAPANESE_CHAR_PATTERN = re.compile(r"[ぁ-んァ-ヶ一-龠々ー]")
NOISE_SUFFIX_PATTERN = re.compile(r"\s*[-|｜]\s*(Reuters|Bloomberg|ロイター|ブルームバーグ).*$", re.IGNORECASE)
NEWS_TITLE_EXCLUDE_PATTERN = re.compile(
    r"(?:"
    r"\bStock\s+Price\s+Quote\b|"
    r"\bQuote\s*[-:]|"
    r"\bQuote\b.*\b(?:Index|Fund|ETF|OTC|NYSE|NASDAQ|New\s+York|Tokyo)\b|"
    r"\b(?:ETF|Fund)\b.*\bQuote\b"
    r")",
    re.IGNORECASE,
)


BLOOMBERG_ALLOWED_LINK_PATTERN = re.compile(
    r"https?://(?:www\.)?(?:"
    r"bloomberg\.co\.jp/news/|"
    r"bloomberg\.com/jp(?:/news/|/latest)?|"
    r"news\.google\.com/(?:rss/)?articles/"
    r")",
    re.IGNORECASE,
)

BLOOMBERG_JP_FALLBACK_URLS = [
    "https://www.bloomberg.com/jp",
    "https://www.bloomberg.co.jp/",
]
BLOOMBERG_JP_ARTICLE_PATH_PATTERN = re.compile(
    r"(?:href=|data-url=|content=)[\"']"
    r"(?P<link>(?:https?://(?:www\.)?bloomberg\.(?:co\.jp|com)/jp/news/articles/[^\"'#? ]+(?:\?[^\"'# ]*)?|"
    r"https?://(?:www\.)?bloomberg\.co\.jp/news/articles/[^\"'#? ]+(?:\?[^\"'# ]*)?|"
    r"/jp/news/articles/[^\"'#? ]+(?:\?[^\"'# ]*)?|"
    r"/news/articles/[^\"'#? ]+(?:\?[^\"'# ]*)?))"
    r"[\"']"
    r"[^>]*>(?P<title>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
BLOOMBERG_JP_OG_URL_PATTERN = re.compile(
    r"<meta[^>]+property=[\"']og:url[\"'][^>]+content=[\"']([^\"']+)[\"']",
    re.IGNORECASE,
)

YAHOO_TOPIX_URLS = [
    "https://finance.yahoo.co.jp/quote/998405.T",
    "https://finance.yahoo.co.jp/quote/998405.T/",
    "https://finance.yahoo.co.jp/quote/998405.T/history",
]
JPX_REALVALUES_URLS = [
    "https://www.jpx.co.jp/markets/indices/realvalues/index.html",
    "https://www.jpx.co.jp/markets/indices/realvalues/01.html",
]
JPX_TOPIX_QUOTE_URLS = [
    "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=real_index&qcode=151",
    "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=real_index&mode=D&qcode=151",
    "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_real_index&qcode=151",
]
JPX_REIT_QUOTE_URLS = [
    "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=real_index&qcode=155",
    "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=real_index&mode=D&qcode=155",
    "https://quote.jpx.co.jp/jpxhp/main/index.aspx?F=e_real_index&qcode=155",
]
JPX_INDEX_JSON_URL = "https://www.jpx.co.jp/market/indices/indices_stock_price3.txt"
INVESTING_TOPIX_URLS = [
    "https://www.investing.com/indices/topix",
    "https://jp.investing.com/indices/topix",
]
INVESTING_TOPIX_HISTORICAL_URLS = [
    "https://jp.investing.com/indices/topix-historical-data",
    "https://www.investing.com/indices/topix-historical-data",
]
INVESTING_REIT_URLS = [
    "https://jp.investing.com/indices/topix-reit-market",
    "https://www.investing.com/indices/topix-reit-market",
]
INVESTING_REIT_HISTORICAL_URLS = [
    "https://jp.investing.com/indices/topix-reit-market-historical-data",
    "https://www.investing.com/indices/topix-reit-market-historical-data",
]
MOF_JGB_CSV_URLS = [
    "https://www.mof.go.jp/jgbs/reference/interest_rate/jgbcm.csv",
    "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/historical/jgbcme_all.csv",
]
INVESTING_JGB_URLS = {
    "日本国債2年利回り": "https://jp.investing.com/rates-bonds/japan-2-year-bond-yield",
    "日本国債5年利回り": "https://www.investing.com/rates-bonds/japan-5-year-bond-yield",
    "日本国債10年利回り": "https://www.investing.com/rates-bonds/japan-10-year-bond-yield",
    "日本国債30年利回り": "https://www.investing.com/rates-bonds/japan-30-year-bond-yield",
}
INVESTING_US_BOND_URLS = {
    "米国債2年利回り": "https://www.investing.com/rates-bonds/u.s.-2-year-bond-yield",
}

YF_ITEMS = {
    "株式": [
        {"name": "NYダウ", "symbol": "^DJI", "source": "Yahoo Finance"},
        {"name": "NASDAQ総合", "symbol": "^IXIC", "source": "Yahoo Finance"},
        {"name": "S&P500", "symbol": "^GSPC", "source": "Yahoo Finance"},
        {"name": "SOX", "symbol": "^SOX", "source": "Yahoo Finance"},
        {"name": "VIX", "symbol": "^VIX", "source": "Yahoo Finance"},
        {"name": "日経225", "symbol": "^N225", "source": "Yahoo Finance"},
        {"name": "TOPIX", "symbol": "998405.T", "source": "Yahoo!ファイナンス", "custom_method": "yahoo_topix_page"},
        {"name": "J-REIT", "symbol": "TREIT", "source": "JPX", "custom_method": "jpx_reit_page"},
    ],
    "為替": [
        {"name": "ドルインデックス", "symbol": "DX-Y.NYB", "source": "Yahoo Finance"},
    ],
    "米国債": [
        {"name": "米国債2年利回り", "symbol": "US2YT=X", "source": "Investing.com", "suffix": "%", "custom_method": "investing_us_bond_2y"},
        {"name": "米国債5年利回り", "symbol": "^FVX", "source": "Yahoo Finance", "suffix": "%"},
        {"name": "米国債10年利回り", "symbol": "^TNX", "source": "Yahoo Finance", "suffix": "%"},
        {"name": "米国債30年利回り", "symbol": "^TYX", "source": "Yahoo Finance", "suffix": "%"},
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

YAHOO_FX_PAGE_URL = "https://finance.yahoo.co.jp/fx"
YAHOO_FX_BATCH_SIZE = 80
YAHOO_FX_CURRENCY_NAME_CODE_PAIRS = [
    ("UAE ディルハム", "AED"),
    ("オーストラリア ドル", "AUD"),
    ("ブラジル レアル", "BRL"),
    ("カナダ ドル", "CAD"),
    ("スイス フラン", "CHF"),
    ("チリ ペソ", "CLP"),
    ("中国 元", "CNY"),
    ("コロンビア ペソ", "COP"),
    ("デンマーク クローネ", "DKK"),
    ("エジプト ポンド", "EGP"),
    ("欧州 ユーロ", "EUR"),
    ("イギリス ポンド", "GBP"),
    ("香港 ドル", "HKD"),
    ("インドネシア ルピア", "IDR"),
    ("インド ルピー", "INR"),
    ("ヨルダン ディナール", "JOD"),
    ("日本 円", "JPY"),
    ("韓国 ウォン", "KRW"),
    ("クウェート ディナール", "KWD"),
    ("レバノン ポンド", "LBP"),
    ("メキシコ ペソ", "MXN"),
    ("マレーシア リンギット", "MYR"),
    ("ノルウェー クローネ", "NOK"),
    ("ニュージーランド ドル", "NZD"),
    ("ペルー ソル", "PEN"),
    ("フィリピン ペソ", "PHP"),
    ("パラグアイ グァラニ", "PYG"),
    ("ルーマニア レウ", "RON"),
    ("ロシア ルーブル", "RUB"),
    ("サウジアラビア リヤル", "SAR"),
    ("スウェーデン クローナ", "SEK"),
    ("シンガポール ドル", "SGD"),
    ("タイ バーツ", "THB"),
    ("トルコ リラ", "TRY"),
    ("台湾 ドル", "TWD"),
    ("アメリカ ドル", "USD"),
    ("ベネズエラ ボリバル・ソベラノ", "VES"),
    ("南アフリカ ランド", "ZAR"),
]
YAHOO_FX_PRIORITY_PAIRS = [
    "USD/JPY",
    "AUD/JPY",
    "GBP/JPY",
    "EUR/JPY",
    "NZD/JPY",
    "ZAR/JPY",
    "CAD/JPY",
    "CHF/JPY",
    "EUR/USD",
    "GBP/USD",
    "AUD/USD",
    "NZD/USD",
    "EUR/AUD",
    "EUR/GBP",
    "USD/CHF",
    "GBP/CHF",
    "EUR/CHF",
]


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
        return f"{self.source}（{self.note}）" if self.note else self.source


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def should_run_now(force: bool) -> bool:
    if force:
        return True

    now_ny = datetime.now(NY)

    if now_ny.hour != 16:
        return False

    if not (10 <= now_ny.minute < 20):
        return False

    return True


def requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        }
    )
    return session


def build_favicon_links() -> str:
    lines = []
    for filename, mime_type in FAVICON_CANDIDATES:
        if (SRC_DIR / filename).exists():
            lines.append(f'  <link rel="icon" href="../{html.escape(filename)}" type="{html.escape(mime_type)}">')
    return "\n".join(lines)


def decode_response_content(response: requests.Response) -> str:
    for encoding in ("utf-8-sig", "cp932", response.encoding or "utf-8"):
        try:
            return response.content.decode(encoding)
        except Exception:
            continue
    return response.text


def strip_html_tags(raw_html: str) -> str:
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html, flags=re.IGNORECASE)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace(" ", " ")
    return re.sub(r"\s+", " ", text).strip()


def parse_decimal(text: Optional[str]) -> Optional[float]:
    if text is None:
        return None
    normalized = text.replace(",", "").replace("＋", "+").replace("−", "-").strip()
    if not normalized:
        return None
    try:
        return float(normalized)
    except Exception:
        return None


def extract_by_patterns(text: str, patterns: List[str]) -> Optional[str]:
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)
    return None


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

        return MarketRow(category, name, current, previous, change, change_pct, source, acquired_at, suffix, note)
    except Exception as exc:
        LOGGER.exception("Yahoo取得失敗: %s", symbol)
        return MarketRow(category, name, None, None, None, None, source, None, suffix, note, f"Yahoo取得失敗: {exc}")


def chunked(items: List[dict], size: int) -> List[List[dict]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def extract_close_series_from_download(hist, symbol: str):
    if hist is None or getattr(hist, "empty", True):
        return None

    columns = getattr(hist, "columns", None)
    if columns is None:
        return None

    try:
        if getattr(columns, "nlevels", 1) >= 2:
            level0 = set(columns.get_level_values(0))
            level1 = set(columns.get_level_values(1))
            if symbol in level0 and "Close" in level1:
                series = hist[symbol]["Close"]
            elif symbol in level1 and "Close" in level0:
                series = hist["Close"][symbol]
            else:
                return None
        else:
            if "Close" not in columns:
                return None
            series = hist["Close"]

        if hasattr(series, "dropna"):
            series = series.dropna()
        return series if len(series) else None
    except Exception:
        return None


def build_market_row_from_close_series(category: str, spec: dict, close_series) -> MarketRow:
    current = float(close_series.iloc[-1])
    previous = float(close_series.iloc[-2]) if len(close_series) >= 2 else None
    change = None if previous is None else current - previous
    change_pct = None if previous in (None, 0) else (change / previous) * 100

    acquired_at = None
    idx = close_series.index[-1]
    if hasattr(idx, "tz_convert"):
        try:
            acquired_at = idx.tz_convert(JST).strftime("%Y-%m-%d %H:%M:%S JST")
        except Exception:
            acquired_at = idx.strftime("%Y-%m-%d")
    else:
        acquired_at = str(idx)

    return MarketRow(
        category=category,
        name=spec["name"],
        value=current,
        previous=previous,
        change=change,
        change_pct=change_pct,
        source=spec["source"],
        acquired_at=acquired_at,
        suffix=spec.get("suffix", ""),
        note=spec.get("note", ""),
    )


def fetch_yahoo_rows_bulk(category: str, specs: List[dict], allow_individual_fallback: bool = False) -> List[MarketRow]:
    rows: List[MarketRow] = []

    for batch_specs in chunked(specs, YAHOO_FX_BATCH_SIZE):
        symbols = [spec["symbol"] for spec in batch_specs]
        hist = None
        batch_error = ""

        try:
            hist = yf.download(
                tickers=" ".join(symbols),
                period="7d",
                interval="1d",
                auto_adjust=False,
                actions=False,
                group_by="ticker",
                progress=False,
                threads=True,
            )
        except Exception as exc:
            LOGGER.exception("Yahoo一括取得失敗: %s", ", ".join(symbols))
            batch_error = f"Yahoo一括取得失敗: {exc}"

        for spec in batch_specs:
            close_series = extract_close_series_from_download(hist, spec["symbol"])
            if close_series is not None:
                rows.append(build_market_row_from_close_series(category, spec, close_series))
                continue

            if allow_individual_fallback:
                try:
                    rows.append(fetch_yahoo_row(category, spec))
                    continue
                except Exception as exc:
                    rows.append(
                        MarketRow(
                            category=category,
                            name=spec["name"],
                            value=None,
                            previous=None,
                            change=None,
                            change_pct=None,
                            source=spec["source"],
                            acquired_at=None,
                            suffix=spec.get("suffix", ""),
                            note=spec.get("note", ""),
                            missing_reason=batch_error or f"Yahoo取得失敗: {exc}",
                        )
                    )
                    continue

            rows.append(
                MarketRow(
                    category=category,
                    name=spec["name"],
                    value=None,
                    previous=None,
                    change=None,
                    change_pct=None,
                    source=spec["source"],
                    acquired_at=None,
                    suffix=spec.get("suffix", ""),
                    note=spec.get("note", ""),
                    missing_reason=batch_error or "Yahoo一括取得でデータを確認できませんでした。",
                )
            )

    return rows


def extract_supported_yahoo_fx_currency_codes(session: requests.Session) -> List[str]:
    try:
        response = session.get(YAHOO_FX_PAGE_URL, timeout=30)
        response.raise_for_status()
        text = strip_html_tags(decode_response_content(response))
        calculator_block = extract_by_patterns(
            text,
            [r"為替レート計算\s*(.*?)\s*を\s*.*?\s*計算\s*FXチャート・レート"],
        )
        source_text = calculator_block or text

        detected = []
        for currency_name, currency_code in YAHOO_FX_CURRENCY_NAME_CODE_PAIRS:
            position = source_text.find(currency_name)
            if position >= 0:
                detected.append((position, currency_code))

        detected.sort(key=lambda item: item[0])
        codes = [currency_code for _, currency_code in detected]
        if codes:
            return codes
    except Exception as exc:
        LOGGER.exception("Yahoo!ファイナンス FX通貨一覧取得失敗")
        LOGGER.warning("Yahoo!ファイナンス FX通貨一覧取得失敗のため固定一覧へフォールバックします: %s", exc)

    return [currency_code for _, currency_code in YAHOO_FX_CURRENCY_NAME_CODE_PAIRS]


def build_all_yahoo_fx_pair_specs(currency_codes: List[str]) -> List[dict]:
    priority_map = {pair_name: index for index, pair_name in enumerate(YAHOO_FX_PRIORITY_PAIRS)}
    specs: List[dict] = []

    for base_code in currency_codes:
        for quote_code in currency_codes:
            if base_code == quote_code:
                continue
            pair_name = f"{base_code}/{quote_code}"
            specs.append(
                {
                    "name": pair_name,
                    "symbol": f"{base_code}{quote_code}=X",
                    "source": "Yahoo!ファイナンス",
                    "sort_key": (0 if pair_name in priority_map else 1, priority_map.get(pair_name, 9999), pair_name),
                }
            )

    specs.sort(key=lambda spec: spec["sort_key"])
    for spec in specs:
        spec.pop("sort_key", None)
    return specs


def build_fallback_forex_specs() -> List[dict]:
    return [
        {"name": "USD/JPY", "symbol": "USDJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "AUD/JPY", "symbol": "AUDJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "GBP/JPY", "symbol": "GBPJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "EUR/JPY", "symbol": "EURJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "NZD/JPY", "symbol": "NZDJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "ZAR/JPY", "symbol": "ZARJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "CAD/JPY", "symbol": "CADJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "CHF/JPY", "symbol": "CHFJPY=X", "source": "Yahoo!ファイナンス"},
        {"name": "EUR/USD", "symbol": "EURUSD=X", "source": "Yahoo!ファイナンス"},
        {"name": "GBP/USD", "symbol": "GBPUSD=X", "source": "Yahoo!ファイナンス"},
        {"name": "AUD/USD", "symbol": "AUDUSD=X", "source": "Yahoo!ファイナンス"},
        {"name": "NZD/USD", "symbol": "NZDUSD=X", "source": "Yahoo!ファイナンス"},
        {"name": "EUR/AUD", "symbol": "EURAUD=X", "source": "Yahoo!ファイナンス"},
        {"name": "EUR/GBP", "symbol": "EURGBP=X", "source": "Yahoo!ファイナンス"},
        {"name": "USD/CHF", "symbol": "USDCHF=X", "source": "Yahoo!ファイナンス"},
        {"name": "GBP/CHF", "symbol": "GBPCHF=X", "source": "Yahoo!ファイナンス"},
        {"name": "EUR/CHF", "symbol": "EURCHF=X", "source": "Yahoo!ファイナンス"},
    ]


def fetch_forex_rows(session: requests.Session) -> List[MarketRow]:
    rows: List[MarketRow] = []

    for spec in YF_ITEMS["為替"]:
        rows.append(fetch_yahoo_row("為替", spec))

    currency_codes = extract_supported_yahoo_fx_currency_codes(session)
    pair_specs = build_all_yahoo_fx_pair_specs(currency_codes)
    pair_rows = [row for row in fetch_yahoo_rows_bulk("為替", pair_specs, allow_individual_fallback=False) if not row.is_missing]

    if not pair_rows:
        LOGGER.warning("Yahoo!ファイナンスの為替ペア一括取得結果が空のため主要ペアへフォールバックします。")
        pair_rows = [row for row in fetch_yahoo_rows_bulk("為替", build_fallback_forex_specs(), allow_individual_fallback=True) if not row.is_missing]

    rows.extend(pair_rows)
    return rows


def fill_derived_fields(
    current: Optional[float],
    previous: Optional[float],
    change: Optional[float],
    change_pct: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    if previous is None and current is not None and change is not None:
        previous = current - change

    if change is None and current is not None and previous is not None:
        change = current - previous

    if change_pct is None and change is not None and previous not in (None, 0):
        change_pct = (change / previous) * 100

    return previous, change, change_pct


def normalize_bloomberg_article_url(url: str) -> str:
    normalized = html.unescape(url).strip()
    if not normalized:
        return ""
    if normalized.startswith("//"):
        normalized = f"https:{normalized}"
    if normalized.startswith("/jp/"):
        normalized = urljoin("https://www.bloomberg.com", normalized)
    elif normalized.startswith("/news/"):
        normalized = urljoin("https://www.bloomberg.co.jp", normalized)
    normalized = normalized.replace("https://bloomberg.com/", "https://www.bloomberg.com/")
    normalized = normalized.replace("https://bloomberg.co.jp/", "https://www.bloomberg.co.jp/")
    return normalized


def parse_yahoo_topix_history_rows(text: str) -> Optional[tuple[float, Optional[float], Optional[float], Optional[float], Optional[str]]]:
    matches = list(
        re.finditer(
            r"(20\d{2}年\d{1,2}月\d{1,2}日)\s+"
            r"([0-9,]+(?:\.[0-9]+)?)\s+"
            r"([0-9,]+(?:\.[0-9]+)?)\s+"
            r"([0-9,]+(?:\.[0-9]+)?)\s+"
            r"([0-9,]+(?:\.[0-9]+)?)",
            text,
        )
    )
    if not matches:
        return None

    current = parse_decimal(matches[0].group(5))
    previous = parse_decimal(matches[1].group(5)) if len(matches) >= 2 else None
    acquired_at = matches[0].group(1)
    if current is None:
        return None

    change = None if previous is None else current - previous
    change_pct = None if previous in (None, 0) else (change / previous) * 100
    previous, change, change_pct = fill_derived_fields(current, previous, change, change_pct)
    return current, previous, change, change_pct, acquired_at


def parse_yahoo_topix_snapshot(text: str) -> Optional[tuple[float, Optional[float], Optional[float], Optional[float], Optional[str]]]:
    current = parse_decimal(
        extract_by_patterns(
            text,
            [
                r"TOPIX〖998405\.T〗\s*国内指数\s*TOPIX\s*998405\.T\s*([0-9,]+(?:\.[0-9]+)?)\s*前日比",
                r"##\s*TOPIX\s*998405\.T\s*([0-9,]+(?:\.[0-9]+)?)\s*前日比",
                r"TOPIX\s*998405\.T\s*([0-9,]+(?:\.[0-9]+)?)\s*前日比",
                r"TOPIX\s*([0-9,]+(?:\.[0-9]+)?)\s*前日比",
            ],
        )
    )
    previous = parse_decimal(
        extract_by_patterns(
            text,
            [
                r"前日終値\s*([0-9,]+(?:\.[0-9]+)?)\(",
                r"前日終値\s*([0-9,]+(?:\.[0-9]+)?)",
            ],
        )
    )
    change = parse_decimal(
        extract_by_patterns(
            text,
            [
                r"前日比\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\(",
                r"前日比\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)",
            ],
        )
    )
    change_pct = parse_decimal(
        extract_by_patterns(
            text,
            [
                r"前日比\s*[+\-−＋]?[0-9,]+(?:\.[0-9]+)?\(([+\-−＋]?[0-9.]+)%\)",
            ],
        )
    )
    acquired_at = extract_by_patterns(
        text,
        [
            r"リアルタイム株価\s*([0-9]{1,2}:[0-9]{2})",
            r"リアルタイムで表示\s*([0-9]{1,2}:[0-9]{2})",
        ],
    )

    if current is None:
        return None

    previous, change, change_pct = fill_derived_fields(current, previous, change, change_pct)
    return current, previous, change, change_pct, acquired_at


def parse_jpx_list_snapshot(text: str, index_labels: List[str]) -> Optional[tuple[float, Optional[float], Optional[float], Optional[str]]]:
    label_pattern = "(?:" + "|".join(index_labels) + ")"
    patterns = [
        rf"{label_pattern}\s*,\s*([0-9,]+(?:\.[0-9]+)?)\s*,\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\s*,\s*([+\-−＋]?[0-9.]+)\s*%",
        rf"{label_pattern}[\s\S]{{0,200}}?現在値\s*([0-9,]+(?:\.[0-9]+)?)\s*前日比\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\s*([+\-−＋]?[0-9.]+)\s*%",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue
        current = parse_decimal(match.group(1))
        change = parse_decimal(match.group(2))
        change_pct = parse_decimal(match.group(3))
        if current is not None:
            previous, change, change_pct = fill_derived_fields(current, None, change, change_pct)
            return current, previous, change, change_pct, None
    return None


def fetch_topix_from_jpx_realvalues(session: requests.Session) -> MarketRow:
    errors: List[str] = []
    for url in JPX_REALVALUES_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            raw_html = decode_response_content(response)
            stripped = strip_html_tags(raw_html)
            parsed = parse_jpx_list_snapshot(raw_html, [r"TOPIX \\(東証株価指数\\)", r"TOPIX（東証株価指数）", r"TOPIX"]) or parse_jpx_list_snapshot(stripped, [r"TOPIX \\(東証株価指数\\)", r"TOPIX（東証株価指数）", r"TOPIX"])
            if parsed is None:
                errors.append(f"JPX realvalues TOPIX解析失敗: {url}")
                continue

            current, previous, change, change_pct, acquired_at = parsed
            return MarketRow(
                category="株式",
                name="TOPIX",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="JPX",
                acquired_at=acquired_at,
                note="代替取得",
            )
        except Exception as exc:
            LOGGER.exception("JPX realvalues TOPIX取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    return MarketRow(
        category="株式",
        name="TOPIX",
        value=None,
        previous=None,
        change=None,
        change_pct=None,
        source="JPX",
        acquired_at=None,
        note="代替取得",
        missing_reason=" / ".join(errors) if errors else "JPX realvalues から TOPIX を取得できませんでした。",
    )


def fetch_reit_from_jpx_realvalues(session: requests.Session) -> MarketRow:
    errors: List[str] = []
    for url in JPX_REALVALUES_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            raw_html = decode_response_content(response)
            stripped = strip_html_tags(raw_html)
            parsed = parse_jpx_list_snapshot(raw_html, [r"東証REIT指数", r"Tokyo Stock Exchange REIT Index"]) or parse_jpx_list_snapshot(stripped, [r"東証REIT指数", r"Tokyo Stock Exchange REIT Index"])
            if parsed is None:
                errors.append(f"JPX realvalues REIT解析失敗: {url}")
                continue

            current, previous, change, change_pct, acquired_at = parsed
            return MarketRow(
                category="株式",
                name="J-REIT",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="JPX",
                acquired_at=acquired_at,
                note="代替取得",
            )
        except Exception as exc:
            LOGGER.exception("JPX realvalues REIT取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    return MarketRow(
        category="株式",
        name="J-REIT",
        value=None,
        previous=None,
        change=None,
        change_pct=None,
        source="JPX",
        acquired_at=None,
        note="代替取得",
        missing_reason=" / ".join(errors) if errors else "JPX realvalues から J-REIT を取得できませんでした。",
    )


def supplement_market_row(base: MarketRow, supplement: MarketRow, supplement_note: str) -> MarketRow:
    if supplement.is_missing:
        if supplement.missing_reason:
            if base.missing_reason:
                base.missing_reason = f"{base.missing_reason} / {supplement.missing_reason}"
            else:
                base.missing_reason = supplement.missing_reason
        return base

    if base.is_missing:
        supplement.note = supplement_note if not supplement.note else f"{supplement.note} / {supplement_note}"
        return supplement

    updated = False
    for attr in ("previous", "change", "change_pct", "acquired_at"):
        if getattr(base, attr) is None and getattr(supplement, attr) is not None:
            setattr(base, attr, getattr(supplement, attr))
            updated = True

    if updated:
        base.note = supplement_note if not base.note else f"{base.note} / {supplement_note}"

    base.previous, base.change, base.change_pct = fill_derived_fields(
        base.value,
        base.previous,
        base.change,
        base.change_pct,
    )

    return base


def parse_jpx_quote_snapshot(raw_html: str, index_labels: List[str]) -> Optional[tuple[float, float, float, Optional[str]]]:
    compact_html = re.sub(r"\s+", " ", raw_html)
    label_pattern = "(?:" + "|".join(index_labels) + ")"
    patterns = [
        rf"{label_pattern}[\s\S]{{0,1200}}?(\d{{4}}/\d{{2}}/\d{{2}}),\s*([0-9,]+(?:\.[0-9]+)?)\s*\(([^)]+)\),\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\s*\(([+\-−＋]?[0-9.]+)%\)",
        rf"{label_pattern}[\s\S]{{0,1200}}?Date\s*Recent\s*Change\s*Open\s*High\s*Low[\s\S]{{0,400}}?(\d{{4}}/\d{{2}}/\d{{2}}),\s*([0-9,]+(?:\.[0-9]+)?)\s*\(([^)]+)\),\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\s*\(([+\-−＋]?[0-9.]+)%\)",
    ]

    for pattern in patterns:
        match = re.search(pattern, compact_html, re.IGNORECASE)
        if not match:
            continue

        current = parse_decimal(match.group(2))
        change = parse_decimal(match.group(4))
        change_pct = parse_decimal(match.group(5))
        acquired_at = f"{match.group(1)} {match.group(3)}"
        if current is not None and change is not None:
            return current, change, change_pct, acquired_at

    return None


def parse_investing_snapshot(
    text: str,
    name_patterns: List[str],
    watchlist_patterns: List[str],
    previous_patterns: List[str],
) -> Optional[tuple[float, Optional[float], float, float, Optional[str]]]:
    for name_pattern in name_patterns:
        for watchlist_pattern in watchlist_patterns:
            match = re.search(
                rf"{name_pattern}[\s\S]{{0,500}}?{watchlist_pattern}\s*([0-9,]+(?:\.[0-9]+)?)\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\(([+\-−＋]?[0-9.]+)%\)\s*(?:Closed|終了)[·・]?\s*([0-9]{{1,2}}/[0-9]{{2}})?",
                text,
                re.IGNORECASE,
            )
            if match:
                current = parse_decimal(match.group(1))
                change = parse_decimal(match.group(2))
                change_pct = parse_decimal(match.group(3))
                acquired_at = match.group(4)
                previous = parse_decimal(extract_by_patterns(text, previous_patterns))
                if current is None or change is None or change_pct is None:
                    continue
                previous, change, change_pct = fill_derived_fields(current, previous, change, change_pct)
                return current, previous, change, change_pct, acquired_at

    return None

def parse_investing_historical_latest_rows(
    text: str,
    section_markers: List[str],
) -> Optional[tuple[float, Optional[float], Optional[float], Optional[float], Optional[str]]]:
    target_text = text
    for marker in section_markers:
        position = text.find(marker)
        if position >= 0:
            target_text = text[position:]
            break

    date_patterns = [
        r"\d{4}年\d{1,2}月\d{1,2}日",
        r"\d{4}/\d{1,2}/\d{1,2}",
        r"[A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}",
    ]
    date_pattern = "(?:" + "|".join(date_patterns) + ")"
    number_pattern = r"[+\-−＋]?[0-9,]+(?:\.[0-9]+)?"
    volume_pattern = r"(?:[0-9.,]+[KMBT]?|-)"

    row_pattern = (
        rf"({date_pattern})"
        rf"\s+({number_pattern})"
        rf"\s+({number_pattern})"
        rf"\s+({number_pattern})"
        rf"\s+({number_pattern})"
        rf"(?:\s+({volume_pattern}))?"
        rf"\s*([+\-−＋]?[0-9.]+)%"
    )

    matches = list(re.finditer(row_pattern, target_text, re.IGNORECASE))
    if not matches:
        return None

    current = parse_decimal(matches[0].group(2))
    change_pct = parse_decimal(matches[0].group(7))
    previous = parse_decimal(matches[1].group(2)) if len(matches) >= 2 else None
    acquired_at = matches[0].group(1)
    if current is None:
        return None

    change = None if previous is None else current - previous
    previous, change, change_pct = fill_derived_fields(current, previous, change, change_pct)
    return current, previous, change, change_pct, acquired_at


def fetch_investing_bond_row(
    session: requests.Session,
    category: str,
    name: str,
    url: str,
    jp_mode: bool = False,
) -> MarketRow:
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        text = strip_html_tags(decode_response_content(response))

        current = extract_by_patterns(
            text,
            [
                r"Add to Watchlist\s*([0-9,]+(?:\.[0-9]+)?)\s*[+\-−＋]?[0-9,]+(?:\.[0-9]+)?\s*\(",
                r"ウォッチリストに加える\s*([0-9,]+(?:\.[0-9]+)?)\s*[+\-−＋]?[0-9,]+(?:\.[0-9]+)?\s*\(",
            ],
        )
        previous = extract_by_patterns(
            text,
            [
                r"Prev\. Close\s*([0-9,]+(?:\.[0-9]+)?)",
                r"前日終値\s*([0-9,]+(?:\.[0-9]+)?)",
            ],
        )
        change = extract_by_patterns(
            text,
            [
                r"Add to Watchlist\s*[0-9,]+(?:\.[0-9]+)?\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\(",
                r"ウォッチリストに加える\s*[0-9,]+(?:\.[0-9]+)?\s*([+\-−＋]?[0-9,]+(?:\.[0-9]+)?)\(",
            ],
        )
        change_pct = extract_by_patterns(
            text,
            [
                r"Add to Watchlist\s*[0-9,]+(?:\.[0-9]+)?\s*[+\-−＋]?[0-9,]+(?:\.[0-9]+)?\(([+\-−＋]?[0-9.]+)%\)",
                r"ウォッチリストに加える\s*[0-9,]+(?:\.[0-9]+)?\s*[+\-−＋]?[0-9,]+(?:\.[0-9]+)?\(([+\-−＋]?[0-9.]+)%\)",
            ],
        )
        acquired_at = extract_by_patterns(
            text,
            [
                r"(?:Closed|終了)[·・]?\s*([0-9]{1,2}/[0-9]{2}(?:/[0-9]{2,4})?|[0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)",
            ],
        )

        current_value = parse_decimal(current)
        previous_value = parse_decimal(previous)
        change_value = parse_decimal(change)
        change_pct_value = parse_decimal(change_pct)
        if current_value is None:
            raise ValueError("Investing.comから現在値を抽出できませんでした。")

        previous_value, change_value, change_pct_value = fill_derived_fields(
            current_value,
            previous_value,
            change_value,
            change_pct_value,
        )

        return MarketRow(
            category=category,
            name=name,
            value=current_value,
            previous=previous_value,
            change=change_value,
            change_pct=change_pct_value,
            source="Investing.com",
            acquired_at=acquired_at,
            suffix="%",
            note="代替取得" if jp_mode else "",
        )
    except Exception as exc:
        LOGGER.exception("Investing bond取得失敗: %s", name)
        return MarketRow(
            category=category,
            name=name,
            value=None,
            previous=None,
            change=None,
            change_pct=None,
            source="Investing.com",
            acquired_at=None,
            suffix="%",
            note="代替取得" if jp_mode else "",
            missing_reason=f"Investing取得失敗: {exc}",
        )


def fetch_us_2y_from_investing(session: requests.Session) -> MarketRow:
    row = fetch_investing_bond_row(
        session=session,
        category="米国債",
        name="米国債2年利回り",
        url=INVESTING_US_BOND_URLS["米国債2年利回り"],
        jp_mode=False,
    )
    if not row.is_missing:
        return row

    # Investing.com失敗時のYahoo Financeフォールバック
    yf_row = fetch_yahoo_row("米国債", {"name": "米国債2年利回り", "symbol": "2YY=F", "source": "Yahoo Finance", "suffix": "%"})
    if not yf_row.is_missing:
        yf_row.note = "Investing.com失敗時の代替取得"
        return yf_row

    return row


def fetch_topix_from_investing_historical(session: requests.Session) -> MarketRow:
    errors: List[str] = []

    for url in INVESTING_TOPIX_HISTORICAL_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            text = strip_html_tags(decode_response_content(response))
            parsed = parse_investing_historical_latest_rows(
                text=text,
                section_markers=["## TOPIX 過去データ", "# TOPIX (TOPX)", "# TOPIX 過去のレート"],
            )
            if parsed is None:
                errors.append(f"Investing.com TOPIX過去データ解析失敗: {url}")
                continue

            current, previous, change, change_pct, acquired_at = parsed
            return MarketRow(
                category="株式",
                name="TOPIX",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="Investing.com",
                acquired_at=acquired_at,
                note="過去データページから取得",
            )
        except Exception as exc:
            LOGGER.exception("Investing TOPIX過去データ取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    return MarketRow(
        category="株式",
        name="TOPIX",
        value=None,
        previous=None,
        change=None,
        change_pct=None,
        source="Investing.com",
        acquired_at=None,
        note="代替取得",
        missing_reason=" / ".join(errors) if errors else "Investing.com から TOPIX 過去データを取得できませんでした。",
    )


def fetch_topix_from_investing(session: requests.Session) -> MarketRow:
    errors: List[str] = []
    for url in INVESTING_TOPIX_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            text = strip_html_tags(decode_response_content(response))
            parsed = parse_investing_snapshot(
                text=text,
                name_patterns=[r"TOPIX\s*\(TOPX\)", r"#\s*TOPIX\s*\(TOPX\)"],
                watchlist_patterns=[r"Add to Watchlist", r"ウォッチリストに加える"],
                previous_patterns=[r"Prev\. Close\s*([0-9,]+(?:\.[0-9]+)?)", r"前日終値\s*([0-9,]+(?:\.[0-9]+)?)"],
            )
            if parsed is None:
                errors.append(f"Investing TOPIX解析失敗: {url}")
                continue

            current, previous, change, change_pct, acquired_at = parsed
            return MarketRow(
                category="株式",
                name="TOPIX",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="Investing.com",
                acquired_at=acquired_at,
                note="代替取得",
            )
        except Exception as exc:
            LOGGER.exception("Investing TOPIX取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    return MarketRow(
        category="株式",
        name="TOPIX",
        value=None,
        previous=None,
        change=None,
        change_pct=None,
        source="Investing.com",
        acquired_at=None,
        note="代替取得",
        missing_reason=" / ".join(errors) if errors else "Investing.com から TOPIX を取得できませんでした。",
    )


def fetch_topix_from_jpx_quote(session: requests.Session) -> MarketRow:
    errors: List[str] = []
    for url in JPX_TOPIX_QUOTE_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            raw_html = decode_response_content(response)
            parsed = parse_jpx_quote_snapshot(raw_html, [r"TOPIX", r"TOPIX \(東証株価指数\)"])
            if parsed is None:
                errors.append(f"JPX TOPIX解析失敗: {url}")
                continue

            current, change, change_pct, acquired_at = parsed
            previous, change, change_pct = fill_derived_fields(current, None, change, change_pct)
            return MarketRow(
                category="株式",
                name="TOPIX",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="JPX",
                acquired_at=acquired_at,
                note="代替取得",
            )
        except Exception as exc:
            LOGGER.exception("JPX TOPIX取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    return MarketRow(
        category="株式",
        name="TOPIX",
        value=None,
        previous=None,
        change=None,
        change_pct=None,
        source="JPX",
        acquired_at=None,
        note="代替取得",
        missing_reason=" / ".join(errors) if errors else "JPX から TOPIX を取得できませんでした。",
    )


def fetch_reit_from_jpx_quote(session: requests.Session) -> MarketRow:
    errors: List[str] = []
    for url in JPX_REIT_QUOTE_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            raw_html = decode_response_content(response)
            parsed = parse_jpx_quote_snapshot(raw_html, [r"東証REIT指数", r"Tokyo Stock Exchange REIT Index"])
            if parsed is None:
                errors.append(f"JPX 東証REIT解析失敗: {url}")
                continue

            current, change, change_pct, acquired_at = parsed
            previous, change, change_pct = fill_derived_fields(current, None, change, change_pct)
            return MarketRow(
                category="株式",
                name="J-REIT",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="JPX",
                acquired_at=acquired_at,
                note="代替取得",
            )
        except Exception as exc:
            LOGGER.exception("JPX 東証REIT取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    return MarketRow(
        category="株式",
        name="J-REIT",
        value=None,
        previous=None,
        change=None,
        change_pct=None,
        source="JPX",
        acquired_at=None,
        note="代替取得",
        missing_reason=" / ".join(errors) if errors else "JPX 個別指数ページから J-REIT を取得できませんでした。",
    )


def fetch_topix_from_yahoo_finance(session: requests.Session) -> MarketRow:
    errors: List[str] = []
    for url in YAHOO_TOPIX_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            raw_html = decode_response_content(response)
            text = strip_html_tags(raw_html)

            parsed = parse_yahoo_topix_snapshot(text)
            if parsed is None:
                parsed = parse_yahoo_topix_history_rows(text)
            if parsed is None:
                raise ValueError("Yahoo!ファイナンスのTOPIXページから現在値を抽出できませんでした。")

            current, previous, change, change_pct, acquired_at = parsed
            row = MarketRow(
                category="株式",
                name="TOPIX",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="Yahoo!ファイナンス",
                acquired_at=acquired_at,
            )

            if row.previous is None or row.change is None or row.change_pct is None:
                row = supplement_market_row(row, fetch_topix_from_investing_historical(session), "一部をInvesting.com過去データで補完")

            if row.previous is None or row.change is None or row.change_pct is None:
                row = supplement_market_row(row, fetch_topix_from_investing(session), "一部をInvesting.com概要ページで補完")

            if row.previous is None or row.change is None or row.change_pct is None:
                row = supplement_market_row(row, fetch_topix_from_jpx_realvalues(session), "一部をJPXリアルタイム指数一覧で補完")

            if row.previous is None or row.change is None or row.change_pct is None:
                row = supplement_market_row(row, fetch_topix_from_jpx_quote(session), "一部をJPX個別指数ページで補完")

            return row
        except Exception as exc:
            LOGGER.exception("TOPIX取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    fallback = fetch_topix_from_investing_historical(session)
    if not fallback.is_missing:
        fallback.note = "Yahoo!ファイナンス失敗時の代替取得" if not fallback.note else f"{fallback.note} / Yahoo!ファイナンス失敗時の代替取得"
        if fallback.previous is None or fallback.change is None or fallback.change_pct is None:
            fallback = supplement_market_row(fallback, fetch_topix_from_investing(session), "Yahoo!ファイナンス失敗時のInvesting.com概要ページ補完")
        if fallback.previous is None or fallback.change is None or fallback.change_pct is None:
            fallback = supplement_market_row(fallback, fetch_topix_from_jpx_realvalues(session), "Yahoo!ファイナンス失敗時のJPXリアルタイム指数一覧補完")
        if fallback.previous is None or fallback.change is None or fallback.change_pct is None:
            fallback = supplement_market_row(fallback, fetch_topix_from_jpx_quote(session), "Yahoo!ファイナンス失敗時のJPX個別指数補完")
        return fallback

    fallback = fetch_topix_from_investing(session)
    if not fallback.is_missing:
        fallback.note = "Yahoo!ファイナンス失敗時の代替取得" if not fallback.note else f"{fallback.note} / Yahoo!ファイナンス失敗時の代替取得"
        if fallback.previous is None or fallback.change is None or fallback.change_pct is None:
            fallback = supplement_market_row(fallback, fetch_topix_from_jpx_realvalues(session), "Yahoo!ファイナンス失敗時のJPXリアルタイム指数一覧補完")
        if fallback.previous is None or fallback.change is None or fallback.change_pct is None:
            fallback = supplement_market_row(fallback, fetch_topix_from_jpx_quote(session), "Yahoo!ファイナンス失敗時のJPX個別指数補完")
        return fallback

    fallback = supplement_market_row(fallback, fetch_topix_from_jpx_realvalues(session), "Yahoo!ファイナンス失敗時の代替取得")
    fallback = supplement_market_row(fallback, fetch_topix_from_jpx_quote(session), "Yahoo!ファイナンス失敗時の代替取得")
    if fallback.missing_reason:
        prefix = " / ".join(errors)
        fallback.missing_reason = f"{prefix} / {fallback.missing_reason}" if prefix else fallback.missing_reason
    return fallback


def fetch_tse_reit_from_investing(session: requests.Session, prior_errors: Optional[List[str]] = None) -> MarketRow:
    errors = list(prior_errors or [])

    for url in INVESTING_REIT_HISTORICAL_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            text = strip_html_tags(decode_response_content(response))
            parsed = parse_investing_historical_latest_rows(
                text=text,
                section_markers=["## 東証REIT指数 過去データ", "# 東証REIT指数 (TREIT)"],
            )
            if parsed is None:
                errors.append(f"Investing.com 東証REIT過去データ解析失敗: {url}")
                continue

            current, previous, change, change_pct, acquired_at = parsed
            return MarketRow(
                category="株式",
                name="J-REIT",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="Investing.com",
                acquired_at=acquired_at,
                note="過去データページから取得",
            )
        except Exception as exc:
            LOGGER.exception("Investing 東証REIT過去データ取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    for url in INVESTING_REIT_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            text = strip_html_tags(decode_response_content(response))
            parsed = parse_investing_snapshot(
                text=text,
                name_patterns=[r"東証REIT指数\s*\(TREIT\)", r"Tokyo Stock Exchange REIT"],
                watchlist_patterns=[r"ウォッチリストに加える", r"Add to Watchlist"],
                previous_patterns=[r"前日終値\s*([0-9,]+(?:\.[0-9]+)?)", r"Prev\. Close\s*([0-9,]+(?:\.[0-9]+)?)"],
            )
            if parsed is None:
                errors.append(f"Investing.com 東証REIT解析失敗: {url}")
                continue

            current, previous, change, change_pct, acquired_at = parsed
            return MarketRow(
                category="株式",
                name="J-REIT",
                value=current,
                previous=previous,
                change=change,
                change_pct=change_pct,
                source="Investing.com",
                acquired_at=acquired_at,
                note="概要ページから取得",
            )
        except Exception as exc:
            LOGGER.exception("Investing 東証REIT取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    return MarketRow(
        category="株式",
        name="J-REIT",
        value=None,
        previous=None,
        change=None,
        change_pct=None,
        source="Investing.com",
        acquired_at=None,
        note="代替取得",
        missing_reason=" / ".join(errors) if errors else "Investing.com から J-REIT を取得できませんでした。",
    )


def fetch_reit_from_jpx_json(session: requests.Session) -> MarketRow:
    try:
        response = session.get(JPX_INDEX_JSON_URL, timeout=30)
        response.raise_for_status()
        data = response.json()
        reit = data.get("MainStockIndex", {}).get("TseReitIndex")
        if reit is None:
            reit = data.get("TseMarketType", {}).get("TseReitIndex")
        if reit is None:
            raise ValueError("TseReitIndex が見つかりません。")

        current = parse_decimal(reit.get("currentPrice"))
        change = parse_decimal(reit.get("previousDayComparison"))
        change_pct = parse_decimal(reit.get("previousDayRatio"))
        if current is None:
            raise ValueError("J-REIT現在値を取得できませんでした。")

        previous, change, change_pct = fill_derived_fields(current, None, change, change_pct)
        return MarketRow(
            category="株式",
            name="J-REIT",
            value=current,
            previous=previous,
            change=change,
            change_pct=change_pct,
            source="JPX",
            acquired_at=None,
            note="JPX JSON APIから取得",
        )
    except Exception as exc:
        LOGGER.exception("JPX JSON REIT取得失敗")
        return MarketRow(
            category="株式",
            name="J-REIT",
            value=None,
            previous=None,
            change=None,
            change_pct=None,
            source="JPX",
            acquired_at=None,
            note="代替取得",
            missing_reason=f"JPX JSON取得失敗: {exc}",
        )


def fetch_tse_reit_from_jpx(session: requests.Session) -> MarketRow:
    investing_row = fetch_tse_reit_from_investing(session)
    if not investing_row.is_missing:
        if investing_row.previous is None or investing_row.change is None or investing_row.change_pct is None:
            investing_row = supplement_market_row(investing_row, fetch_reit_from_jpx_json(session), "一部をJPX JSON APIで補完")
        return investing_row

    errors: List[str] = []

    json_row = fetch_reit_from_jpx_json(session)
    if not json_row.is_missing:
        return json_row
    if json_row.missing_reason:
        errors.append(json_row.missing_reason)

    realvalues_row = fetch_reit_from_jpx_realvalues(session)
    if not realvalues_row.is_missing:
        realvalues_row.note = "Investing.com失敗時の代替取得" if not realvalues_row.note else f"{realvalues_row.note} / Investing.com失敗時の代替取得"
        return realvalues_row
    if realvalues_row.missing_reason:
        errors.append(realvalues_row.missing_reason)

    quote_row = fetch_reit_from_jpx_quote(session)
    if not quote_row.is_missing:
        quote_row.note = "Investing.com失敗時の代替取得" if not quote_row.note else f"{quote_row.note} / Investing.com失敗時の代替取得"
        return quote_row
    if quote_row.missing_reason:
        errors.append(quote_row.missing_reason)

    fallback = fetch_tse_reit_from_investing(session, errors)
    if not fallback.is_missing:
        fallback.note = "JPX取得失敗時の代替取得" if not fallback.note else f"{fallback.note} / JPX取得失敗時の代替取得"
    return fallback

def normalize_header(value: str) -> str:
    return re.sub(r"\s+", "", value).strip().lower()


def find_header_index(header_map: Dict[str, int], aliases: List[str]) -> Optional[int]:
    for alias in aliases:
        key = normalize_header(alias)
        if key in header_map:
            return header_map[key]
    return None


def parse_mof_jgb_rows(session: requests.Session) -> Dict[str, MarketRow]:
    last_error = None

    for url in MOF_JGB_CSV_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            text = decode_response_content(response)
            reader = csv.reader(text.splitlines())
            rows = [row for row in reader if row and any(cell.strip() for cell in row)]
            if not rows:
                raise ValueError("CSVが空です。")

            # タイトル行をスキップし、実際のヘッダー行を検出
            header_row = None
            header_row_idx = None
            date_aliases = ["基準日", "Date"]
            for ri, row in enumerate(rows):
                for ci, cell in enumerate(row):
                    if normalize_header(cell) in {normalize_header(a) for a in date_aliases}:
                        header_row = row
                        header_row_idx = ri
                        break
                if header_row is not None:
                    break
            if header_row is None:
                raise ValueError(f"ヘッダー行を検出できませんでした。 rows[0]={rows[0]}")
            data_rows = rows[header_row_idx + 1:]
            header_map = {normalize_header(cell): idx for idx, cell in enumerate(header_row)}

            date_idx = find_header_index(header_map, ["基準日", "Date"])
            idx_2 = find_header_index(header_map, ["2年", "2Year", "2Y"])
            idx_5 = find_header_index(header_map, ["5年", "5Year", "5Y"])
            idx_10 = find_header_index(header_map, ["10年", "10Year", "10Y"])
            idx_30 = find_header_index(header_map, ["30年", "30Year", "30Y"])

            if date_idx is None or idx_2 is None or idx_5 is None or idx_10 is None or idx_30 is None:
                raise ValueError(f"必要列を検出できませんでした。 header={header_row}")

            latest_values = {"日本国債2年利回り": None, "日本国債5年利回り": None, "日本国債10年利回り": None, "日本国債30年利回り": None}
            previous_values = {"日本国債2年利回り": None, "日本国債5年利回り": None, "日本国債10年利回り": None, "日本国債30年利回り": None}
            latest_dates = {"日本国債2年利回り": None, "日本国債5年利回り": None, "日本国債10年利回り": None, "日本国債30年利回り": None}
            index_map = {
                "日本国債2年利回り": idx_2,
                "日本国債5年利回り": idx_5,
                "日本国債10年利回り": idx_10,
                "日本国債30年利回り": idx_30,
            }

            for row in reversed(data_rows):
                for name, idx in index_map.items():
                    if idx >= len(row):
                        continue
                    value = parse_decimal(row[idx])
                    if value is None:
                        continue
                    if latest_values[name] is None:
                        latest_values[name] = value
                        latest_dates[name] = row[date_idx] if date_idx < len(row) else None
                    elif previous_values[name] is None:
                        previous_values[name] = value

                if all(latest_values[name] is not None for name in latest_values) and all(previous_values[name] is not None for name in previous_values):
                    break

            result: Dict[str, MarketRow] = {}
            for name in ("日本国債2年利回り", "日本国債5年利回り", "日本国債10年利回り", "日本国債30年利回り"):
                current = latest_values[name]
                previous = previous_values[name]
                if current is None:
                    result[name] = MarketRow(
                        category="日本国債",
                        name=name,
                        value=None,
                        previous=None,
                        change=None,
                        change_pct=None,
                        source="財務省",
                        acquired_at=None,
                        suffix="%",
                        missing_reason="財務省CSVの最新営業日データを検出できませんでした。",
                    )
                    continue

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
                    acquired_at=latest_dates[name],
                    suffix="%",
                )
            return result

        except Exception as exc:
            LOGGER.exception("財務省JGB取得失敗: %s", url)
            last_error = exc

    error_message = f"財務省取得失敗: {last_error}" if last_error else "財務省取得失敗"
    return {
        "日本国債2年利回り": MarketRow("日本国債", "日本国債2年利回り", None, None, None, None, "財務省", None, "%", missing_reason=error_message),
        "日本国債5年利回り": MarketRow("日本国債", "日本国債5年利回り", None, None, None, None, "財務省", None, "%", missing_reason=error_message),
        "日本国債10年利回り": MarketRow("日本国債", "日本国債10年利回り", None, None, None, None, "財務省", None, "%", missing_reason=error_message),
        "日本国債30年利回り": MarketRow("日本国債", "日本国債30年利回り", None, None, None, None, "財務省", None, "%", missing_reason=error_message),
    }


def parse_first_float(text: str, pattern: str) -> Optional[float]:
    match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return parse_decimal(match.group(1))


def fetch_investing_jgb_row(session: requests.Session, name: str, url: str) -> MarketRow:
    return fetch_investing_bond_row(
        session=session,
        category="日本国債",
        name=name,
        url=url,
        jp_mode=True,
    )


def fetch_jgb_rows(session: requests.Session) -> List[MarketRow]:
    primary = parse_mof_jgb_rows(session)
    rows: List[MarketRow] = []

    for name in ("日本国債2年利回り", "日本国債5年利回り", "日本国債10年利回り", "日本国債30年利回り"):
        row = primary[name]
        if not row.is_missing:
            rows.append(row)
            continue

        fallback = fetch_investing_jgb_row(session, name, INVESTING_JGB_URLS[name])
        if fallback.is_missing:
            if fallback.missing_reason:
                row.missing_reason = f"{row.missing_reason} / {fallback.missing_reason}"
            rows.append(row)
        else:
            rows.append(fallback)

    return rows


def normalize_news_title(title: str) -> str:
    cleaned = html.unescape(title).strip()
    cleaned = NOISE_SUFFIX_PATTERN.sub("", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned.strip()


def is_japanese_title(title: str) -> bool:
    return bool(title and JAPANESE_CHAR_PATTERN.search(title))


def is_noise_news_title(title: str, publisher: str) -> bool:
    if not title:
        return True

    if publisher == "Bloomberg日本語" and NEWS_TITLE_EXCLUDE_PATTERN.search(title):
        return True

    return False


def is_allowed_news_link(link: str, publisher: str) -> bool:
    if not link:
        return False

    if publisher == "Bloomberg日本語":
        return bool(BLOOMBERG_ALLOWED_LINK_PATTERN.search(link))

    return True


def is_allowed_news_source(source_name: str, publisher: str) -> bool:
    if publisher != "Bloomberg日本語":
        return True

    if not source_name:
        return True

    normalized = html.unescape(source_name).strip().lower()
    return "bloomberg" in normalized or "ブルームバーグ" in normalized


def fetch_bloomberg_jp_homepage_items(session: requests.Session, limit: int = 10) -> List[dict]:
    items: List[dict] = []
    seen_titles = set()

    for url in BLOOMBERG_JP_FALLBACK_URLS:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            raw_html = decode_response_content(response)

            og_url_match = BLOOMBERG_JP_OG_URL_PATTERN.search(raw_html)
            base_url = og_url_match.group(1) if og_url_match else url

            for match in BLOOMBERG_JP_ARTICLE_PATH_PATTERN.finditer(raw_html):
                title = normalize_news_title(strip_html_tags(match.group("title")))
                link = normalize_bloomberg_article_url(urljoin(base_url, match.group("link")))

                if (
                    not title
                    or not link
                    or not is_allowed_news_link(link, "Bloomberg日本語")
                    or not is_japanese_title(title)
                    or is_noise_news_title(title, "Bloomberg日本語")
                    or title in seen_titles
                ):
                    continue

                seen_titles.add(title)
                items.append({
                    "title": title,
                    "link": link,
                    "pub_date": "",
                    "source": "Bloomberg",
                })
                if len(items) >= limit:
                    return items
        except Exception:
            LOGGER.exception("Bloomberg日本語ホーム取得失敗: %s", url)

    return items


def fetch_news_items(session: requests.Session, publisher: str, urls: List[str], limit: int = 10) -> List[dict]:
    items: List[dict] = []
    seen_titles = set()
    errors: List[str] = []

    for url in urls:
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            root = ET.fromstring(response.content)

            for item in root.findall(".//item"):
                raw_title = (item.findtext("title") or "").strip()
                title = normalize_news_title(raw_title)
                link = (item.findtext("link") or "").strip()
                pub_date = (item.findtext("pubDate") or "").strip()
                source_name = (item.findtext("source") or "").strip()

                if publisher == "Bloomberg日本語":
                    link = normalize_bloomberg_article_url(link)

                if (
                    not title
                    or not link
                    or not is_allowed_news_link(link, publisher)
                    or not is_allowed_news_source(source_name, publisher)
                    or not is_japanese_title(title)
                    or is_noise_news_title(title, publisher)
                    or title in seen_titles
                ):
                    continue

                seen_titles.add(title)
                items.append({
                    "title": title,
                    "link": link,
                    "pub_date": pub_date,
                    "source": source_name,
                })
                if len(items) >= limit:
                    return items
        except Exception as exc:
            LOGGER.exception("ニュース取得失敗: %s", url)
            errors.append(f"{url}: {exc}")

    if publisher == "Bloomberg日本語" and not items:
        fallback_items = fetch_bloomberg_jp_homepage_items(session, limit=limit)
        if fallback_items:
            return fallback_items

    if items:
        return items

    if errors:
        return [{"title": f"日本語ニュース取得失敗: {' / '.join(errors)}", "link": "", "pub_date": ""}]

    return [{"title": "日本語ニュースを取得できませんでした。", "link": "", "pub_date": ""}]


def fetch_all_data() -> Dict[str, List[MarketRow]]:
    session = requests_session()
    results = {category: [] for category in CATEGORY_ORDER}

    for category in ("株式", "米国債", "商品", "暗号資産"):
        for spec in YF_ITEMS[category]:
            custom_method = spec.get("custom_method")
            if custom_method == "yahoo_topix_page":
                results[category].append(fetch_topix_from_yahoo_finance(session))
            elif custom_method == "jpx_reit_page":
                results[category].append(fetch_tse_reit_from_jpx(session))
            elif custom_method == "investing_us_bond_2y":
                results[category].append(fetch_us_2y_from_investing(session))
            else:
                results[category].append(fetch_yahoo_row(category, spec))

    results["為替"] = fetch_forex_rows(session)
    results["日本国債"] = fetch_jgb_rows(session)
    return results


def unique_rows(results: Dict[str, List[MarketRow]]) -> Dict[str, List[MarketRow]]:
    deduped = {}
    seen_names = set()
    for category in CATEGORY_ORDER:
        deduped[category] = []
        for row in results.get(category, []):
            if row.name in seen_names:
                continue
            seen_names.add(row.name)
            deduped[category].append(row)
    return deduped


def resolve_forex_decimals(value: float) -> int:
    absolute_value = abs(value)
    if absolute_value >= 1000:
        return 2
    if absolute_value >= 100:
        return 3
    if absolute_value >= 1:
        return 4
    if absolute_value >= 0.1:
        return 5
    return 6


def resolve_display_decimals(row: MarketRow) -> int:
    if row.suffix == "%":
        return 3

    if row.category == "為替" and row.name != "ドルインデックス" and row.value is not None:
        return resolve_forex_decimals(row.value)

    if row.name in {"ドルインデックス"}:
        return 4

    if "円" in row.name and "国債" not in row.name:
        return 3

    return 2


def format_value(row: Optional[MarketRow]) -> str:
    if row is None or row.value is None:
        return "未取得"

    decimals = resolve_display_decimals(row)
    return f"{row.value:,.{decimals}f}{row.suffix}"


def format_change(row: MarketRow) -> str:
    if row.change is None:
        return "未確認"
    sign = "+" if row.change >= 0 else ""
    decimals = resolve_display_decimals(row)
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


def build_overview_paragraphs(results: Dict[str, List[MarketRow]]) -> List[str]:
    spx = pick_row(results, "S&P500")
    nasdaq = pick_row(results, "NASDAQ総合")
    dow = pick_row(results, "NYダウ")
    sox = pick_row(results, "SOX")
    vix = pick_row(results, "VIX")
    nikkei = pick_row(results, "日経225")
    topix = pick_row(results, "TOPIX")
    reit = pick_row(results, "J-REIT")
    usd_jpy = pick_row(results, "USD/JPY")
    eur_usd = pick_row(results, "EUR/USD")
    dxy = pick_row(results, "ドルインデックス")
    us10 = pick_row(results, "米国債10年利回り")
    jp10 = pick_row(results, "日本国債10年利回り")
    gold = pick_row(results, "金")
    oil = pick_row(results, "WTI原油")
    copper = pick_row(results, "銅")
    btc = pick_row(results, "BTC/USD")
    eth = pick_row(results, "ETH/USD")

    paragraphs = []
    paragraphs.append(
        "足元の相場全体をみると、米国株は "
        f"NYダウ {format_value(dow)}、S&P500 {format_value(spx)}、NASDAQ総合 {format_value(nasdaq)} "
        f"の並びで弱含みとなっており、SOX {format_value(sox)} の動きもあわせてみると、"
        "ハイテク・半導体まで売りが広がっている構図です。"
        f"一方で VIX は {format_value(vix)} と高めで、株式市場の不安心理がまだ残っていることを示しています。"
    )
    paragraphs.append(
        "日本株は、日経225 "
        f"{format_value(nikkei)} と TOPIX {format_value(topix)} を比べると、"
        "大型株主導なのか、より広い市場全体に売買が波及しているのかを切り分けやすい状態です。"
        f"J-REITは {format_value(reit)} で、金利の水準や国内不動産関連の見方を補助的に確認する材料になります。"
    )
    paragraphs.append(
        "為替と金利では、ドルインデックス "
        f"{format_value(dxy)}、USD/JPY {format_value(usd_jpy)}、EUR/USD {format_value(eur_usd)} "
        "を並べることで、ドル高そのものなのか、円安やユーロ安が主因なのかを整理しやすくなります。"
        f"加えて、米10年債利回り {format_value(us10)} と日本10年債利回り {format_value(jp10)} を見ると、"
        "日米金利差が為替をどの程度支えているかを確認できます。"
    )
    paragraphs.append(
        "商品市況では、金 "
        f"{format_value(gold)}、WTI原油 {format_value(oil)}、銅 {format_value(copper)} "
        "を中心に見ると、安全資産、エネルギー、景気敏感という異なる軸を同時に追えます。"
        "金が強く、原油や銅が弱い局面なら慎重姿勢が強いと読みやすく、逆なら景気期待が支えになっている可能性があります。"
    )
    paragraphs.append(
        "暗号資産は、BTC/USD "
        f"{format_value(btc)} と ETH/USD {format_value(eth)} を中心に、"
        "伝統資産とは別のリスク選好の温度感を測る補助指標として扱っています。"
        "株式が弱いのに暗号資産が底堅い場合は、投機資金の残存を示すことがあり、逆に同時安ならリスク回避色が強いと解釈しやすいです。"
    )
    return paragraphs


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


def build_summary_html(results: Dict[str, List[MarketRow]], news_map: Dict[str, List[dict]], generated_at_jst: datetime, generated_at_ny: datetime) -> str:
    overview_paragraphs = build_overview_paragraphs(results)
    overview_html = "".join(f"<p>{html.escape(text)}</p>" for text in overview_paragraphs)
    sections_html = build_category_sections(results)
    news_html = build_news_sections(news_map)
    favicon_links = build_favicon_links()
    head_favicon_block = f"\n{favicon_links}" if favicon_links else ""

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>世界経済サマリー</title>
  <link rel="stylesheet" href="../style.css">{head_favicon_block}
</head>
<body class="summary-page">
  <header class="page-header">
    <div class="page-header-top">
      <div class="year-nav">
        <a class="year-nav-button" href="../index.html">カレンダーへ戻る</a>
      </div>
      <div class="year-range-note">取得時点差あり</div>
    </div>
    <h1>世界経済サマリー</h1>
    <p class="summary-meta">生成時刻 JST: {html.escape(generated_at_jst.strftime("%Y-%m-%d %H:%M:%S"))} / New York: {html.escape(generated_at_ny.strftime("%Y-%m-%d %H:%M:%S"))}</p>
  </header>

  <section class="summary-section summary-overview-section">
    <h2>概況</h2>
    <div class="summary-overview-prose">
      {overview_html}
    </div>
  </section>

  <section class="summary-sections-grid">
    {sections_html}
  </section>


  {news_html}

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
    news_map = {publisher: fetch_news_items(session, publisher, urls) for publisher, urls in NEWS_SOURCES.items()}

    now_jst = datetime.now(JST)
    now_ny = now_jst.astimezone(NY)
    html_text = build_summary_html(results, news_map, now_jst, now_ny)
    payload = build_payload(results, news_map, now_jst, now_ny)
    write_outputs(html_text, payload, now_jst.strftime("%Y-%m-%d"))
    LOGGER.info("世界経済サマリー生成完了: %s", SUMMARY_DIR)
    return 0


if __name__ == "__main__":
    sys.exit(main())
