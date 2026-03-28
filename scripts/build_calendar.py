#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import calendar
import html
import logging
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger(__name__)

JST = ZoneInfo("Asia/Tokyo")
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
SRC_STYLE_PATH = SRC_DIR / "style.css"
DIST_DIR = ROOT_DIR / "dist"
DIST_STYLE_PATH = DIST_DIR / "style.css"

YEAR_RANGE = 20

MONTH_LABELS = {
    1: ("January", "睦月"),
    2: ("February", "如月"),
    3: ("March", "弥生"),
    4: ("April", "卯月"),
    5: ("May", "皐月"),
    6: ("June", "水無月"),
    7: ("July", "文月"),
    8: ("August", "葉月"),
    9: ("September", "長月"),
    10: ("October", "神無月"),
    11: ("November", "霜月"),
    12: ("December", "師走"),
}

ZODIAC_LABELS = [
    ("子", "ね"),
    ("丑", "うし"),
    ("寅", "とら"),
    ("卯", "う"),
    ("辰", "たつ"),
    ("巳", "み"),
    ("午", "うま"),
    ("未", "ひつじ"),
    ("申", "さる"),
    ("酉", "とり"),
    ("戌", "いぬ"),
    ("亥", "い"),
]

FAVICON_CANDIDATES = [
    ("favicon.svg", "image/svg+xml"),
    ("favicon.ico", "image/x-icon"),
    ("favicon.png", "image/png"),
]


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
    if nth < 1:
        raise ValueError("nth は 1 以上である必要があります。")

    current = date(year, month, 1)
    while current.weekday() != weekday:
        current += timedelta(days=1)

    current += timedelta(weeks=nth - 1)

    if current.month != month:
        raise ValueError("指定した第 n 曜日は存在しません。")

    return current


def vernal_equinox_day(year: int) -> int:
    if not 2000 <= year <= 2099:
        raise ValueError("春分日の計算対象は 2000年から2099年です。")
    return int(20.8431 + 0.242194 * (year - 1980) - ((year - 1980) // 4))


def autumn_equinox_day(year: int) -> int:
    if not 2000 <= year <= 2099:
        raise ValueError("秋分日の計算対象は 2000年から2099年です。")
    return int(23.2488 + 0.242194 * (year - 1980) - ((year - 1980) // 4))


def get_national_holidays(year: int) -> Dict[date, str]:
    if not 2000 <= year <= 2099:
        raise ValueError("このカレンダーの祝日計算対象は 2000年から2099年です。")

    holidays: Dict[date, str] = {}

    def add_holiday(target_date: date, name: str) -> None:
        holidays[target_date] = name

    add_holiday(date(year, 1, 1), "元日")
    add_holiday(nth_weekday(year, 1, 0, 2), "成人の日")
    add_holiday(date(year, 2, 11), "建国記念の日")

    if year >= 2020:
        add_holiday(date(year, 2, 23), "天皇誕生日")

    add_holiday(date(year, 3, vernal_equinox_day(year)), "春分の日")
    add_holiday(date(year, 4, 29), "昭和の日" if year >= 2007 else "みどりの日")
    add_holiday(date(year, 5, 3), "憲法記念日")

    if year >= 2007:
        add_holiday(date(year, 5, 4), "みどりの日")

    add_holiday(date(year, 5, 5), "こどもの日")

    if year == 2020:
        add_holiday(date(year, 7, 23), "海の日")
    elif year == 2021:
        add_holiday(date(year, 7, 22), "海の日")
    elif year >= 2003:
        add_holiday(nth_weekday(year, 7, 0, 3), "海の日")
    else:
        add_holiday(date(year, 7, 20), "海の日")

    if year >= 2016:
        if year == 2020:
            add_holiday(date(year, 8, 10), "山の日")
        elif year == 2021:
            add_holiday(date(year, 8, 8), "山の日")
        else:
            add_holiday(date(year, 8, 11), "山の日")

    add_holiday(nth_weekday(year, 9, 0, 3), "敬老の日")
    add_holiday(date(year, 9, autumn_equinox_day(year)), "秋分の日")

    if year == 2020:
        add_holiday(date(year, 7, 24), "スポーツの日")
    elif year == 2021:
        add_holiday(date(year, 7, 23), "スポーツの日")
    elif year >= 2022:
        add_holiday(nth_weekday(year, 10, 0, 2), "スポーツの日")
    else:
        add_holiday(nth_weekday(year, 10, 0, 2), "体育の日")

    add_holiday(date(year, 11, 3), "文化の日")
    add_holiday(date(year, 11, 23), "勤労感謝の日")
    return holidays


def get_citizens_holidays(national_holidays: Dict[date, str], year: int) -> Dict[date, str]:
    citizens_holidays: Dict[date, str] = {}
    current = date(year, 1, 2)
    end_date = date(year, 12, 30)

    while current <= end_date:
        previous_day = current - timedelta(days=1)
        next_day = current + timedelta(days=1)
        if (
            current not in national_holidays
            and previous_day in national_holidays
            and next_day in national_holidays
            and current.weekday() != 6
        ):
            citizens_holidays[current] = "国民の休日"
        current += timedelta(days=1)
    return citizens_holidays


def get_substitute_holidays(national_holidays: Dict[date, str], existing_holidays: Dict[date, str]) -> Dict[date, str]:
    substitute_holidays: Dict[date, str] = {}
    occupied_days = set(existing_holidays.keys())

    for holiday_date in sorted(national_holidays.keys()):
        if holiday_date.weekday() != 6:
            continue
        candidate = holiday_date + timedelta(days=1)
        while candidate in occupied_days or candidate in substitute_holidays:
            candidate += timedelta(days=1)
        substitute_holidays[candidate] = "振替休日"

    return substitute_holidays


def get_japanese_holidays(year: int) -> Dict[date, str]:
    national_holidays = get_national_holidays(year)
    citizens_holidays = get_citizens_holidays(national_holidays, year)
    all_holidays: Dict[date, str] = {}
    all_holidays.update(national_holidays)
    all_holidays.update(citizens_holidays)
    all_holidays.update(get_substitute_holidays(national_holidays, all_holidays))
    return dict(sorted(all_holidays.items()))


def get_era_label(year: int) -> str:
    if year >= 2019:
        return f"令和{year - 2018}年"
    if year >= 1989:
        return f"平成{year - 1988}年"
    if year >= 1926:
        return f"昭和{year - 1925}年"
    return f"西暦{year}年"


def get_zodiac_label(year: int) -> str:
    kanji, reading = ZODIAC_LABELS[(year - 2020) % 12]
    return f"{kanji}年[{reading}年]"


def get_year_title(year: int) -> str:
    return f"{year}年（{get_era_label(year)}）{get_zodiac_label(year)}カレンダー"


def get_month_title(month: int) -> str:
    english_name, japanese_name = MONTH_LABELS[month]
    return f"{english_name} ({japanese_name})"


def build_today_link(current_date: date, today: date) -> str:
    if current_date == today:
        return f'<a class="today-link" href="summary/latest.html" aria-label="世界経済サマリーを表示">{current_date.day}</a>'
    return str(current_date.day)


def build_day_cell(current_date: date, today: date, holidays: Dict[date, str]) -> str:
    classes = []
    holiday_name = holidays.get(current_date)

    if current_date.weekday() == 6:
        classes.append("sun")
    elif current_date.weekday() == 5:
        classes.append("sat")

    if holiday_name is not None:
        classes.append("holiday")

    if current_date == today:
        classes.append("today")

    class_attr = f' class="{" ".join(classes)}"' if classes else ""
    escaped_holiday_name = html.escape(holiday_name) if holiday_name is not None else ""
    holiday_html = f'<div class="holiday-name">{escaped_holiday_name}</div>' if holiday_name is not None else ""

    return (
        f"<td{class_attr}>"
        f'<div class="day-number">{build_today_link(current_date, today)}</div>'
        f"{holiday_html}"
        f"</td>"
    )


def normalize_weeks(weeks: list[list[int]]) -> list[list[int]]:
    normalized = list(weeks)
    while len(normalized) < 6:
        normalized.append([0, 0, 0, 0, 0, 0, 0])
    return normalized[:6]


def build_month(year: int, month: int, today: date, holidays: Dict[date, str]) -> str:
    cal = calendar.Calendar(firstweekday=6)
    month_class = "month current-month" if year == today.year and month == today.month else "month"
    rows = [f'<section class="{month_class}">', f"<h2>{get_month_title(month)}</h2>", '<table class="calendar-table">']
    rows.append(
        "<thead><tr>"
        "<th class='sun'>日</th>"
        "<th>月</th>"
        "<th>火</th>"
        "<th>水</th>"
        "<th>木</th>"
        "<th>金</th>"
        "<th class='sat'>土</th>"
        "</tr></thead>"
    )
    rows.append("<tbody>")

    for week in normalize_weeks(cal.monthdayscalendar(year, month)):
        rows.append("<tr>")
        for day in week:
            if day == 0:
                rows.append('<td class="empty"></td>')
                continue
            current_date = date(year, month, day)
            rows.append(build_day_cell(current_date, today, holidays))
        rows.append("</tr>")

    rows.extend(["</tbody>", "</table>", "</section>"])
    return "\n".join(rows)


def get_year_filename(year: int) -> str:
    return f"{year}.html"


def build_nav_button(label: str, target_year: Optional[int], css_class: str) -> str:
    if target_year is None:
        return f'<span class="year-nav-button disabled {css_class}">{label}</span>'
    return f'<a class="year-nav-button {css_class}" href="{html.escape(get_year_filename(target_year))}">{label}</a>'


def build_favicon_links() -> str:
    lines = []
    for filename, mime_type in FAVICON_CANDIDATES:
        path = SRC_DIR / filename
        if path.exists():
            lines.append(f'  <link rel="icon" href="{html.escape(filename)}" type="{html.escape(mime_type)}">')
    return "\n".join(lines)


def build_html(year: int, today: date, holidays: Dict[date, str], min_year: int, max_year: int) -> str:
    months_html = "\n".join(build_month(year, month, today, holidays) for month in range(1, 13))
    previous_year = year - 1 if year > min_year else None
    next_year = year + 1 if year < max_year else None
    favicon_links = build_favicon_links()
    head_favicon_block = f"\n{favicon_links}" if favicon_links else ""

    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{html.escape(get_year_title(year))}</title>
  <link rel="stylesheet" href="style.css">{head_favicon_block}
</head>
<body>
  <header class="page-header">
    <div class="page-header-top">
      <div class="year-nav">
        {build_nav_button("前", previous_year, "prev")}
        {build_nav_button("次", next_year, "next")}
      </div>
      <div class="year-range-note">{min_year}年〜{max_year}年を表示できます</div>
    </div>
    <h1>{html.escape(get_year_title(year))}</h1>
  </header>
  <main class="months-grid">
    {months_html}
  </main>
</body>
</html>
"""


def copy_static_assets() -> None:
    if not SRC_STYLE_PATH.exists():
        raise FileNotFoundError(f"CSS ファイルが見つかりません: {SRC_STYLE_PATH}")

    DIST_STYLE_PATH.write_text(SRC_STYLE_PATH.read_text(encoding="utf-8"), encoding="utf-8")

    for filename, _ in FAVICON_CANDIDATES:
        source_path = SRC_DIR / filename
        if source_path.exists():
            shutil.copy2(source_path, DIST_DIR / filename)


def write_output_files(current_year: int, html_by_year: Dict[int, str]) -> None:
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir(parents=True, exist_ok=True)
    copy_static_assets()

    for year, html_text in html_by_year.items():
        (DIST_DIR / get_year_filename(year)).write_text(html_text, encoding="utf-8")
    (DIST_DIR / "index.html").write_text(html_by_year[current_year], encoding="utf-8")


def main() -> int:
    setup_logging()
    try:
        now = datetime.now(JST)
        today = now.date()
        current_year = today.year
        min_year = current_year - YEAR_RANGE
        max_year = current_year + YEAR_RANGE

        html_by_year: Dict[int, str] = {}
        for year in range(min_year, max_year + 1):
            holidays = get_japanese_holidays(year)
            html_by_year[year] = build_html(year, today, holidays, min_year, max_year)

        write_output_files(current_year, html_by_year)
        LOGGER.info("カレンダー生成が完了しました。 output_dir=%s", DIST_DIR)
        return 0
    except Exception:
        LOGGER.exception("カレンダー生成に失敗しました。")
        return 1


if __name__ == "__main__":
    sys.exit(main())
