#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime
from zoneinfo import ZoneInfo
import calendar
import os

JST = ZoneInfo("Asia/Tokyo")


def build_month(year: int, month: int, today) -> str:
    cal = calendar.Calendar(firstweekday=6)
    month_name = f"{month}月"
    is_current_month = (year == today.year and month == today.month)
    month_class = "month current-month" if is_current_month else "month"

    rows = []
    rows.append(f'<section class="{month_class}">')
    rows.append(f"<h2>{month_name}</h2>")
    rows.append('<table class="calendar-table">')
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

    for week in cal.monthdayscalendar(year, month):
        rows.append("<tr>")
        for idx, day in enumerate(week):
            if day == 0:
                rows.append('<td class="empty"></td>')
                continue

            classes = []
            if idx == 0:
                classes.append("sun")
            elif idx == 6:
                classes.append("sat")

            if year == today.year and month == today.month and day == today.day:
                classes.append("today")

            class_attr = f' class="{" ".join(classes)}"' if classes else ""
            rows.append(f"<td{class_attr}>{day}</td>")
        rows.append("</tr>")

    rows.append("</tbody>")
    rows.append("</table>")
    rows.append("</section>")
    return "\n".join(rows)


def main() -> None:
    now = datetime.now(JST)
    year = now.year

    months_html = "\n".join(build_month(year, m, now.date()) for m in range(1, 13))

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{year}年カレンダー</title>
  <link rel="stylesheet" href="style.css">
</head>
<body>
  <header class="page-header">
    <h1>{year}年カレンダー</h1>
    <p>毎日 0時台に自動更新</p>
  </header>
  <main class="months-grid">
    {months_html}
  </main>
</body>
</html>
"""

    os.makedirs("dist", exist_ok=True)

    with open("dist/index.html", "w", encoding="utf-8") as f:
        f.write(html)

    with open("src/style.css", "r", encoding="utf-8") as src:
        css = src.read()

    with open("dist/style.css", "w", encoding="utf-8") as dst:
        dst.write(css)


if __name__ == "__main__":
    main()
