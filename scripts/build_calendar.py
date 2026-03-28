\
    #!/usr/bin/env python3
    # -*- coding: utf-8 -*-

    """
    GitHub Pages 用の年カレンダーを静的生成するスクリプトです。

    要件:
    - JST 基準で当年の12か月分を生成
    - 当日を強調表示
    - 当月を他の月より見やすく表示
    - 日本の祝日を表示
    - ヘッダーの「毎日 0時台に自動更新」は表示しない
    """

    from __future__ import annotations

    import calendar
    import html
    import logging
    import sys
    from datetime import date, datetime, timedelta
    from pathlib import Path
    from typing import Dict
    from zoneinfo import ZoneInfo


    LOGGER = logging.getLogger(__name__)

    JST = ZoneInfo("Asia/Tokyo")
    ROOT_DIR = Path(__file__).resolve().parent.parent
    SRC_STYLE_PATH = ROOT_DIR / "src" / "style.css"
    DIST_DIR = ROOT_DIR / "dist"
    DIST_HTML_PATH = DIST_DIR / "index.html"
    DIST_STYLE_PATH = DIST_DIR / "style.css"


    def setup_logging() -> None:
        """
        ログ設定を初期化します。
        """
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )


    def nth_weekday(year: int, month: int, weekday: int, nth: int) -> date:
        """
        指定年月の第 n 週の曜日の日付を返します。

        :param year: 年
        :param month: 月
        :param weekday: 曜日。月曜=0、日曜=6
        :param nth: 第何週か。1始まり
        :return: 該当日付
        :raises ValueError: 該当日付を求められない場合
        """
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
        """
        春分日の概算日を返します。

        2000年から2099年を対象にした一般的な近似式です。
        祝日法上の春分日は毎年公表されるため、将来の制度変更時は見直しが必要です。

        :param year: 年
        :return: 3月の春分日
        :raises ValueError: 対応範囲外の年
        """
        if not 2000 <= year <= 2099:
            raise ValueError("春分日の計算対象は 2000年から2099年です。")

        return int(20.8431 + 0.242194 * (year - 1980) - ((year - 1980) // 4))


    def autumn_equinox_day(year: int) -> int:
        """
        秋分日の概算日を返します。

        2000年から2099年を対象にした一般的な近似式です。
        祝日法上の秋分日は毎年公表されるため、将来の制度変更時は見直しが必要です。

        :param year: 年
        :return: 9月の秋分日
        :raises ValueError: 対応範囲外の年
        """
        if not 2000 <= year <= 2099:
            raise ValueError("秋分日の計算対象は 2000年から2099年です。")

        return int(23.2488 + 0.242194 * (year - 1980) - ((year - 1980) // 4))


    def get_national_holidays(year: int) -> Dict[date, str]:
        """
        祝日法上の国民の祝日を返します。

        振替休日と国民の休日は別処理で付与します。
        今回の用途に合わせ、2000年から2099年を対象にしています。

        :param year: 年
        :return: 日付をキー、祝日名を値とする辞書
        """
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

        if year >= 2007:
            add_holiday(date(year, 4, 29), "昭和の日")
        else:
            add_holiday(date(year, 4, 29), "みどりの日")

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
        elif year >= 2000:
            add_holiday(nth_weekday(year, 10, 0, 2), "体育の日")

        add_holiday(date(year, 11, 3), "文化の日")
        add_holiday(date(year, 11, 23), "勤労感謝の日")

        return holidays


    def get_citizens_holidays(national_holidays: Dict[date, str], year: int) -> Dict[date, str]:
        """
        国民の休日を返します。

        前日と翌日が国民の祝日である平日を対象にします。

        :param national_holidays: 祝日法上の国民の祝日
        :param year: 年
        :return: 国民の休日の辞書
        """
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


    def get_substitute_holidays(
        national_holidays: Dict[date, str],
        existing_holidays: Dict[date, str],
    ) -> Dict[date, str]:
        """
        振替休日を返します。

        国民の祝日が日曜日に当たる場合、その後で最初の非祝日を振替休日にします。

        :param national_holidays: 祝日法上の国民の祝日
        :param existing_holidays: 既存の休日一式
        :return: 振替休日の辞書
        """
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
        """
        表示用の日本の祝日一覧を返します。

        :param year: 年
        :return: 日付をキー、休日名を値とする辞書
        """
        national_holidays = get_national_holidays(year)
        citizens_holidays = get_citizens_holidays(national_holidays, year)

        all_holidays: Dict[date, str] = {}
        all_holidays.update(national_holidays)
        all_holidays.update(citizens_holidays)

        substitute_holidays = get_substitute_holidays(national_holidays, all_holidays)
        all_holidays.update(substitute_holidays)

        return dict(sorted(all_holidays.items()))


    def build_day_cell(
        current_date: date,
        today: date,
        holidays: Dict[date, str],
    ) -> str:
        """
        日付セルの HTML を生成します。

        :param current_date: 対象日付
        :param today: 今日の日付
        :param holidays: 祝日辞書
        :return: td 要素の文字列
        """
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

        holiday_html = ""
        if holiday_name is not None:
            holiday_html = f'<div class="holiday-name">{escaped_holiday_name}</div>'

        return (
            f"<td{class_attr}>"
            f'<div class="day-number">{current_date.day}</div>'
            f"{holiday_html}"
            f"</td>"
        )


    def build_month(year: int, month: int, today: date, holidays: Dict[date, str]) -> str:
        """
        1か月分の HTML を生成します。

        :param year: 年
        :param month: 月
        :param today: 今日の日付
        :param holidays: 祝日辞書
        :return: 1か月分の HTML
        """
        cal = calendar.Calendar(firstweekday=6)
        month_name = f"{month}月"
        is_current_month = year == today.year and month == today.month
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
            for day in week:
                if day == 0:
                    rows.append('<td class="empty"></td>')
                    continue

                current_date = date(year, month, day)
                rows.append(build_day_cell(current_date, today, holidays))
            rows.append("</tr>")

        rows.append("</tbody>")
        rows.append("</table>")
        rows.append("</section>")

        return "\n".join(rows)


    def build_html(year: int, today: date, holidays: Dict[date, str]) -> str:
        """
        ページ全体の HTML を生成します。

        :param year: 年
        :param today: 今日の日付
        :param holidays: 祝日辞書
        :return: HTML 全文
        """
        months_html = "\n".join(build_month(year, month, today, holidays) for month in range(1, 13))

        return f"""<!DOCTYPE html>
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
      </header>
      <main class="months-grid">
        {months_html}
      </main>
    </body>
    </html>
    """


    def write_output_files(html_text: str) -> None:
        """
        生成した HTML と CSS を dist 配下へ出力します。

        :param html_text: 出力する HTML
        :raises FileNotFoundError: style.css が存在しない場合
        """
        if not SRC_STYLE_PATH.exists():
            raise FileNotFoundError(f"CSS ファイルが見つかりません: {SRC_STYLE_PATH}")

        DIST_DIR.mkdir(parents=True, exist_ok=True)

        DIST_HTML_PATH.write_text(html_text, encoding="utf-8")
        DIST_STYLE_PATH.write_text(SRC_STYLE_PATH.read_text(encoding="utf-8"), encoding="utf-8")


    def main() -> int:
        """
        エントリーポイントです。

        :return: 終了コード
        """
        setup_logging()

        try:
            now = datetime.now(JST)
            today = now.date()
            year = today.year

            LOGGER.info("カレンダー生成を開始します。 year=%s date=%s", year, today.isoformat())

            holidays = get_japanese_holidays(year)
            html_text = build_html(year, today, holidays)
            write_output_files(html_text)

            LOGGER.info("カレンダー生成が完了しました。 output=%s", DIST_HTML_PATH)
            return 0

        except Exception:
            LOGGER.exception("カレンダー生成に失敗しました。")
            return 1


    if __name__ == "__main__":
        sys.exit(main())
