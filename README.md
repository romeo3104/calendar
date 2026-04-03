# calendar

GitHub Pages で公開する年カレンダーと、世界経済サマリーを生成する静的サイトです。

このリポジトリは Python スクリプトで HTML と JSON を生成し、GitHub Actions で Pages へデプロイする構成です。カレンダーは 1990 年から 2050 年までの年ページを静的生成し、ブラウザ側で JST の現在日付を再判定して当日表示を維持します。

## 1. このリポジトリでできること

- 1990 年から 2050 年までの年カレンダーを生成する
- 日本の祝日、和暦、干支、和風月名を表示する
- JST の当日セルを強調し、世界経済サマリーへ遷移できるようにする
- JST 0 時を跨いでも、ページを開いたまま当日表示と当月表示を更新する
- `index.html` から閲覧時点の JST 現在年ページへ遷移する
- スマホなどの狭い画面幅では、初回表示時に当月セクションまで自動スクロールする
- 世界経済サマリーを HTML と JSON で生成する
- GitHub Actions で Pages デプロイ用 workflow とサマリー生成用 workflow を分離運用する

## 2. ディレクトリ構成

- `scripts/build_calendar.py`
  - 年カレンダーと `index.html` を生成する
- `scripts/build_world_summary.py`
  - 世界経済サマリーの HTML / JSON を生成する
- `src/style.css`
  - カレンダーとサマリーの共通スタイル
- `src/favicon.svg`
  - ファビコン元ファイル
- `.github/workflows/deploy-calendar-midnight.yml`
  - Pages 向けの本体デプロイ workflow
- `.github/workflows/deploy-world-summary.yml`
  - 世界経済サマリー生成 workflow
- `dist/`
  - Pages 配信用の出力先（生成時に再作成される）
- `dist/summary/`
  - 世界経済サマリーの出力先

## 3. カレンダー生成仕様

### 3-1. 生成対象年

- 1990 年から 2050 年までを生成する
- 各年のファイル名は `YYYY.html`
- `index.html` は固定年ページのコピーではなく、閲覧時点の JST 現在年ページへ遷移する

出力例:

- `1990.html`
- `2026.html`
- `2050.html`
- `index.html`

### 3-2. 年タイトル

年タイトルは次の形式です。

- `2026年（令和8年）午年[うま年]カレンダー`

含まれる要素:

- 西暦
- 和暦
- 干支
- カレンダー

### 3-3. 月タイトル

月タイトルは次の形式です。

- `1月　January (睦月)`
- `2月　February (如月)`
- `3月　March (弥生)`

含まれる要素:

- 月番号
- 英語月名
- 和風月名

### 3-4. 年ナビゲーション

上部ナビゲーションには次を表示します。

- `前`
  - 1 年前へ移動
- 表示中の年チップ
- `次`
  - 1 年後へ移動
- `今`
  - 閲覧時点の JST 現在年へ移動

境界条件:

- 1990 年では `前` を無効表示
- 2050 年では `次` を無効表示
- 現在年では `今` を無効表示

補足:

- 直前に押したナビゲーションボタンのフォーカス状態を localStorage で保持する JavaScript を埋め込んでいます
- `今` ボタンの遷移先はブラウザ側で JST 現在年に合わせて更新します

### 3-5. 月パネル描画

- 各月は常に 6 週分を描画する
- 4 週や 5 週で収まる月も空セルで高さを揃える
- 当月は他の月より見やすい強調表示にする

### 3-6. 当日表示と JST 0 時更新

当日セルには次を適用します。

- 背景色の強調
- 枠線の強調
- 太字表示
- 日付リンク化

補足:

- 初回表示時にブラウザ側で JST の当日を再判定する
- ページを開いたまま JST 0 時を跨いでも、当日セルと当月強調を更新する（`requestAnimationFrame` とタイムアウトで次の 0 時をスケジューリング）
- 当日リンク先は `summary/latest.html`
- 年跨ぎ時は `今` ボタンの遷移先も JST 現在年に合わせて更新する

### 3-7. モバイル表示時の当月スクロール

スマホなどの狭い画面幅では、初回表示時だけ当月セクションまで自動スクロールします。

挙動条件:

- 画面幅が狭いときだけ動作する
- 表示中ページの年が JST の現在年である場合だけ動作する
- URL にハッシュが付いている場合は勝手にスクロールしない
- 深夜の自動日付更新時には再スクロールしない

この実装により、モバイルで年ページを開いたときに当月へ素早く到達できます。

### 3-8. 祝日表示

日本の祝日をセル内に表示します。

対象例:

- 元日
- 成人の日
- 建国記念の日
- 天皇誕生日
- 春分の日
- 昭和の日
- 憲法記念日
- みどりの日
- こどもの日
- 海の日
- 山の日
- 敬老の日
- 秋分の日
- 体育の日 / スポーツの日
- 文化の日
- 勤労感謝の日
- 振替休日
- 国民の休日

補足:

- ハッピーマンデーを考慮する
- 2020 年と 2021 年の特例移動祝日に対応する
- 春分日と秋分日は近似式で算出する
- 祝日計算の対象は 1949 年から 2099 年だが、生成対象は 1990 年から 2050 年

### 3-9. ファビコン

ファビコンは `src/` ディレクトリから候補を順に探して `dist/` へコピーします。

優先順:

1. `favicon.svg`（`image/svg+xml`）
2. `favicon.ico`（`image/x-icon`）
3. `favicon.png`（`image/png`）

### 3-10. dist/ の再作成

`build_calendar.py` は実行のたびに `dist/` ディレクトリを削除して再作成します。そのため、Pages に出す workflow ではカレンダーとサマリーを同一実行内で生成する必要があります。

## 4. 世界経済サマリー生成仕様

### 4-1. 出力ファイル

世界経済サマリーは次を出力します。

- `dist/summary/latest.html`
- `dist/summary/latest.json`
- `dist/summary/YYYY-MM-DD.html`
- `dist/summary/YYYY-MM-DD.json`

用途:

- `latest.html`
  - カレンダー当日セルから開く最新サマリー
- `latest.json`
  - 最新サマリーの機械読取用データ
- 日付付きファイル
  - 生成履歴の保存

### 4-2. 表示内容

サマリー画面には次を表示します。

- 生成時刻
  - JST
  - New York
- 概況テキスト
- カテゴリ別テーブル
- Reuters 日本語ニュース
- Bloomberg 日本語ニュース
- カレンダーへ戻るボタン

補足:

- 概況は取得済みデータから文章を組み立てる
- テーブルはカテゴリごとに横スクロールできる
- サマリーページから `../index.html` 経由でカレンダーへ戻れる

### 4-3. 対象カテゴリ

カテゴリは次の順序で出力します。

- 株式
- 為替
- 米国債
- 日本国債
- 商品
- 暗号資産

### 4-4. 主な取得対象

株式カテゴリの主な対象:

- NYダウ
- NASDAQ 総合
- S&P500
- SOX
- VIX
- 日経 225
- TOPIX
- J-REIT

米国債カテゴリの主な対象:

- 米国債 2 年利回り
- 米国債 5 年利回り
- 米国債 10 年利回り
- 米国債 30 年利回り

日本国債カテゴリの主な対象:

- 日本国債 2 年利回り
- 日本国債 5 年利回り
- 日本国債 10 年利回り
- 日本国債 30 年利回り

商品カテゴリの主な対象:

- 金
- 銀
- WTI 原油
- Brent 原油
- 天然ガス
- 銅
- プラチナ
- パラジウム

暗号資産カテゴリの主な対象:

- BTC/USD
- BTC/JPY
- ETH/USD
- XRP/USD

為替カテゴリの主な対象:

- ドルインデックス
- Yahoo! ファイナンス FX ページから検出した為替ペア

### 4-5. 取得元と補完ロジック

主な取得元は次のとおりです。

- `yfinance`
  - 米国株指数、VIX、日経 225、商品、暗号資産など
- Yahoo! ファイナンス
  - TOPIX、為替関連ページ
- JPX
  - TOPIX、J-REIT の補完取得（JSON API、リアルタイム指数一覧、個別指数ページ）
- Investing.com
  - TOPIX、J-REIT、日本国債、米国債 2 年利回りの補完取得（概要ページ、過去データページ）
- 財務省 CSV
  - 日本国債利回り
- Google ニュース RSS
  - Reuters 日本語、Bloomberg 日本語ニュースの取得入口

補足:

- TOPIX と J-REIT は単一取得元に依存せず、複数ソースで補完する
- 日本語ニュースは Google ニュース RSS と Bloomberg 日本語トップ補完を組み合わせる
- ニュースタイトルの日本語判定や不要なサフィックス除去のフィルタリングを行う

### 4-6. TOPIX の取得フロー

TOPIX は単一ソースの障害に備え、多段のフォールバックで取得します。

1. Yahoo! ファイナンス TOPIX ページ（スナップショット解析 → 時系列行解析）
2. 取得成功後、前日比・騰落率が欠けている場合は以下で補完:
   - Investing.com 過去データページ
   - Investing.com 概要ページ
   - JPX リアルタイム指数一覧
   - JPX 個別指数ページ
3. Yahoo! ファイナンスが全て失敗した場合のフォールバック:
   - JPX JSON API
   - Investing.com 過去データページ（+ 上記の補完）
   - Investing.com 概要ページ（+ 上記の補完）
   - JPX リアルタイム指数一覧 + JPX 個別指数ページ

### 4-7. J-REIT の取得フロー

J-REIT も複数ソースで取得します。

1. Investing.com 過去データページ
2. Investing.com 概要ページ
3. JPX 個別指数ページ
4. JPX JSON API
5. JPX リアルタイム指数一覧

### 4-8. 日本国債の取得フロー

日本国債利回りは財務省 CSV を基本とし、Investing.com で補完します。

1. 財務省 CSV（当年分 → 前年分）から各年限の利回りを取得
2. 前日比・騰落率が欠けている年限は Investing.com の個別ページから補完

## 5. GitHub Actions 構成

### 5-1. workflow の役割分離

このリポジトリでは workflow を役割で分けます。

- `deploy-calendar-midnight.yml`
  - Pages 配信用の本体 workflow
  - カレンダーを生成する
  - 世界経済サマリーも `--force` 付きで生成する
  - `dist/` を artifact として Pages へ配信する
- `deploy-world-summary.yml`
  - 世界経済サマリー生成 workflow
  - カレンダーも生成する（`dist/` 再作成のため必須）
  - `push`・手動実行では `--force`、定期実行では時刻条件付きで生成する
  - `dist/` を artifact として Pages へ配信する

補足:

- 両 workflow ともカレンダーとサマリーの両方を生成し、`dist/` 全体を Pages に出す
- `build_calendar.py` は `dist/` を再作成するため、サマリーだけを個別に配信すると `index.html` が欠けて 404 になる
- 同一 concurrency group (`pages`) で排他制御する

### 5-2. 定期実行時刻

世界経済サマリーは、workflow 側の `cron` と Python 側の時刻判定を組み合わせて運用します。

`deploy-calendar-midnight.yml`:

- `0 15 * * *` UTC（= JST 0:00）
  - 毎日深夜にカレンダーを再生成し、サマリーも `--force` で生成する

`deploy-world-summary.yml`:

- `10 20 * * *` UTC（= NY 16:10 EST / 冬時間期間）
- `10 21 * * *` UTC（= NY 17:10 EDT / 夏時間期間）
  - NY 市場クローズ 10 分後を狙う
  - 夏時間と冬時間で UTC が 1 時間ずれるため、2 つの cron を設定する
  - 定期実行時は Python 側の `should_run_now()` で NY 16 時台かつ 10 分以降を確認する

時刻調整時の確認箇所:

- `.github/workflows/*.yml`
- `scripts/build_world_summary.py` の `should_run_now()`

### 5-3. トリガーと実行条件

| トリガー | カレンダー | サマリー | 備考 |
|---|---|---|---|
| `push` to main | 生成 | `--force` で生成 | 両 workflow が起動する |
| `workflow_dispatch` | 生成 | `--force` で生成 | 手動実行 |
| `schedule`（midnight） | 生成 | `--force` で生成 | JST 0:00 |
| `schedule`（summary） | 生成 | 時刻条件付き | NY 16:10 判定 |

## 6. ローカル生成手順

### 6-1. 依存関係

最低限必要な実行環境:

- Python 3.12 系を想定
- `yfinance`
- `requests`

インストール例:

```bash
python -m pip install --upgrade pip
pip install yfinance requests
```

### 6-2. カレンダー生成

```bash
python scripts/build_calendar.py
```

生成物:

- `dist/YYYY.html`（1990〜2050）
- `dist/index.html`
- `dist/style.css`
- `dist/favicon.*`

注意:

- 実行のたびに `dist/` を削除して再作成します
- 既存の `dist/summary/` も消えるため、サマリーが必要な場合は続けて生成してください

### 6-3. 世界経済サマリー生成

強制生成:

```bash
python scripts/build_world_summary.py --force
```

時刻条件付き生成:

```bash
python scripts/build_world_summary.py
```

補足:

- `--force` を付けると Python 側の時刻条件を無視して生成します
- ローカル確認や `push` 起点の再生成に向いています
- 通常実行時は NY 時間 16 時台かつ 10 分以降のときに生成します

## 7. 運用上の注意

- `build_calendar.py` 実行時は `dist/` を削除して再作成する
- そのため、両 workflow ではカレンダーとサマリーを同一ジョブ内で連続して生成する
- `index.html` は閲覧時点の JST 現在年へ遷移するため、生成時刻の固定年へ縛られない
- 当日判定はブラウザ側で JST を使って再計算するため、JST 0 時跨ぎでも表示が追従する
- モバイル自動スクロールは初回表示時だけであり、閲覧中の勝手な再スクロールは行わない
- 両 workflow は同一 concurrency group で排他制御されるため、同時実行時は後発が先発をキャンセルする

## 8. デバッグ時の確認ポイント

不具合調査時は次を優先して確認してください。

- `dist/index.html` が存在するか
- `dist/summary/latest.html` が存在するか
- Pages に出す artifact が `dist/` 全体になっているか
- workflow の `cron` と Python 側の時刻判定が一致しているか
- `push`、`workflow_dispatch`、`schedule` で実行条件が分岐していないか
- JST 当日判定と表示中年判定が期待どおりか
- モバイル表示で URL ハッシュがある場合に勝手なスクロールが起きていないか
- TOPIX / J-REIT の取得が全ソースで失敗していないか（ログで確認）
- 日本国債の前日比・騰落率が Investing.com で補完されているか
