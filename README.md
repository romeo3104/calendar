# calendar

GitHub Pages で公開する年カレンダーと、NY 市場クローズ後の世界経済サマリーを生成する静的サイトです。

このリポジトリは、Python スクリプトで静的 HTML と JSON を生成し、GitHub Actions で GitHub Pages へ公開する構成です。

## 1. 概要

このプログラムは、次の 2 つを生成します。

- 1990年から2050年までの日本の年カレンダー
- 当日セルから参照できる世界経済サマリー

主な特徴は次のとおりです。

- 日本の祝日を表示する
- 和暦と干支を含む年タイトルを表示する
- 当日セルを強調し、`summary/latest.html` へリンクする
- 年カレンダーを静的 HTML で生成する
- 世界経済サマリーを静的 HTML と JSON で生成する
- GitHub Actions で自動ビルドし、GitHub Pages へ公開する

## 2. ディレクトリ構成

主なファイルと役割は次のとおりです。

- `scripts/build_calendar.py`
  - 年カレンダーを生成する
- `scripts/build_world_summary.py`
  - 世界経済サマリーを生成する
- `src/style.css`
  - カレンダー画面とサマリー画面の共通スタイル
- `src/favicon.svg`
  - ファビコンの元ファイル
- `.github/workflows/deploy.yml`
  - GitHub Actions によるビルドと Pages デプロイ
- `dist/`
  - 生成物の出力先
- `dist/summary/`
  - 世界経済サマリーの出力先

## 3. カレンダー機能

### 3-1. 生成対象年

- 1990年から2050年までを生成する
- 各年の出力ファイル名は `YYYY.html`
- `index.html` は閲覧時点の JST 現在年ページへリダイレクトする

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
  - 1年前へ移動
- 現在表示中の年チップ
- `次`
  - 1年後へ移動
- `今`
  - 閲覧時点の JST 現在年へ移動

境界条件:

- 1990年では `前` を無効表示
- 2050年では `次` を無効表示
- 現在年では `今` を無効表示

補足:

- 直前に押したナビゲーションボタンのフォーカス状態を保持する JavaScript を埋め込んでいます

### 3-5. 月パネル描画

- 各月は常に 6 週分を描画する
- 4 週や 5 週で収まる月も空セルで高さを揃える
- 当月は青枠で強調する

### 3-6. 当日表示

当日セルには次を適用します。

- 背景色の強調
- 枠線の強調
- 太字表示
- 日付リンク化

補足:

- 初回表示時にブラウザ側で JST の当日を再判定する
- JST 0時を跨いだままページを開き続けても、自動で当日表示を更新する
- 当月の強調と `今` ボタンの遷移先も同じ判定で更新する

当日リンク先:

- `summary/latest.html`

### 3-7. 祝日表示

日本の祝日をセル内に表示します。

対象:

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

- ハッピーマンデーを考慮している
- 2020年と2021年の特例移動祝日に対応している
- 春分日と秋分日は近似式で算出している
- 祝日計算の対象は 1949年から2099年だが、実際の生成対象は 1990年から2050年

## 4. 世界経済サマリー機能

### 4-1. 生成タイミング

世界経済サマリーは GitHub Actions から生成します。

実行契機:

- `push` で強制生成
- `workflow_dispatch` で強制生成
- `schedule` で定期生成

定期生成の判定:

- workflow 側では UTC ベースで 2 本の cron を設定する
- Python 側では `America/New_York` の時刻を見て、平日 16:10〜16:19 のときだけ生成する
- `push` と `workflow_dispatch` では `--force` を付けて時刻条件を無視する

### 4-2. 出力ファイル

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

### 4-3. 表示内容

サマリー画面には次を表示します。

- 生成時刻
  - JST
  - New York
- 概況テキスト
- カテゴリ別テーブル
- Reuters日本語ニュース
- Bloomberg日本語ニュース
- カレンダーへ戻るボタン

補足:

- 概況は取得済みデータから文章を組み立てる
- テーブルはカテゴリごとにスクロール可能
- テーブルヘッダーは sticky 表示

### 4-4. 対象カテゴリ

カテゴリは次の順序で出力します。

- 株式
- 為替
- 米国債
- 日本国債
- 商品
- 暗号資産

### 4-5. 株式カテゴリ

現在の実装で対象としている株式系指標は次のとおりです。

- NYダウ
- NASDAQ総合
- S&P500
- SOX
- VIX
- 日経225
- TOPIX
- J-REIT

取得方法の概要:

- NYダウ、NASDAQ総合、S&P500、SOX、VIX、日経225
  - `yfinance`
- TOPIX
  - Yahoo!ファイナンスの TOPIX ページを主系にし、必要に応じて Investing.com と JPX で補完
- J-REIT
  - Investing.com の東証REIT指数 過去データページを主系にし、概要ページ、JPX リアルタイム指数一覧、JPX 個別指数ページで補完

### 4-6. 為替カテゴリ

為替カテゴリは次の構成です。

- ドルインデックス
- Yahoo!ファイナンス FX ページで検出できた通貨コードから組み立てた為替ペア

実装仕様:

- まず `ドルインデックス` を取得する
- Yahoo!ファイナンス FX ページから対応通貨一覧を抽出する
- 抽出した通貨コードの総当たりで `AAA/BBB` 形式の候補を生成する
- `AAABBB=X` 形式で一括取得し、取得できたペアだけを表示する
- 一括取得結果が空の場合は、主要ペアのみの固定リストへフォールバックする

優先表示対象の例:

- USD/JPY
- AUD/JPY
- GBP/JPY
- EUR/JPY
- NZD/JPY
- ZAR/JPY
- CAD/JPY
- CHF/JPY
- EUR/USD
- GBP/USD
- AUD/USD
- NZD/USD
- EUR/AUD
- EUR/GBP
- USD/CHF
- GBP/CHF
- EUR/CHF

### 4-7. 米国債カテゴリ

現在の実装で対象としている米国債利回りは次のとおりです。

- 米国債2年利回り
- 米国債5年利回り
- 米国債10年利回り
- 米国債30年利回り

取得方法:

- 2年
  - Investing.com
- 5年、10年、30年
  - `yfinance`
  - `^FVX` `^TNX` `^TYX` を利用し、値を 10 で割って `%` 表示する

### 4-8. 日本国債カテゴリ

現在の実装で対象としている日本国債利回りは次のとおりです。

- 日本国債2年利回り
- 日本国債5年利回り
- 日本国債10年利回り
- 日本国債30年利回り

取得方法:

- 主系
  - 財務省の国債金利 CSV
- 代替
  - Investing.com

実装仕様:

- 財務省 CSV の `2年` `5年` `10年` `30年` 列を読む
- 最新営業日値と前営業日値から前日比と騰落率を算出する
- 主系取得に失敗した年限だけ Investing.com で補完する

### 4-9. 商品カテゴリ

現在の実装で対象としている商品は次のとおりです。

- 金
- 銀
- WTI原油
- Brent原油
- 天然ガス
- 銅
- プラチナ
- パラジウム

取得方法:

- `yfinance`

### 4-10. 暗号資産カテゴリ

現在の実装で対象としている暗号資産は次のとおりです。

- BTC/USD
- BTC/JPY
- ETH/USD
- XRP/USD

取得方法:

- `yfinance`

### 4-11. ニュース取得

ニュース取得元:

- Reuters日本語
- Bloomberg日本語

取得方法:

- Google News RSS 検索結果を利用する
- RSS からタイトル、リンク、公開日時を読む
- タイトルの重複を除外する
- 日本語を含まないタイトルを除外する
- Bloomberg では quote 系ノイズタイトルを除外する

タイトル整形:

- `Reuters` `Bloomberg` などの末尾ノイズを除去する
- 余分な空白を整理する

取得件数:

- 各媒体ごとに最大 10 件

取得失敗時:

- その媒体の欄には `日本語ニュースを取得できませんでした。` または例外メッセージを表示する

## 5. データ欠損時の挙動

市場データやニュース取得に失敗しても、可能な範囲でページ生成を継続します。

挙動:

- 個別項目が取れない場合は `未取得` `未確認` を表示する
- サマリー全体の HTML と JSON は可能な限り生成を継続する
- 詳細はログへ出力する

## 6. デザインとレイアウト

### 6-1. 全体テーマ

- 明るい背景色を使う
- パネル背景は白
- 境界線は薄いグレー
- 当月は青枠で強調
- 日曜は赤
- 土曜は青
- 祝日は赤寄り
- 当日は黄系で強調

### 6-2. カレンダー画面レイアウト

- 広い画面では 4 列表示
- `1180px` 以下で 3 列表示
- `920px` 以下で 2 列表示
- `760px` 以下で 1 列表示

### 6-3. サマリー画面レイアウト

- カテゴリセクションは 2 列グリッド
- `1100px` 以下で 1 列表示
- 各テーブルは縦方向に `420px` までスクロール可能
- `760px` 以下では全体余白を縮小する

### 6-4. ファビコン

対応ファイル:

- `src/favicon.svg`
- `src/favicon.ico`
- `src/favicon.png`

存在するファイルだけを `<head>` に埋め込み、`dist/` にコピーします。

## 7. GitHub Actions 仕様

workflow:

- `.github/workflows/deploy.yml`
- `.github/workflows/deploy-calendar-midnight.yml`

実行内容:

1. リポジトリ取得
2. Python 3.12 セットアップ
3. `pip` 更新
4. `yfinance` と `requests` をインストール
5. `scripts/build_calendar.py` 実行
6. `scripts/build_world_summary.py` 実行
7. Pages 用アーティファクト作成
8. GitHub Pages へデプロイ

Node 実行環境:

- `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true`

schedule:

- `.github/workflows/deploy.yml`
  - `20:10 UTC` 平日
  - `21:10 UTC` 平日
- `.github/workflows/deploy-calendar-midnight.yml`
  - `15:00 UTC` 毎日（JST 0:00）

補足:

- 深夜0時JSTの再生成は専用 workflow に分離しており、既存の平日スケジュールは変更しない
- 深夜0時JSTの workflow では、`build_world_summary.py --force` を実行して `dist/summary/` を欠落させない

## 8. ローカル実行例

### 8-1. 依存関係

必要ライブラリ:

- `yfinance`
- `requests`

### 8-2. カレンダー生成

```bash
python scripts/build_calendar.py
```

### 8-3. 世界経済サマリー生成

通常実行:

```bash
python scripts/build_world_summary.py
```

強制実行:

```bash
python scripts/build_world_summary.py --force
```

### 8-4. 変更反映

```bash
git add README.md
git commit -m "READMEを現行仕様へ更新"
git push origin HEAD
```

## 9. 出力物

`dist/` には次が出力されます。

- `index.html`
- `1990.html` 〜 `2050.html`
- `style.css`
- `favicon.svg` など
- `summary/latest.html`
- `summary/latest.json`
- `summary/YYYY-MM-DD.html`
- `summary/YYYY-MM-DD.json`

## 10. 既知の制約

- 外部データは取得元の仕様変更や遅延の影響を受ける
- J-REIT と TOPIX は取得元により HTML 構造の差が大きく、補完取得に依存する場合がある
- 春分日と秋分日は近似計算
- 祝日制度の細部については主要ルールを実装しているが、全例外を網羅しているとは限らない
- GitHub Pages は静的配信なので、閲覧時にその場でリアルタイム API を呼び出す構成ではない
- ニュースはビルド時点の取得結果であり、ページ閲覧時に更新はされない

## 11. 変更時の確認ポイント

仕様変更時は最低でも次を確認してください。

- `scripts/build_calendar.py`
- `scripts/build_world_summary.py`
- `.github/workflows/deploy.yml`
- `src/style.css`
- `src/favicon.*`

特に依存がある項目:

- 年範囲変更
  - `build_calendar.py`
  - README
- サマリー対象銘柄変更
  - `build_world_summary.py`
  - README
- データ取得元変更
  - `build_world_summary.py`
  - README
- 定期実行時刻変更
  - `deploy.yml`
  - `build_world_summary.py`
  - README
- レイアウト変更
  - `src/style.css`
  - README
