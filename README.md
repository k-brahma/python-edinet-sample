# EDINET企業情報取得・財務分析ツール

このツールは、金融庁が提供するEDINETシステムから上場企業（特に自動車メーカー）の有価証券報告書データを取得し、財務情報を自動的に抽出・分析するプログラムです。財務指標の推移をグラフ化し、企業間比較や個別企業の財務状況を視覚的に把握することができます。

## 主な機能

1. **企業情報の取得**
   - EDINETコードリストから自動車メーカーなどの特定業種の企業情報を抽出
   - 取得した企業情報をJSON形式で保存

2. **有価証券報告書の検索**
   - 指定した期間内の有価証券報告書をEDINETから検索
   - 対象企業の書類をフィルタリング

3. **XBRLデータの取得と分析**
   - 有価証券報告書からXBRLデータをダウンロード
   - 財務情報（売上高、営業利益、経常利益、当期純利益など）を自動抽出
   - 企業ごとの財務指標の年度別推移を集計

4. **財務データの可視化**
   - 企業間の財務指標比較グラフの作成（売上高、営業利益、当期純利益）
   - 個別企業の財務指標推移グラフの作成

## ディレクトリ構成

```
edinet/
│
├── collector/               # データ収集関連モジュール
│   ├── companies.py         # 企業情報取得モジュール
│   └── documents.py         # 書類情報取得モジュール
│
├── config/                  # 設定ファイル
│   ├── base_settings.py     # 基本設定
│   └── settings.py          # ユーザー設定
│
├── edinet/                  # EDINETアクセス関連
│   ├── edinet_core.py       # EDINET API コア機能
│   └── document_processor.py # 書類処理機能
│
├── xbrl/                    # XBRL処理関連
│   ├── analyzer.py          # XBRLデータ分析
│   ├── processor.py         # XBRLデータ処理
│   └── visualizer.py        # データ可視化
│
├── data/                    # 入力データ保存ディレクトリ
│   └── EdinetcodeDlInfo.zip # EDINETコードリスト（必須）
│
├── results/                 # 出力結果
│   ├── csv/                 # CSVデータ
│   └── charts/              # 生成されたグラフ
│
├── main.py                  # メイン実行スクリプト
├── requirements.txt         # 必要なライブラリリスト
├── .env                     # 環境変数設定ファイル
└── .env.sample              # 環境変数設定サンプル
```

## 必要環境

- Python 3.8以上
- 必要なライブラリ（requirements.txtに記載）

## セットアップ

1. リポジトリをクローン
   ```
   git clone https://github.com/yourusername/edinet-analyzer.git
   cd edinet-analyzer
   ```

2. 仮想環境の作成と有効化（任意）
   ```
   python -m venv venv
   # Windows
   venv\Scripts\activate
   # macOS/Linux
   source venv/bin/activate
   ```

3. 必要なライブラリのインストール
   ```
   pip install -r requirements.txt
   ```

4. EDINETコードリストの準備（必須）
   - EDINET公式サイト（https://disclosure.edinet-fsa.go.jp/）にアクセス
   - トップページの「コード一覧・EDINET提出書類公開サイト」リンクをクリック
   - ページ中央下部の「EDINETコード一覧」ボタンをクリック
   - ダウンロードが開始されます。ファイル名は「EdinetcodeDlInfo.zip」となります
   - ダウンロードしたZIPファイルを `data/EdinetcodeDlInfo.zip` に配置してください
   - 注意：EDINETコードリストは定期的に更新されるため、最新のデータを使用することをお勧めします
   - このZIPファイルにはCSV形式の企業情報が含まれており、本ツールの企業情報抽出に必須です

5. EDINET APIキーの設定（オプション）
   - `.env.sample`ファイルを`.env`にコピーして編集
   ```
   EDINET_API_KEY=あなたのAPIキー
   ```
   - APIキーがなくても基本機能は動作しますが、一部の機能が制限される場合があります

## 使い方

### 前提条件

このプログラムを実行する前に、必ず以下の準備が必要です：

1. EDINETコードリストのZIPファイルが `data/EdinetcodeDlInfo.zip` に配置されていること
2. 必要なPythonライブラリがインストールされていること

### 基本的な実行方法

すべての処理を一括実行するには、メインスクリプトを実行します：

```
python main.py
```

このコマンドにより以下の処理が順番に実行されます：
1. 企業情報の取得（collector/companies.py）
2. 有価証券報告書の検索（edinet/document_processor.py）
3. XBRLデータの取得と財務情報抽出（xbrl/processor.py、xbrl/analyzer.py）
4. 財務データの可視化（xbrl/visualizer.py）

### 接続テスト

EDINET APIへの接続をテストするには、以下のコマンドを実行します：

```
python connection_test.py
```

### カスタム設定

`config/settings.py`ファイルを編集することで、様々な設定をカスタマイズできます：

- 分析対象の企業設定
- データ取得期間
- データ取得対象月
- グラフに表示する上位企業数

## 出力ファイル

実行後、以下のディレクトリとファイルが生成されます：

- `results/csv/company_info.json`: 対象企業の基本情報
- `results/csv/filtered_securities_reports.csv`: 検索された有価証券報告書情報
- `results/csv/financial_indicators.csv`: 抽出された財務指標データ
- `results/csv/all_companies_financial_trends.csv`: 全企業の財務推移データ
- `results/charts/`: 財務指標比較グラフ
  - `revenue_comparison.png`: 売上高比較
  - `operating_income_comparison.png`: 営業利益比較
  - `net_income_comparison.png`: 当期純利益比較
- `results/charts/individual/`: 個別企業の財務推移グラフ

## 注意事項

- EDINETの仕様変更により、一部の機能が動作しなくなる可能性があります
- データの取得量や頻度が多すぎると、EDINETサーバーへの負荷が大きくなる可能性があるため、適切な間隔をあけて使用してください
- 抽出されたデータは自動処理による概算値であり、正確性を保証するものではありません
- 日本語フォントの表示にはjapanize-matplotlibパッケージを使用しています

## トラブルシューティング

1. **企業情報が正しく取得できない場合**
   - EDINETコードリストが正しく配置されているか確認してください（`data/EdinetcodeDlInfo.zip`）
   - ZIPファイルの内容が最新かつ有効であることを確認してください
   - `config/settings.py`の企業設定を正確に指定してください

2. **グラフの日本語が文字化けする場合**
   - japanize-matplotlibが正しくインストールされているか確認してください
   - 必要に応じて追加の日本語フォントをインストールしてください

3. **API接続エラーが発生する場合**
   - `connection_test.py`を実行してAPI接続を確認してください
   - `.env`ファイルのAPIキー設定を確認してください
   - インターネット接続状態を確認してください 

4. **EDINETコードリストが見つからない場合**
   - EDINETのトップページデザインが変更されている可能性があります
   - 「EDINETコード一覧」や「コード一覧」などのキーワードでページ内を検索してください
   - あるいはEDINETのヘルプページや「よくある質問」から該当するダウンロードページを探してください
   - ダウンロードしたファイル名が異なる場合は、`config/base_settings.py`の`EDINETCODE_ZIP_PATH`設定を変更してください