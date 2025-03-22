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

4. EDINET APIキーの設定（オプション）
   - `.env`ファイルを作成し、以下の内容を記述
   ```
   EDINET_API_KEY=あなたのAPIキー
   ```
   - APIキーがなくても基本機能は動作しますが、一部の機能が制限される場合があります

## 使い方

### 基本的な実行方法

すべての処理を一括実行するには、メインスクリプトを実行します：

```
python main.py
```

このコマンドにより以下の処理が順番に実行されます：
1. 企業情報の取得（companies.py）
2. 有価証券報告書の検索（research.py）
3. XBRLデータの取得と財務情報抽出（xbrl_getter_async.py）

### カスタム設定

`config.py`ファイルを編集することで、様々な設定をカスタマイズできます：

- `AUTO_MANUFACTURERS`: 分析対象の企業名リスト
- `START_YEAR`, `END_YEAR`: データ取得期間
- `CHART_TOP_COMPANIES`: グラフに表示する上位企業数

### ステップごとの個別実行

特定のステップのみを実行したい場合は、以下のように個別のスクリプトを実行できます：

1. 企業情報の取得のみ
   ```
   python companies.py
   ```

2. 有価証券報告書の検索のみ
   ```
   python research.py
   ```

3. XBRLデータの取得と分析のみ
   ```
   python xbrl_getter_async.py
   ```

## 出力ファイル

実行後、以下のディレクトリとファイルが生成されます：

- `results/csv/company_info.json`: 対象企業の基本情報
- `results/csv/fixed_filtered_securities_reports.csv`: 検索された有価証券報告書情報
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
   - EDINETコードリストを手動でダウンロードして、`data/EdinetcodeDlInfo.zip`に配置してください
   - `config.py`の`AUTO_MANUFACTURERS`リストの企業名を正確に指定してください

2. **グラフの日本語が文字化けする場合**
   - japanize-matplotlibが正しくインストールされているか確認してください
   - 必要に応じて追加の日本語フォントをインストールしてください

3. **特定の企業データが表示されない場合**
   - `config.py`のリストに対象企業が含まれているか確認してください
   - 企業名の表記が正確かどうか（「トヨタ」→「トヨタ自動車株式会社」など）確認してください 