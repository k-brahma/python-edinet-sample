"""
EDINETデータ取得・分析用の設定ファイル
"""

import os
from pathlib import Path

# ディレクトリ設定
BASE_DIR = Path(__file__).parent
DATA_DIR = os.path.join(BASE_DIR, "data")
TEMP_DIR = os.path.join(DATA_DIR, "temp")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
XBRL_DOWNLOAD_DIR = os.path.join(RESULTS_DIR, "xbrl")
CHARTS_DIR = os.path.join(RESULTS_DIR, "charts")

# EDINET API 設定
API_KEY_ENV_NAME = "EDINET_API_KEY"
REQUEST_TIMEOUT = 30  # 秒
API_MAX_CONCURRENT = 5  # 同時リクエスト数上限

# XBRL設定
XBRL_DOWNLOADER_MAX_CONCURRENT = 3  # XBRL同時ダウンロード数

# ファイルパス設定
# 新しいダウンロードURL
EDINETCODE_ZIP_URL = "https://disclosure.edinet-fsa.go.jp/E01EW/download?uji.verb=W1E62071EdinetCodeDownload&uji.bean=ee.bean.W1E62071.EEW1E62071Bean&TID=W1E62071&PID=W1E62071&SESSIONKEY=9999&downloadFileName=EdinetcodeDlInfo.zip&buttonClick=false&lgKbn=2&uji.Userid=&uji.Password="
EDINETCODE_ZIP_PATH = os.path.join(DATA_DIR, "EdinetcodeDlInfo.zip")
EDINET_CODE_LIST_CSV = os.path.join(TEMP_DIR, "EdinetcodeDlInfo.csv")

# 出力ファイル設定
COMPANY_INFO_JSON = os.path.join(RESULTS_DIR, "csv","company_info.json")
ALL_DOCUMENTS_CSV = os.path.join(RESULTS_DIR, "csv","all_documents.csv")
FILTERED_DOCUMENTS_CSV = os.path.join(RESULTS_DIR, "csv","filtered_documents.csv")
SECURITIES_REPORTS_CSV = os.path.join(RESULTS_DIR, "csv","securities_reports.csv")
FILTERED_SECURITIES_REPORTS_CSV = os.path.join(RESULTS_DIR, "csv","filtered_securities_reports.csv")
FIXED_FILTERED_SECURITIES_REPORTS_CSV = os.path.join(RESULTS_DIR, "csv","fixed_filtered_securities_reports.csv")
FINAL_SECURITIES_REPORTS_CSV = os.path.join(RESULTS_DIR, "csv","final_securities_reports.csv")
FINANCIAL_INDICATORS_CSV = os.path.join(RESULTS_DIR, "csv","financial_indicators.csv")
FINANCIAL_TRENDS_CSV = os.path.join(RESULTS_DIR, "csv","financial_trends.csv")
ALL_COMPANIES_FINANCIAL_TRENDS_CSV = os.path.join(RESULTS_DIR, "csv","all_companies_financial_trends.csv")

# 調査設定
START_YEAR = 2018
END_YEAR = 2023
TARGET_MONTH = 6

# 企業名の正式名称マッピング
COMPANY_FULL_NAMES = {
    "トヨタ": "トヨタ自動車株式会社",
    "ホンダ": "本田技研工業株式会社",
    "日産": "日産自動車株式会社", 
    "スズキ": "スズキ株式会社",
    "マツダ": "マツダ株式会社",
    # "三菱自動車": "三菱自動車工業株式会社",
    # "SUBARU": "株式会社SUBARU",
    # "いすゞ": "いすゞ自動車株式会社",
    # "日野": "日野自動車株式会社",
}

# グラフ設定
CHART_TOP_COMPANIES = 5  # グラフに表示する上位企業数

# ディレクトリの初期化
for directory in [DATA_DIR, TEMP_DIR, RESULTS_DIR, XBRL_DOWNLOAD_DIR, CHARTS_DIR]:
    os.makedirs(directory, exist_ok=True)
