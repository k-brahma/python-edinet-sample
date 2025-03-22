"""
EDINETデータ取得・分析用の基本設定ファイル（システム固有設定）

このモジュールは基本的に編集することはありません
"""

import os
from pathlib import Path

# ディレクトリ設定
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = os.path.join(BASE_DIR, "data")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
XBRL_DOWNLOAD_DIR = os.path.join(RESULTS_DIR, "xbrl")
CHARTS_DIR = os.path.join(RESULTS_DIR, "charts")

# EDINET API 設定
API_KEY_ENV_NAME = "EDINET_API_KEY"
EDINET_API_BASE_URL = "https://api.edinet-fsa.go.jp/api/v2"
EDINET_API_V1_BASE_URL = "https://api.edinet-fsa.go.jp/api/v1"
DOCUMENTS_ENDPOINT = f"{EDINET_API_BASE_URL}/documents.json"
DOCUMENT_ENDPOINT = f"{EDINET_API_BASE_URL}/documents"  # /{doc_id}?type=1 の形式で使用
DOCUMENT_INFO_ENDPOINT = f"{EDINET_API_V1_BASE_URL}/documents"  # /{doc_id} の形式で使用

# XBRL設定
XBRL_DOWNLOADER_MAX_CONCURRENT = 3  # XBRL同時ダウンロード数

# ファイルパス設定
# 新しいダウンロードURL
EDINETCODE_ZIP_URL = "https://disclosure.edinet-fsa.go.jp/E01EW/download?uji.verb=W1E62071EdinetCodeDownload&uji.bean=ee.bean.W1E62071.EEW1E62071Bean&TID=W1E62071&PID=W1E62071&SESSIONKEY=9999&downloadFileName=EdinetcodeDlInfo.zip&buttonClick=false&lgKbn=2&uji.Userid=&uji.Password="
EDINETCODE_ZIP_PATH = os.path.join(DATA_DIR, "EdinetcodeDlInfo.zip")

# 出力ファイル設定
COMPANY_INFO_JSON = os.path.join(RESULTS_DIR, "csv", "company_info.json")
ALL_DOCUMENTS_CSV = os.path.join(RESULTS_DIR, "csv", "all_documents.csv")
FILTERED_DOCUMENTS_CSV = os.path.join(RESULTS_DIR, "csv", "filtered_documents.csv")
SECURITIES_REPORTS_CSV = os.path.join(RESULTS_DIR, "csv", "securities_reports.csv")
FILTERED_SECURITIES_REPORTS_CSV = os.path.join(
    RESULTS_DIR, "csv", "filtered_securities_reports.csv"
)
FIXED_FILTERED_SECURITIES_REPORTS_CSV = os.path.join(
    RESULTS_DIR, "csv", "fixed_filtered_securities_reports.csv"
)
FINAL_SECURITIES_REPORTS_CSV = os.path.join(RESULTS_DIR, "csv", "final_securities_reports.csv")
FINANCIAL_INDICATORS_CSV = os.path.join(RESULTS_DIR, "csv", "financial_indicators.csv")
FINANCIAL_TRENDS_CSV = os.path.join(RESULTS_DIR, "csv", "financial_trends.csv")
ALL_COMPANIES_FINANCIAL_TRENDS_CSV = os.path.join(
    RESULTS_DIR, "csv", "all_companies_financial_trends.csv"
)

# ディレクトリの初期化
for directory in [DATA_DIR, RESULTS_DIR, XBRL_DOWNLOAD_DIR, CHARTS_DIR]:
    os.makedirs(directory, exist_ok=True)
