"""
EDINETデータ取得・分析用のユーザー設定ファイル
"""

# 調査設定
START_YEAR = 2018
END_YEAR = 2024
TARGET_MONTH = 6

# リクエスト設定
REQUEST_TIMEOUT = 30

# 同時リクエスト数上限
API_MAX_CONCURRENT = 5

# グラフに表示する上位企業数
CHART_TOP_COMPANIES = 5

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
