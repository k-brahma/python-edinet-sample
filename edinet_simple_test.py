import os
from datetime import date, timedelta

import requests
from dotenv import load_dotenv

# 環境変数からAPIキーを取得
load_dotenv()
api_key = os.getenv("EDINET_API_KEY")

# 30日前の日付でAPIにリクエスト
test_date = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
url = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
params = {"date": test_date, "type": 2, "Subscription-Key": api_key}

# リクエスト実行と結果表示
response = requests.get(url, params=params, timeout=10)
print(f"ステータスコード: {response.status_code}")
print(f"取得件数: {len(response.json().get('results', []))}件")
print(f"APIキー: {api_key[:4]}...{api_key[-4:]}" if api_key else "APIキーが設定されていません")
