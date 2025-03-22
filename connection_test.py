"""
EDINET APIのシンプルなテスト

APIキーを環境変数から取得し、指定された日付のデータを取得します。

期待される結果:
ステータスコード: 200
取得件数: 591件
APIキー: part-of-your-api-key

"""

import os

import requests
from dotenv import load_dotenv

from config import base_settings, settings

# 環境変数からAPIキーを取得
load_dotenv()
api_key = os.getenv(base_settings.API_KEY_ENV_NAME)

# 確実に値が存在する日付（2024年6月20日）でAPIにリクエスト
test_date = "2024-06-20"
params = {"date": test_date, "type": 2, "Subscription-Key": api_key}

# リクエスト実行と結果表示
response = requests.get(
    base_settings.DOCUMENTS_ENDPOINT, params=params, timeout=settings.REQUEST_TIMEOUT
)
print(f"ステータスコード: {response.status_code}")
print(f"取得件数: {len(response.json().get('results', []))}件")
print(f"APIキー: {api_key[:4]}...{api_key[-4:]}" if api_key else "APIキーが設定されていません")
