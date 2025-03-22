"""
EDINET APIを使用してデータを取得するコアモジュール

このモジュールはEDINET APIとの通信を担当し、データの取得と基本的な変換を行います。
"""

import asyncio
import csv
import datetime
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp
from dotenv import load_dotenv

from config import base_settings, settings

# ロガーの設定
logger = logging.getLogger(__name__)

# 環境変数のロード
load_dotenv()

# EDINET API Key
EDINET_API_KEY = os.getenv(base_settings.API_KEY_ENV_NAME)

# 同時に実行するリクエスト数
MAX_CONCURRENT_REQUESTS = settings.API_MAX_CONCURRENT


async def fetch_documents_for_date(session, date_str, semaphore):
    """非同期で特定の日付の全書類を取得する

    Parameters
    ----------
    session : aiohttp.ClientSession
        HTTPリクエストを行うためのセッションオブジェクト
    date_str : str
        日付文字列 (YYYY-MM-DD)
    semaphore : asyncio.Semaphore
        同時リクエスト数を制限するためのセマフォ

    Returns
    -------
    list
        日付と書類データのタプルのリスト [(日付, 書類データ), ...]
    """
    url = base_settings.DOCUMENTS_ENDPOINT
    params = {"date": date_str, "type": 2, "Subscription-Key": EDINET_API_KEY}

    async with semaphore:
        try:
            async with session.get(
                url, params=params, timeout=settings.REQUEST_TIMEOUT
            ) as response:
                response.raise_for_status()
                data = await response.json()

                results = data.get("results", [])
                if results:
                    logger.info(f"  {date_str}: {len(results)}件の書類を取得しました")
                    return [(date_str, doc) for doc in results]
                else:
                    logger.info(f"  {date_str}: 書類はありませんでした")
                    return []

        except Exception as e:
            logger.error(f"  {date_str} でエラー発生: {e}")
            return []


async def collect_documents_for_period_async(start_date, end_date):
    """指定された期間内の全提出書類を非同期で収集

    Parameters
    ----------
    start_date : datetime.date
        検索開始日
    end_date : datetime.date
        検索終了日

    Returns
    -------
    list
        検索結果の書類リスト [(日付, 書類データ), ...]
    """
    # 検索対象の日付リストを作成
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += datetime.timedelta(days=1)

    logger.info(f"{start_date}から{end_date}までの{len(date_list)}日間の書類を一括検索します")

    # 同時接続数を制限するためのセマフォを作成
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # 結果を保存するリスト
    all_documents = []

    # 非同期リクエストを実行
    async with aiohttp.ClientSession() as session:
        tasks = []
        for target_date in date_list:
            date_str = target_date.strftime("%Y-%m-%d")
            task = fetch_documents_for_date(session, date_str, semaphore)
            tasks.append(task)

        # すべてのタスクを実行し結果を待機
        logger.info(f"全{len(tasks)}日分のデータを同時に取得中...")
        results = await asyncio.gather(*tasks)

        # 結果をフラット化して結合
        for date_docs in results:
            all_documents.extend(date_docs)

    return all_documents


def get_date_ranges_for_years(start_year=None, end_year=None, month=None):
    """指定された年の範囲について、各年の特定月の日付範囲を生成する

    Parameters
    ----------
    start_year : int, optional
        開始年
    end_year : int, optional
        終了年
    month : int, optional
        対象月（デフォルトは6月）

    Returns
    -------
    list
        (年, 開始日, 終了日)のタプルのリスト
    """
    # デフォルト値を設定
    if start_year is None:
        start_year = settings.START_YEAR
    if end_year is None:
        end_year = settings.END_YEAR
    if month is None:
        month = settings.TARGET_MONTH

    date_ranges = []

    for year in range(start_year, end_year + 1):
        # 指定月の初日
        start_date = datetime.date(year, month, 1)

        # 指定月の最終日（次の月の初日から1日引く）
        if month == 12:
            next_month = datetime.date(year + 1, 1, 1)
        else:
            next_month = datetime.date(year, month + 1, 1)
        end_date = next_month - datetime.timedelta(days=1)

        date_ranges.append((year, start_date, end_date))

    return date_ranges


def ensure_dir(directory):
    """ディレクトリが存在しない場合は作成する

    Parameters
    ----------
    directory : str
        作成するディレクトリのパス

    Returns
    -------
    None
    """
    if directory:
        os.makedirs(directory, exist_ok=True)


def load_json_file(file_path):
    """JSONファイルを読み込む

    Parameters
    ----------
    file_path : str
        JSONファイルのパス

    Returns
    -------
    dict or list
        読み込んだJSONデータ
    None
        読み込みに失敗した場合

    Raises
    ------
    FileNotFoundError
        ファイルが見つからない場合
    json.JSONDecodeError
        JSONデータの形式が不正な場合
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        logger.error(f"ファイルが見つかりません: {file_path}")
        return None
    except json.JSONDecodeError:
        logger.error(f"JSONの形式が不正です: {file_path}")
        return None
    except Exception as e:
        logger.error(f"ファイルの読み込みエラー: {e}")
        return None


def save_to_json(data, filepath):
    """結果をJSONファイルに保存

    Parameters
    ----------
    data : dict or list
        保存するデータ
    filepath : str
        保存先ファイルパス

    Returns
    -------
    bool
        保存成功の場合True、失敗の場合False

    Raises
    ------
    IOError
        ファイルの書き込みに失敗した場合
    """
    if not data:
        logger.warning(f"保存するデータがありません: {filepath}")
        return False

    # ディレクトリが存在しなければ作成
    ensure_dir(os.path.dirname(filepath))

    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info(f"データを{filepath}に保存しました（{len(data)}件）")
        return True
    except Exception as e:
        logger.error(f"JSONファイルの保存に失敗しました: {e}")
        return False


def save_to_csv(data, filepath, date_field=True):
    """結果をCSVファイルに保存

    Parameters
    ----------
    data : list
        保存するデータ（辞書のリストまたはタプル）
    filepath : str
        保存先ファイルパス
    date_field : bool, optional
        日付フィールドを含めるかどうか（デフォルト: True）

    Returns
    -------
    bool
        保存成功の場合True、失敗の場合False

    Raises
    ------
    IOError
        ファイルの書き込みに失敗した場合
    """
    if not data:
        logger.warning(f"保存するデータがありません: {filepath}")
        return False

    # ディレクトリが存在しなければ作成
    ensure_dir(os.path.dirname(filepath))

    try:
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            if isinstance(data[0], tuple) and date_field:
                # [(日付, 書類データ), ...] 形式の場合
                # 日付フィールドを追加して書き込む
                fieldnames = ["document_date"] + list(data[0][1].keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for date_str, doc in data:
                    row = {"document_date": date_str}
                    row.update(doc)
                    writer.writerow(row)
            else:
                # 通常の辞書リストの場合
                fieldnames = data[0].keys()
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)

        logger.info(f"データを{filepath}に保存しました（{len(data)}件）")
        return True
    except Exception as e:
        logger.error(f"CSVファイルの保存に失敗しました: {e}")
        return False


def safe_copy_file(src, dst):
    """ファイルを安全にコピーする

    Parameters
    ----------
    src : str
        コピー元ファイルパス
    dst : str
        コピー先ファイルパス

    Returns
    -------
    bool
        コピー成功の場合True、失敗の場合False

    Raises
    ------
    OSError
        ファイルコピー操作が失敗した場合
    """
    try:
        import shutil

        shutil.copy(src, dst)
        logger.info(f"ファイルをコピーしました: {src} → {dst}")
        return True
    except Exception as e:
        logger.error(f"ファイルコピーに失敗しました: {e}")
        return False
