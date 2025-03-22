"""
EDINET XBRLデータ取得・解析のコア機能

このモジュールはXBRLデータの取得と基本的な解析機能を提供します。
"""

import asyncio
import logging
import os
import zipfile
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv

from config import base_settings, settings

# ロガーの設定
logger = logging.getLogger(__name__)

# 環境変数のロード
load_dotenv()

# EDINET API Key
EDINET_API_KEY = os.getenv(base_settings.API_KEY_ENV_NAME)
if not EDINET_API_KEY:
    logger.error(
        f"{base_settings.API_KEY_ENV_NAME}が設定されていません。.envファイルに{base_settings.API_KEY_ENV_NAME}=あなたのキーを設定してください。"
    )
    # サンプル用にダミーキーを設定（実際は機能しません）
    EDINET_API_KEY = "dummy_key"

# ダウンロード先ディレクトリ
DOWNLOAD_DIR = base_settings.XBRL_DOWNLOAD_DIR
# 同時実行数
MAX_CONCURRENT_REQUESTS = base_settings.XBRL_DOWNLOADER_MAX_CONCURRENT


async def get_document_info(
    session: aiohttp.ClientSession, doc_id: str
) -> Optional[Dict[str, Any]]:
    """書類IDから書類の詳細情報を取得する

    Parameters
    ----------
    session : aiohttp.ClientSession
        HTTPリクエストを行うためのセッションオブジェクト
    doc_id : str
        書類ID

    Returns
    -------
    dict or None
        書類の詳細情報を含む辞書、または取得に失敗した場合はNone
    """
    url = f"{base_settings.DOCUMENT_INFO_ENDPOINT}/{doc_id}"
    headers = {"X-API-KEY": EDINET_API_KEY}

    async with session.get(url, headers=headers, timeout=settings.REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        return await response.json()


async def download_xbrl(
    session: aiohttp.ClientSession,
    doc_id: str,
    save_dir: Optional[str] = None,
    thread_pool: Optional[ThreadPoolExecutor] = None,
) -> List[str]:
    """書類IDを使用してXBRLデータをダウンロードする

    Parameters
    ----------
    session : aiohttp.ClientSession
        HTTPリクエストを行うためのセッションオブジェクト
    doc_id : str
        書類ID
    save_dir : str, optional
        保存先ディレクトリ、指定しない場合はDOWNLOAD_DIR/doc_idが使用される
    thread_pool : ThreadPoolExecutor, optional
        ファイル操作に使用するスレッドプール

    Returns
    -------
    list
        ダウンロードされたXBRLファイルのパスのリスト
    """
    url = f"{base_settings.DOCUMENT_ENDPOINT}/{doc_id}?type=1"
    params = {"Subscription-Key": EDINET_API_KEY}

    logger.info(f"書類 {doc_id} をダウンロード中...")
    async with session.get(url, params=params, timeout=settings.REQUEST_TIMEOUT) as response:
        response.raise_for_status()
        content = await response.read()

    # ダウンロードディレクトリの作成
    if save_dir is None:
        save_dir = os.path.join(DOWNLOAD_DIR, doc_id)

    os.makedirs(save_dir, exist_ok=True)

    # ZIPファイルとして保存して展開
    zip_path = os.path.join(save_dir, f"{doc_id}.zip")
    with open(zip_path, "wb") as f:
        f.write(content)

    # ZIPファイルを展開
    loop = asyncio.get_running_loop()
    if thread_pool:
        await loop.run_in_executor(thread_pool, extract_zip, zip_path, save_dir)
    else:
        # スレッドプールが指定されていない場合はデフォルトのエグゼキュータを使用
        await loop.run_in_executor(None, extract_zip, zip_path, save_dir)

    logger.info(f"書類 {doc_id} を {save_dir} に保存しました")

    # XBRLファイルのパスを返す
    if thread_pool:
        xbrl_files = await loop.run_in_executor(thread_pool, find_xbrl_files, save_dir)
    else:
        xbrl_files = await loop.run_in_executor(None, find_xbrl_files, save_dir)

    if not xbrl_files:
        logger.warning(f"書類 {doc_id} にXBRLファイルが見つかりませんでした")

    return xbrl_files


def extract_zip(zip_path: str, extract_dir: str) -> None:
    """ZIPファイルを展開する

    Parameters
    ----------
    zip_path : str
        ZIPファイルのパス
    extract_dir : str
        展開先ディレクトリ
    """
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)


def find_xbrl_files(directory: str) -> List[str]:
    """ディレクトリ内のXBRLファイルを再帰的に検索

    Parameters
    ----------
    directory : str
        検索対象のディレクトリ

    Returns
    -------
    list
        見つかったXBRLファイルのパスのリスト
    """
    xbrl_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".xbrl"):
                xbrl_files.append(os.path.join(root, file))
    return xbrl_files


def filter_report_files(xbrl_files: List[str]) -> List[str]:
    """有価証券報告書のXBRLファイルをフィルタリングする

    Parameters
    ----------
    xbrl_files : list
        XBRLファイルのパスのリスト

    Returns
    -------
    list
        有価証券報告書と思われるXBRLファイルのパスのリスト
    """
    # 有価証券報告書のXBRLファイルをフィルタリング（jpcrpを含むファイル）
    report_files = [file for file in xbrl_files if "jpcrp" in os.path.basename(file).lower()]

    # 見つからない場合は元のファイルリストを返す
    if not report_files and xbrl_files:
        logger.warning(
            "有価証券報告書のXBRLファイルが見つかりませんでした。他のXBRLファイルを使用します。"
        )
        return xbrl_files

    return report_files
