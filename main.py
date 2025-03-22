"""
EDINET企業情報取得・有価証券報告書データ解析ツール

このプログラムは以下の流れで動作します：
1. 企業情報の取得と保存（companies.py）
2. 有価証券報告書の検索・処理（document_processor.py）
3. XBRLデータの取得と財務情報抽出（xbrl.processor）
"""

import logging
import os

from collector import companies
from config import base_settings
from edinet import document_processor
from xbrl import processor  # 新しいパッケージ構造を使用

# ロガーの設定
logger = logging.getLogger(__name__)


def create_required_directories():
    """必要なディレクトリを作成する

    Returns
    -------
    None

    Raises
    ------
    OSError
        ディレクトリの作成に失敗した場合
    """
    directories = [
        base_settings.DATA_DIR,
        base_settings.RESULTS_DIR,
        base_settings.XBRL_DOWNLOAD_DIR,
        base_settings.CHARTS_DIR,
    ]

    for directory in directories:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"ディレクトリを確認/作成しました: {directory}")


def get_company_info():
    """企業情報の取得と保存を行う

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        企業情報の取得に失敗した場合
    """
    logger.info("\n企業情報の取得と保存を実行します")

    result = companies.main()
    if result != 0:
        raise RuntimeError("企業情報の取得に失敗しました")

    logger.info("企業情報の取得が完了しました")


def search_documents():
    """有価証券報告書の検索・処理を行う

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        企業情報ファイルが見つからない場合
    RuntimeError
        有価証券報告書の検索・処理に失敗した場合
    """
    logger.info("\n有価証券報告書の検索・処理を実行します")

    # 企業情報ファイルの確認
    if not os.path.exists(base_settings.COMPANY_INFO_JSON):
        error_msg = f"企業情報ファイル '{base_settings.COMPANY_INFO_JSON}' が見つかりません"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    result = document_processor.main()
    if result != 0:
        raise RuntimeError("有価証券報告書の検索・処理に失敗しました")

    logger.info("有価証券報告書の検索・処理が完了しました")


def extract_financial_data():
    """XBRLデータの取得と財務情報抽出を行う

    Returns
    -------
    None

    Raises
    ------
    FileNotFoundError
        有価証券報告書データファイルが見つからない場合
    RuntimeError
        XBRLデータの取得と財務情報抽出に失敗した場合
    """
    logger.info("\nXBRLデータの取得と財務情報抽出を実行します")

    # xbrl.processor.main() を呼び出す（xbrl_getter_async.main() の代わりに）
    result = processor.main()
    if result != 0:
        raise RuntimeError("XBRLデータの取得と財務情報抽出に失敗しました")

    logger.info("XBRLデータの取得と財務情報抽出が完了しました")


def show_results_summary():
    """処理結果の概要を表示する

    Returns
    -------
    None
    """
    logger.info("\n===== 処理結果の概要 =====")

    # 結果ファイルの存在確認と表示
    files_to_check = {
        "企業情報": base_settings.COMPANY_INFO_JSON,
        "有価証券報告書データ（全件）": base_settings.ALL_DOCUMENTS_CSV,
        "有価証券報告書データ（フィルタリング済み）": base_settings.FILTERED_DOCUMENTS_CSV,
        "有価証券報告書データ（最終版）": base_settings.FINAL_SECURITIES_REPORTS_CSV,
        "財務指標データ": base_settings.FINANCIAL_INDICATORS_CSV,
        "財務推移データ": base_settings.ALL_COMPANIES_FINANCIAL_TRENDS_CSV,
    }

    logger.info("結果ファイル:")
    for description, file_path in files_to_check.items():
        status = "✓ 存在します" if os.path.exists(file_path) else "✗ 存在しません"
        logger.info(f"- {description}: {file_path} ({status})")

    # ディレクトリの確認
    directories = {
        "結果ディレクトリ": base_settings.RESULTS_DIR,
        "XBRLデータディレクトリ": base_settings.XBRL_DOWNLOAD_DIR,
        "チャートディレクトリ": base_settings.CHARTS_DIR,
    }

    logger.info("\nディレクトリ:")
    for description, dir_path in directories.items():
        status = "✓ 存在します" if os.path.exists(dir_path) else "✗ 存在しません"
        if os.path.exists(dir_path):
            file_count = len(
                [
                    name
                    for name in os.listdir(dir_path)
                    if os.path.isfile(os.path.join(dir_path, name))
                ]
            )
            status += f" ({file_count}ファイル)"
        logger.info(f"- {description}: {dir_path} ({status})")


def main():
    """EDINET企業情報取得・有価証券報告書データ解析のメイン処理

    Returns
    -------
    int
        処理結果コード (0: 成功)

    Raises
    ------
    Various exceptions can be raised during the process
    """
    logger.info("===== EDINET企業情報取得・有価証券報告書データ解析ツール =====")

    # ディレクトリの準備
    create_required_directories()

    # 企業情報の取得
    get_company_info()

    # 有価証券報告書の検索・処理
    search_documents()

    # XBRLデータの取得と財務情報抽出
    extract_financial_data()

    # 結果の概要を表示
    show_results_summary()

    # 全処理の完了
    logger.info("\n===== 処理が完了しました =====")

    return 0


if __name__ == "__main__":
    # ロギングの基本設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    main()
