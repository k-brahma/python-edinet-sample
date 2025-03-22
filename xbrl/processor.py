"""
EDINET XBRLデータ取得・分析の実行モジュール

このモジュールは有価証券報告書からXBRLデータを取得し、財務分析を行います。
"""

import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd

from config import settings, base_settings
import xbrl.analyzer as analyzer
import xbrl.visualizer as visualizer

# ロガーの設定
logger = logging.getLogger(__name__)

# 共有スレッドプール
thread_pool = ThreadPoolExecutor(max_workers=10)


async def process_document(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    index: int,
    total: int,
    doc_id: str,
    company: str,
    doc_description: str,
    row: pd.Series,
) -> List[Dict[str, Any]]:
    """個別の書類を処理する

    Parameters
    ----------
    session : aiohttp.ClientSession
        HTTPリクエスト用のセッション
    semaphore : asyncio.Semaphore
        同時実行数を制限するセマフォ
    index : int
        現在の処理インデックス
    total : int
        総処理数
    doc_id : str
        書類ID
    company : str
        会社名
    doc_description : str
        書類の説明
    row : pandas.Series
        CSVファイルの行データ

    Returns
    -------
    list
        抽出された財務指標データのリスト
    """
    async with semaphore:  # 同時実行数を制限
        logger.info(f"\n[{index}/{total}] {company} - {doc_description} (ID: {doc_id})")

        # XBRLをダウンロード
        company_dir = os.path.join(visualizer.DOWNLOAD_DIR, company, doc_id)
        xbrl_files = await visualizer.download_xbrl(session, doc_id, company_dir, thread_pool)

        if not xbrl_files:
            logger.warning(f"XBRLファイルが見つかりませんでした: {doc_id}")
            return []

        # 有価証券報告書のXBRLファイルをフィルタリング
        report_files = visualizer.filter_report_files(xbrl_files)

        financial_data_list = []

        # 財務指標の抽出タスクを作成
        extract_tasks = []
        for xbrl_path in report_files:
            extract_tasks.append(
                analyzer.extract_financial_indicators_async(xbrl_path, thread_pool)
            )

        # 全ての抽出タスクを実行
        financial_data_results = await asyncio.gather(*extract_tasks, return_exceptions=True)

        for result in financial_data_results:
            if isinstance(result, Exception):
                logger.error(f"財務指標の抽出中に例外が発生しました: {result}")
                continue

            financial_data = result
            if financial_data:
                # CSVからの情報を追加（重要: 会社名を必ず上書きする）
                financial_data["会社名"] = company  # CSVの会社名を必ず使用
                financial_data["docID"] = doc_id
                financial_data["document_date"] = row.get("document_date", "")
                financial_data["docDescription"] = doc_description

                financial_data_list.append(financial_data)

                # 財務指標を表示
                indicators = ["売上高", "営業利益", "経常利益", "当期純利益"]
                indicator_values = []
                for indicator in indicators:
                    if indicator in financial_data:
                        indicator_values.append(f"{indicator}: {financial_data[indicator]}")

                if indicator_values:
                    logger.info("  " + ", ".join(indicator_values))

        return financial_data_list


async def process_securities_reports_async(
    csv_path: str, limit: Optional[int] = None
) -> List[Dict[str, Any]]:
    """CSVファイルから有価証券報告書の情報を読み込み、XBRLをダウンロードして分析する

    Parameters
    ----------
    csv_path : str
        有価証券報告書情報が含まれるCSVファイルのパス
    limit : int, optional
        処理する最大レコード数

    Returns
    -------
    list
        処理された財務指標データのリスト
    """
    # CSVファイルを読み込み
    df = pd.read_csv(csv_path)

    # ダウンロードディレクトリの作成
    os.makedirs(visualizer.DOWNLOAD_DIR, exist_ok=True)

    logger.info(f"{len(df)}件の有価証券報告書データを処理します")

    # 処理対象の制限（デバッグ用）
    if limit and limit > 0:
        df = df.head(limit)
        logger.info(f"処理対象を{limit}件に制限します")

    all_financial_data = []

    # 同時実行数を制限するセマフォを作成
    semaphore = asyncio.Semaphore(visualizer.MAX_CONCURRENT_REQUESTS)

    # セッション作成
    async with aiohttp.ClientSession() as session:
        # タスクリストを作成
        tasks = []

        for i, (_, row) in enumerate(df.iterrows(), 1):
            doc_id = row["docID"]
            company = row["filerName"]
            doc_description = row["docDescription"]

            # 各書類の処理タスクを作成
            task = process_document(
                session, semaphore, i, len(df), doc_id, company, doc_description, row
            )
            tasks.append(task)

        # 全タスクを実行して結果を待機
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 結果を集計（例外はログに記録し、有効なデータのみ集計）
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"書類処理中に例外が発生しました: {result}")
            elif result:
                all_financial_data.extend(result)

    # 結果をCSVファイルに保存
    if all_financial_data:
        output_file = base_settings.FINANCIAL_INDICATORS_CSV
        analyzer.save_financial_data_to_csv(all_financial_data, output_file)

        logger.info(f"\n抽出された財務指標データ: {len(all_financial_data)}件")
        logger.info(f"データは {output_file} に保存されました")

        # 会社ごとに年度別財務情報をピボットテーブルで表示
        await analyzer.create_pivot_tables_async(all_financial_data, thread_pool)

        # 財務データを基にグラフを作成
        try:
            trends_df = pd.DataFrame(all_financial_data)
            if "document_date" in trends_df.columns and "売上高_raw" in trends_df.columns:
                # 統計情報の作成と可視化
                trends_result = await analyzer.create_pivot_tables_async(
                    all_financial_data, thread_pool
                )

                if "trends_data" in trends_result and trends_result["trends_data"]:
                    trends_df = pd.DataFrame(trends_result["trends_data"])
                    await analyzer.create_charts_async(
                        trends_df, base_settings.CHARTS_DIR, thread_pool
                    )
        except Exception as e:
            logger.error(f"グラフ作成中にエラーが発生しました: {e}")
            # グラフ作成エラーは全体の処理を中断しない

    return all_financial_data


async def main_async():
    """メイン処理（非同期版）

    Returns
    -------
    int
        処理結果コード（0: 成功, 1: 失敗）
    """
    logger.info("有価証券報告書のXBRLデータ取得・解析ツール（非同期版）")

    # CSVファイルのパス
    csv_path = base_settings.FINAL_SECURITIES_REPORTS_CSV

    if not os.path.exists(csv_path):
        logger.error(f"CSVファイルが見つかりません: {csv_path}")

        # 他の候補ファイルを確認
        candidate_files = [
            base_settings.FILTERED_SECURITIES_REPORTS_CSV,
            base_settings.SECURITIES_REPORTS_CSV,
            base_settings.FILTERED_DOCUMENTS_CSV,
            base_settings.ALL_DOCUMENTS_CSV,
        ]

        for alt_path in candidate_files:
            if os.path.exists(alt_path):
                logger.info(f"代替ファイルを使用します: {alt_path}")
                csv_path = alt_path
                break
        else:
            logger.error("有価証券報告書データファイルが見つかりません")
            return 1

    try:
        # 処理を実行
        process_limit = None  # 制限を解除して全件処理
        financial_data = await process_securities_reports_async(csv_path, limit=process_limit)

        if not financial_data:
            logger.warning("財務データが抽出できませんでした")
            return 1

        logger.info("処理が正常に完了しました")
        return 0
    except FileNotFoundError as e:
        logger.error(f"必要なファイルが見つかりません: {e}")
        return 1
    except pd.errors.EmptyDataError:
        logger.error(f"CSVファイルにデータがありません: {csv_path}")
        return 1
    except Exception as e:
        logger.error(f"処理中に予期しないエラーが発生しました: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 1
    finally:
        # プログラム終了時にスレッドプールをシャットダウン
        thread_pool.shutdown(wait=False)


def main():
    """XBRLデータ取得・解析のメイン関数

    Returns
    -------
    int
        処理結果コード（0: 成功, 1: 失敗）
    """
    # ロギングの基本設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Windows環境での非同期処理の対応
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # メイン処理を実行
    return asyncio.run(main_async())


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
