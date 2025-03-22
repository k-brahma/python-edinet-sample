"""
EDINET書類のデータ処理を行うモジュール

このモジュールはEDINETから取得した書類データの処理を行います。
企業情報のフィルタリング、有価証券報告書の抽出、訂正報告書の処理などを担当します。
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from config import base_settings, settings
from edinet import edinet_core

# ロガーの設定
logger = logging.getLogger(__name__)


def load_companies(file_path=None):
    """企業情報をJSONファイルから読み込む

    Args:
        file_path: 企業情報JSONファイルのパス（デフォルトはbase_settings.COMPANY_INFO_JSON）

    Returns:
        list: 企業情報のリスト
    """
    if file_path is None:
        file_path = base_settings.COMPANY_INFO_JSON

    companies = edinet_core.load_json_file(file_path)

    if not companies:
        return []

    # 読み込んだデータが文字列の場合（エラーメッセージなど）はリストに変換
    if not isinstance(companies, list):
        logger.warning(f"警告: 企業情報のJSONファイルの形式が不正です: {companies}")
        return []

    logger.info(f"{len(companies)}社の企業情報を読み込みました")
    return companies


def extract_company_documents(all_documents, seccode):
    """全書類から特定の証券コードの企業の書類を抽出する

    Args:
        all_documents: 日付と書類データのタプルのリスト [(日付, 書類データ), ...]
        seccode: 証券コード

    Returns:
        list: 抽出された書類のリスト [(日付, 書類データ), ...]
    """
    # 証券コードを文字列化
    seccode_str = str(seccode)

    company_docs = []
    for date_str, doc in all_documents:
        # 文字列に変換して比較
        if str(doc.get("secCode", "")) == seccode_str:
            company_docs.append((date_str, doc))
    return company_docs


def summarize_document_types(documents):
    """書類の種類ごとの集計を行う

    Args:
        documents: 日付と書類データのタプルのリスト [(日付, 書類データ), ...]

    Returns:
        dict: 書類種別ごとの件数 {"種別": 件数, ...}
    """
    doc_types = {}
    for _, doc in documents:
        doc_type = doc.get("docDescription", "不明")
        if doc_type in doc_types:
            doc_types[doc_type] += 1
        else:
            doc_types[doc_type] = 1
    return doc_types


def filter_securities_reports(documents):
    """有価証券報告書のみをフィルタリングする

    Args:
        documents: 日付と書類データのタプルのリスト [(日付, 書類データ), ...]

    Returns:
        list: フィルタリングされた書類のリスト [(日付, 書類データ), ...]
    """
    filtered_docs = []
    for date_str, doc in documents:
        doc_description = doc.get("docDescription", "")
        if "有価証券報告書" in doc_description:
            filtered_docs.append((date_str, doc))
    return filtered_docs


def filter_corrected_reports(df):
    """訂正報告書がある場合は元の報告書を削除する

    Args:
        df: pandas DataFrame

    Returns:
        pandas.DataFrame: フィルタリング後のDataFrame
    """
    # 元のデータ数を記録
    original_count = len(df)

    # referenceDocIDカラムが存在するか確認
    if "referenceDocID" not in df.columns:
        logger.warning(
            "referenceDocIDカラムが見つかりません。訂正報告書フィルタリングをスキップします。"
        )
        return df

    # 訂正報告書が参照している元の報告書IDを取得
    try:
        corrected_doc_ids = (
            df[df["docDescription"].str.contains("訂正", na=False)]["referenceDocID"]
            .dropna()
            .unique()
        )

        # 参照されている元の報告書をドロップ
        filtered_df = df[~df["docID"].isin(corrected_doc_ids)]

        # 統計情報
        filtered_count = len(filtered_df)
        removed_count = original_count - filtered_count

        logger.info("訂正報告書フィルタリング:")
        logger.info(f"- 元のデータ件数: {original_count}件")
        logger.info(f"- フィルタリング後のデータ件数: {filtered_count}件")
        logger.info(f"- 重複として削除された件数: {removed_count}件")

        return filtered_df
    except Exception as e:
        logger.error(f"訂正報告書フィルタリング中にエラーが発生しました: {e}")
        logger.warning("元のデータをそのまま返します。")
        return df


def create_filtered_documents_with_pandas(all_documents_csv, companies, output_csv):
    """pandasを使用して企業リストでフィルタリングしたCSVを作成

    Args:
        all_documents_csv: すべての書類データのCSVパス
        companies: 企業情報のリスト
        output_csv: 出力先CSVパス

    Returns:
        bool: 処理成功の場合True、失敗の場合False
    """
    try:
        # CSVファイルを読み込み
        df = pd.read_csv(all_documents_csv)

        # データの確認
        logger.info(f"CSVファイルの行数: {len(df)}")

        # secCodeカラムが存在することを確認
        if "secCode" not in df.columns:
            logger.warning("警告: CSVにsecCodeカラムが存在しません。フィルタリングできません。")
            # 元のファイルをそのまま保存して終了
            df.to_csv(output_csv, index=False, encoding="utf-8")
            logger.info(f"元のデータをそのまま {output_csv} に保存しました")
            return True

        # NaN値を文字列の "" に変換して処理できるようにする
        df["secCode"] = df["secCode"].fillna("").astype(str)

        # 企業の証券コードリストを入手し、各社の証券コードの数値部分と文字列部分を両方検索
        matched_rows = pd.DataFrame()

        for company in companies:
            seccode = company.get("seccode", "")
            if not seccode:
                continue

            company_name = company.get("name", "不明")

            # 証券コードの数値部分（末尾の0を追加したバージョンも含む）
            code_base = str(seccode).lstrip("0")  # 先頭の0を除去
            code_with_zero = code_base + "0"  # 末尾に0を追加したバージョン

            try:
                # 証券コードまたは企業名でマッチング
                company_mask = (df["secCode"].str.contains(code_base, na=False)) | (
                    df["secCode"].str.contains(code_with_zero, na=False)
                )

                # filerNameカラムが存在すれば企業名でもマッチング
                if "filerName" in df.columns:
                    company_mask = company_mask | (
                        df["filerName"].str.contains(company_name, na=False)
                    )

                company_rows = df[company_mask]
                logger.info(
                    f"{company_name} (証券コード={seccode}): {len(company_rows)}件 ({code_base}または{code_with_zero}でマッチング)"
                )

                # マッチしたレコードを追加
                matched_rows = pd.concat([matched_rows, company_rows])
            except Exception as e:
                logger.error(f"{company_name}のフィルタリング中にエラーが発生: {e}")
                continue

        # 重複を除去
        if len(matched_rows) > 0:
            matched_rows = matched_rows.drop_duplicates()
            logger.info(f"最終的なマッチ件数: {len(matched_rows)}件")
        else:
            logger.warning("マッチする企業が見つかりませんでした。元のデータをそのまま使用します。")
            matched_rows = df

        # 結果を保存
        matched_rows.to_csv(output_csv, index=False, encoding="utf-8")

        logger.info(f"{len(matched_rows)}件のデータを{output_csv}に保存しました")
        return True

    except Exception as e:
        import traceback

        logger.error(f"pandas処理でエラーが発生しました: {e}")
        logger.error(traceback.format_exc())

        # エラーが発生した場合でも、元のファイルをコピーして出力する
        try:
            return edinet_core.safe_copy_file(all_documents_csv, output_csv)
        except Exception as copy_e:
            logger.error(f"ファイルのコピーにも失敗しました: {copy_e}")
            return False


def create_securities_reports_with_pandas(input_csv, output_csv):
    """pandasを使用して有価証券報告書のみをフィルタリングしたCSVを作成

    Args:
        input_csv: 入力CSVパス
        output_csv: 出力先CSVパス

    Returns:
        bool: 処理成功の場合True、失敗の場合False
    """
    try:
        # CSVファイルを読み込み
        df = pd.read_csv(input_csv)

        # docDescriptionカラムの存在確認
        if "docDescription" not in df.columns:
            logger.warning(
                f"警告: {input_csv} にdocDescriptionカラムが存在しません。フィルタリングをスキップします。"
            )
            # 元のファイルをそのままコピー
            df.to_csv(output_csv, index=False, encoding="utf-8")
            logger.info(f"元のデータをそのまま {output_csv} に保存しました")
            return True

        # NaN値を処理
        df["docDescription"] = df["docDescription"].fillna("")

        # docDescriptionカラムで「有価証券報告書」を含むものをフィルタリング
        filtered_df = df[df["docDescription"].str.contains("有価証券報告書")]

        # フィルタリング結果が空の場合
        if len(filtered_df) == 0:
            logger.warning(
                f"警告: 有価証券報告書が見つかりませんでした。元のデータをそのまま使用します。"
            )
            filtered_df = df

        # 訂正報告書がある場合は元の報告書を削除
        filtered_df = filter_corrected_reports(filtered_df)

        # 結果を保存
        filtered_df.to_csv(output_csv, index=False, encoding="utf-8")

        logger.info(f"{len(filtered_df)}件の有価証券報告書データを{output_csv}に保存しました")
        return True
    except Exception as e:
        logger.error(f"有価証券報告書のフィルタリング処理でエラーが発生しました: {e}")
        import traceback

        logger.error(traceback.format_exc())

        # エラーが発生した場合でも、元のファイルをコピーして出力する
        try:
            return edinet_core.safe_copy_file(input_csv, output_csv)
        except Exception as copy_e:
            logger.error(f"ファイルのコピーにも失敗しました: {copy_e}")
            return False


async def process_final_reports(filtered_securities_reports_path):
    """訂正報告書処理済みの最終版ファイルを作成

    Args:
        filtered_securities_reports_path: フィルタリング済みの有価証券報告書CSVパス

    Returns:
        str: 最終版CSVパス、または処理失敗時はNone
    """
    logger.info("\n訂正報告書処理済みの最終版ファイルを作成します")
    final_filtered_path = base_settings.FINAL_SECURITIES_REPORTS_CSV

    try:
        final_df = pd.read_csv(filtered_securities_reports_path)

        # 訂正報告書が参照している元の報告書を正しく削除
        logger.info("訂正報告書の元となる報告書を削除します")
        correction_mask = final_df["docDescription"].str.contains("訂正", na=False)
        correction_reports = final_df[correction_mask]

        # 訂正報告書が参照している元の報告書IDを取得
        reference_ids = correction_reports["parentDocID"].dropna().unique()

        # 参照されている元の報告書をドロップ
        filtered_final_df = final_df[~final_df["docID"].isin(reference_ids)]

        # 統計情報
        original_count = len(final_df)
        filtered_count = len(filtered_final_df)
        removed_count = original_count - filtered_count

        logger.info(f"処理結果:")
        logger.info(f"- 元のデータ件数: {original_count}件")
        logger.info(f"- フィルタリング後のデータ件数: {filtered_count}件")
        logger.info(f"- 削除された元報告書の件数: {removed_count}件")

        # 最終データを保存
        filtered_final_df.to_csv(final_filtered_path, index=False, encoding="utf-8")
        logger.info(
            f"最終版データを{final_filtered_path}に保存しました（{len(filtered_final_df)}件）"
        )

        return final_filtered_path
    except Exception as e:
        logger.error(f"最終版ファイル作成中にエラーが発生しました: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return None


async def process_year_documents(year, start_date, end_date, companies):
    """1年分の書類を処理する

    Args:
        year: 処理対象年
        start_date: 開始日
        end_date: 終了日
        companies: 企業情報リスト

    Returns:
        list: 処理された書類のリスト
    """
    logger.info(f"\n== {year}年の処理を開始します ==")

    # 指定期間の全書類を取得
    year_documents = await edinet_core.collect_documents_for_period_async(start_date, end_date)

    if not year_documents:
        logger.info(f"{year}年の書類は0件でした")
        return []

    logger.info(f"{year}年の全書類データ: {len(year_documents)}件")

    # 企業ごとの検出書類を表示
    logger.info(f"企業ごとの検出状況（{year}年）:")
    for company in companies:
        company_name = company.get("name", "不明")
        seccode = company.get("seccode", "")

        if not seccode:
            continue

        # 企業の書類を抽出
        company_docs = extract_company_documents(year_documents, seccode)

        if company_docs:
            doc_types = summarize_document_types(company_docs)
            doc_type_str = ", ".join([f"{t}: {c}件" for t, c in doc_types.items()])
            logger.info(f"- {company_name}: {len(company_docs)}件 ({doc_type_str})")

    return year_documents


async def process_collected_documents(all_documents, companies):
    """取得した書類データの処理を行う

    Args:
        all_documents: 収集した全書類データ
        companies: 企業情報リスト

    Returns:
        int: 処理結果コード (0: 成功)
    """
    logger.info(f"\n全ての期間の書類: {len(all_documents)}件をファイルに保存します")

    # すべてのデータを一つのファイルに保存
    json_path = os.path.join(base_settings.RESULTS_DIR, "all_documents.json")
    await asyncio.to_thread(edinet_core.save_to_json, all_documents, json_path)

    csv_path = base_settings.ALL_DOCUMENTS_CSV
    await asyncio.to_thread(edinet_core.save_to_csv, all_documents, csv_path)

    # 企業ごとのデータ数を集計（保存はせず、表示のみ）
    logger.info("\n企業ごとの総書類数:")
    for company in companies:
        company_name = company.get("name", "不明")
        seccode = company.get("seccode", "")

        if not seccode:
            continue

        # 企業の書類を抽出して集計
        company_docs = extract_company_documents(all_documents, seccode)

        if company_docs:
            # 書類種別の集計
            doc_types = summarize_document_types(company_docs)
            doc_type_str = ", ".join([f"{t}: {c}件" for t, c in doc_types.items()])
            logger.info(f"- {company_name}（{seccode}）: {len(company_docs)}件 ({doc_type_str})")

    # 特定の企業のみをフィルタリングしたデータを作成
    logger.info(
        f"\n{base_settings.COMPANY_INFO_JSON}の企業のみをフィルタリングしたデータを作成します"
    )

    # pandasを使用してフィルタリング
    logger.info("pandasを使用してフィルタリングします")
    filtered_csv_path = base_settings.FILTERED_DOCUMENTS_CSV
    await asyncio.to_thread(
        create_filtered_documents_with_pandas, csv_path, companies, filtered_csv_path
    )

    # 有価証券報告書のみをフィルタリングしたデータを作成
    logger.info("\n有価証券報告書のみをフィルタリングしたデータを作成します")

    # フィルタリング対象は全データと企業フィルタリング済みデータの両方
    securities_reports_path = base_settings.SECURITIES_REPORTS_CSV
    await asyncio.to_thread(
        create_securities_reports_with_pandas, csv_path, securities_reports_path
    )

    filtered_securities_reports_path = base_settings.FILTERED_SECURITIES_REPORTS_CSV
    await asyncio.to_thread(
        create_securities_reports_with_pandas,
        filtered_csv_path,
        filtered_securities_reports_path,
    )

    # 訂正報告書処理済みの最終版ファイルを作成
    final_filtered_path = await process_final_reports(filtered_securities_reports_path)

    # 結果の概要を表示
    logger.info(f"\n全ての処理が完了しました。")
    logger.info(f"- すべてのデータ: {base_settings.ALL_DOCUMENTS_CSV}")
    logger.info(f"- フィルタリングしたデータ: {base_settings.FILTERED_DOCUMENTS_CSV}")
    logger.info(f"- 有価証券報告書（全企業）: {base_settings.SECURITIES_REPORTS_CSV}")
    logger.info(
        f"- 有価証券報告書（指定企業のみ）: {base_settings.FILTERED_SECURITIES_REPORTS_CSV}"
    )
    logger.info(f"- 有価証券報告書（最終版）: {base_settings.FINAL_SECURITIES_REPORTS_CSV}")
    logger.info(
        f"Pandasでの分析例: df = pd.read_csv('{base_settings.FINAL_SECURITIES_REPORTS_CSV}')"
    )

    return 0


async def collect_and_process_documents():
    """書類の収集と処理を行うメイン関数"""
    # 調査する年の範囲設定
    start_year = settings.START_YEAR
    end_year = settings.END_YEAR
    target_month = settings.TARGET_MONTH

    logger.info(f"{start_year}年から{end_year}年までの{target_month}月の書類を検索します")

    # 企業情報の読み込み
    companies = await asyncio.to_thread(load_companies)

    if not companies:
        logger.error("企業情報がないため処理を終了します")
        return 1

    # 結果ディレクトリの作成
    edinet_core.ensure_dir(base_settings.RESULTS_DIR)

    # 年ごとの日付範囲を取得
    date_ranges = await asyncio.to_thread(
        edinet_core.get_date_ranges_for_years, start_year, end_year, target_month
    )

    # 全てのデータを保持するリスト
    all_documents = []

    # 年ごとにデータ取得
    for j, (year, start_date, end_date) in enumerate(date_ranges, 1):
        # 各年のデータを処理
        year_documents = await process_year_documents(year, start_date, end_date, companies)
        all_documents.extend(year_documents)

        # 年ごとの処理間に少し待機
        if j < len(date_ranges):
            logger.info(f"次の年の処理前に1秒待機...")
            await asyncio.sleep(1)

    # 全データを保存して処理
    if all_documents:
        return await process_collected_documents(all_documents, companies)
    else:
        logger.warning("データが1件も取得できませんでした")
        return 0


async def main_async():
    """メイン処理（非同期版）"""
    # ロギングの基本設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    return await collect_and_process_documents()


def main():
    """メイン処理（同期版のラッパー）"""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(main_async())


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
