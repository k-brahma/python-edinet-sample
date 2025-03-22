import asyncio
import csv
import datetime
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, Optional

import aiohttp
import pandas as pd
import requests
from dotenv import load_dotenv

import config

# 環境変数のロード
load_dotenv()

# EDINET API Key
EDINET_API_KEY = os.getenv(config.API_KEY_ENV_NAME)

# 同時に実行するリクエスト数
MAX_CONCURRENT_REQUESTS = config.API_MAX_CONCURRENT


async def fetch_documents_for_date(session, date_str, semaphore):
    """非同期で特定の日付の全書類を取得する"""
    url = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
    params = {"date": date_str, "type": 2, "Subscription-Key": EDINET_API_KEY}

    async with semaphore:
        try:
            async with session.get(url, params=params, timeout=config.REQUEST_TIMEOUT) as response:
                response.raise_for_status()
                data = await response.json()

                results = data.get("results", [])
                if results:
                    print(f"  {date_str}: {len(results)}件の書類を取得しました")
                    return [(date_str, doc) for doc in results]
                else:
                    print(f"  {date_str}: 書類はありませんでした")
                    return []

        except Exception as e:
            print(f"  {date_str} でエラー発生: {e}")
            return []


async def collect_documents_for_period_async(start_date, end_date):
    """
    指定された期間内の全提出書類を非同期で収集

    Args:
        start_date: 検索開始日 (datetime.date)
        end_date: 検索終了日 (datetime.date)

    Returns:
        list: 検索結果の書類リスト [(日付, 書類データ), ...]
    """
    # 検索対象の日付リストを作成
    current_date = start_date
    date_list = []
    while current_date <= end_date:
        date_list.append(current_date)
        current_date += datetime.timedelta(days=1)

    print(f"{start_date}から{end_date}までの{len(date_list)}日間の書類を一括検索します")

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
        print(f"全{len(tasks)}日分のデータを同時に取得中...")
        results = await asyncio.gather(*tasks)

        # 結果をフラット化して結合
        for date_docs in results:
            all_documents.extend(date_docs)

    return all_documents


def load_companies(file_path=None):
    """企業情報をJSONファイルから読み込む"""
    if file_path is None:
        file_path = config.COMPANY_INFO_JSON

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            companies = json.load(f)

        # 読み込んだデータが文字列の場合（エラーメッセージなど）はリストに変換
        if not isinstance(companies, list):
            print(f"警告: 企業情報のJSONファイルの形式が不正です: {companies}")
            return []

        print(f"{len(companies)}社の企業情報を読み込みました")
        return companies
    except FileNotFoundError:
        print(f"企業情報ファイルが見つかりません: {file_path}")
        return []
    except json.JSONDecodeError:
        print(f"企業情報ファイルのJSON形式が不正です: {file_path}")
        return []
    except Exception as e:
        print(f"企業情報ファイルの読み込みエラー: {e}")
        return []


def save_to_json(data, filepath):
    """結果をJSONファイルに保存"""
    # ディレクトリが存在しなければ作成
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print(f"データを{filepath}に保存しました（{len(data)}件）")


def save_to_csv(data, filepath, date_field=True):
    """結果をCSVファイルに保存"""
    # ディレクトリが存在しなければ作成
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    if not data:
        print(f"保存するデータがありません: {filepath}")
        return

    # CSVファイルに書き込み
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

    print(f"データを{filepath}に保存しました（{len(data)}件）")


def filter_documents_by_companies(all_documents, companies):
    """特定の企業リストに含まれる証券コードの書類のみをフィルタリング"""
    # 企業の証券コードのセットを作成（文字列として保存）
    seccode_set = {str(company.get("seccode")) for company in companies if company.get("seccode")}

    # フィルタリングされた書類のリスト
    filtered_docs = []

    for date_str, doc in all_documents:
        # 文字列に変換して比較
        if str(doc.get("secCode", "")) in seccode_set:
            filtered_docs.append((date_str, doc))

    return filtered_docs


def create_filtered_documents_with_pandas(all_documents_csv, companies, output_csv):
    """pandasを使用して企業リストでフィルタリングしたCSVを作成"""
    try:
        # CSVファイルを読み込み
        df = pd.read_csv(all_documents_csv)

        # データの確認
        print(f"CSVファイルの行数: {len(df)}")

        # secCodeカラムが存在することを確認
        if "secCode" not in df.columns:
            print("警告: CSVにsecCodeカラムが存在しません。フィルタリングできません。")
            # 元のファイルをそのまま保存して終了
            df.to_csv(output_csv, index=False, encoding="utf-8")
            print(f"元のデータをそのまま {output_csv} に保存しました")
            return True

        # secCodeカラムのデータ型確認とNaN処理
        print(f"secCodeカラムのデータ型: {df['secCode'].dtype}")

        # NaN値を文字列の "nan" に変換して処理できるようにする
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

            # 企業名の株式会社部分を取り除いた短い名前
            short_name = company_name.replace("株式会社", "").strip()

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
                print(
                    f"{company_name} (証券コード={seccode}): {len(company_rows)}件 ({code_base}または{code_with_zero}でマッチング)"
                )

                # マッチしたレコードを追加
                matched_rows = pd.concat([matched_rows, company_rows])
            except Exception as e:
                print(f"{company_name}のフィルタリング中にエラーが発生: {e}")
                continue

        # 重複を除去
        if len(matched_rows) > 0:
            matched_rows = matched_rows.drop_duplicates()
            print(f"最終的なマッチ件数: {len(matched_rows)}件")
        else:
            print("マッチする企業が見つかりませんでした。元のデータをそのまま使用します。")
            matched_rows = df

        # トヨタのレコードがあるか確認（デバッグ用）
        try:
            if "filerName" in matched_rows.columns:
                toyota_mask = matched_rows["filerName"].str.contains("トヨタ自動車", na=False)
                toyota_rows = matched_rows[toyota_mask]
                if len(toyota_rows) > 0:
                    print(f"トヨタ自動車のレコード: {len(toyota_rows)}件")
                    if "docDescription" in toyota_rows.columns:
                        print(
                            f"有価証券報告書: {len(toyota_rows[toyota_rows['docDescription'].str.contains('有価証券報告書', na=False)])}件"
                        )
        except Exception as e:
            print(f"トヨタ自動車のデータ確認中にエラー: {e}")

        # 結果を保存
        matched_rows.to_csv(output_csv, index=False, encoding="utf-8")

        print(f"{len(matched_rows)}件のデータを{output_csv}に保存しました")
        return True

    except Exception as e:
        import traceback

        print(f"pandas処理でエラーが発生しました: {e}")
        print(traceback.format_exc())

        # エラーが発生した場合でも、元のファイルをコピーして出力する
        try:
            import shutil

            shutil.copy(all_documents_csv, output_csv)
            print(f"エラーが発生したため、元のファイルを {output_csv} にコピーしました")
            return True
        except:
            print("ファイルのコピーにも失敗しました")
            return False


def get_date_ranges_for_years(start_year=None, end_year=None, month=None):
    """指定された年の範囲について、各年の特定月の日付範囲を生成する

    Args:
        start_year: 開始年
        end_year: 終了年
        month: 対象月（デフォルトは6月）

    Returns:
        list: (年, 開始日, 終了日)のタプルのリスト
    """
    # デフォルト値を設定
    if start_year is None:
        start_year = config.START_YEAR
    if end_year is None:
        end_year = config.END_YEAR
    if month is None:
        month = config.TARGET_MONTH

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


def extract_company_documents(all_documents, seccode):
    """全書類から特定の証券コードの企業の書類を抽出する"""
    # 証券コードを文字列化
    seccode_str = str(seccode)

    company_docs = []
    for date_str, doc in all_documents:
        # 文字列に変換して比較
        if str(doc.get("secCode", "")) == seccode_str:
            company_docs.append((date_str, doc))
    return company_docs


def summarize_document_types(documents):
    """書類の種類ごとの集計を行う"""
    doc_types = {}
    for _, doc in documents:
        doc_type = doc.get("docDescription", "不明")
        if doc_type in doc_types:
            doc_types[doc_type] += 1
        else:
            doc_types[doc_type] = 1
    return doc_types


def filter_securities_reports(documents):
    """有価証券報告書のみをフィルタリングする"""
    filtered_docs = []
    for date_str, doc in documents:
        doc_description = doc.get("docDescription", "")
        if "有価証券報告書" in doc_description:
            filtered_docs.append((date_str, doc))
    return filtered_docs


def filter_corrected_reports(df):
    """訂正報告書がある場合は元の報告書を削除する"""
    # 元のデータ数を記録
    original_count = len(df)

    # referenceDocIDカラムが存在するか確認
    if "referenceDocID" not in df.columns:
        print("referenceDocIDカラムが見つかりません。訂正報告書フィルタリングをスキップします。")
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

        print(f"訂正報告書フィルタリング:")
        print(f"- 元のデータ件数: {original_count}件")
        print(f"- フィルタリング後のデータ件数: {filtered_count}件")
        print(f"- 重複として削除された件数: {removed_count}件")

        return filtered_df
    except Exception as e:
        print(f"訂正報告書フィルタリング中にエラーが発生しました: {e}")
        print("元のデータをそのまま返します。")
        return df


def create_securities_reports_with_pandas(input_csv, output_csv):
    """pandasを使用して有価証券報告書のみをフィルタリングしたCSVを作成"""
    try:
        # CSVファイルを読み込み
        df = pd.read_csv(input_csv)

        # docDescriptionカラムの存在確認
        if "docDescription" not in df.columns:
            print(
                f"警告: {input_csv} にdocDescriptionカラムが存在しません。フィルタリングをスキップします。"
            )
            # 元のファイルをそのままコピー
            df.to_csv(output_csv, index=False, encoding="utf-8")
            print(f"元のデータをそのまま {output_csv} に保存しました")
            return True

        # NaN値を処理
        df["docDescription"] = df["docDescription"].fillna("")

        # docDescriptionカラムで「有価証券報告書」を含むものをフィルタリング
        filtered_df = df[df["docDescription"].str.contains("有価証券報告書")]

        # フィルタリング結果が空の場合
        if len(filtered_df) == 0:
            print(f"警告: 有価証券報告書が見つかりませんでした。元のデータをそのまま使用します。")
            filtered_df = df

        # 訂正報告書がある場合は元の報告書を削除
        filtered_df = filter_corrected_reports(filtered_df)

        # 結果を保存
        filtered_df.to_csv(output_csv, index=False, encoding="utf-8")

        print(f"{len(filtered_df)}件の有価証券報告書データを{output_csv}に保存しました")
        return True
    except Exception as e:
        print(f"有価証券報告書のフィルタリング処理でエラーが発生しました: {e}")
        import traceback

        print(traceback.format_exc())

        # エラーが発生した場合でも、元のファイルをコピーして出力する
        try:
            import shutil

            shutil.copy(input_csv, output_csv)
            print(f"エラーが発生したため、元のファイルを {output_csv} にコピーしました")
            return True
        except:
            print("ファイルのコピーにも失敗しました")
            return False


async def main_async():
    """メイン処理（非同期版）"""
    # 調査する年の範囲設定
    start_year = config.START_YEAR
    end_year = config.END_YEAR
    target_month = config.TARGET_MONTH

    print(f"{start_year}年から{end_year}年までの{target_month}月の書類を検索します")

    # 企業情報の読み込み
    companies = await asyncio.to_thread(load_companies)

    if not companies:
        print("企業情報がないため処理を終了します")
        return 1

    # 結果ディレクトリの作成
    os.makedirs(config.RESULTS_DIR, exist_ok=True)

    # 年ごとの日付範囲を取得
    date_ranges = await asyncio.to_thread(
        get_date_ranges_for_years, start_year, end_year, target_month
    )

    # 全てのデータを保持するリスト
    all_documents = []

    # 年ごとにデータ取得（必要データを全て保持）
    for j, (year, start_date, end_date) in enumerate(date_ranges, 1):
        print(f"\n== {year}年の処理を開始します ({j}/{len(date_ranges)}) ==")

        # 指定期間の全書類を取得
        year_documents = await collect_documents_for_period_async(start_date, end_date)

        # 全体のデータに追加
        if year_documents:
            print(f"{year}年の全書類データ: {len(year_documents)}件")
            all_documents.extend(year_documents)

            # 企業ごとの検出書類を表示（保存はしない）
            print(f"企業ごとの検出状況（{year}年）:")
            for company in companies:
                company_name = company.get("name", "不明")
                seccode = company.get("seccode", "")

                if not seccode:
                    continue

                # 企業の書類を抽出（表示のみ）
                company_docs = extract_company_documents(year_documents, seccode)

                if company_docs:
                    doc_types = summarize_document_types(company_docs)
                    doc_type_str = ", ".join([f"{t}: {c}件" for t, c in doc_types.items()])
                    print(f"- {company_name}: {len(company_docs)}件 ({doc_type_str})")
        else:
            print(f"{year}年の書類は0件でした")

        # 年ごとの処理間に少し待機
        if j < len(date_ranges):
            print(f"次の年の処理前に1秒待機...")
            await asyncio.sleep(1)

    # 全データを保存
    if all_documents:
        print(f"\n全ての期間の書類: {len(all_documents)}件をファイルに保存します")

        # すべてのデータを一つのファイルに保存
        json_path = os.path.join(config.RESULTS_DIR, "all_documents.json")
        await asyncio.to_thread(save_to_json, all_documents, json_path)

        csv_path = config.ALL_DOCUMENTS_CSV
        await asyncio.to_thread(save_to_csv, all_documents, csv_path)

        # 企業ごとのデータ数を集計（保存はせず、表示のみ）
        print("\n企業ごとの総書類数:")
        for company in companies:
            company_name = company.get("name", "不明")
            seccode = company.get("seccode", "")

            if not seccode:
                continue

            # 企業の書類を抽出して集計（表示のみ）
            company_docs = extract_company_documents(all_documents, seccode)

            if company_docs:
                # 書類種別の集計
                doc_types = summarize_document_types(company_docs)
                doc_type_str = ", ".join([f"{t}: {c}件" for t, c in doc_types.items()])
                print(f"- {company_name}（{seccode}）: {len(company_docs)}件 ({doc_type_str})")

        # 特定の企業のみをフィルタリングしたデータを作成
        print(f"\n{config.COMPANY_INFO_JSON}の企業のみをフィルタリングしたデータを作成します")

        # pandasが利用可能な場合はpandasを使用
        print("pandasを使用してフィルタリングします")
        filtered_csv_path = config.FILTERED_DOCUMENTS_CSV
        await asyncio.to_thread(
            create_filtered_documents_with_pandas, csv_path, companies, filtered_csv_path
        )
        print(f"フィルタリングしたデータを{filtered_csv_path}に保存しました")

        # 有価証券報告書のみをフィルタリングしたデータを作成
        print("\n有価証券報告書のみをフィルタリングしたデータを作成します")

        # フィルタリング対象は全データと企業フィルタリング済みデータの両方
        securities_reports_path = config.SECURITIES_REPORTS_CSV
        await asyncio.to_thread(
            create_securities_reports_with_pandas, csv_path, securities_reports_path
        )

        filtered_securities_reports_path = config.FILTERED_SECURITIES_REPORTS_CSV
        await asyncio.to_thread(
            create_securities_reports_with_pandas,
            filtered_csv_path,
            filtered_securities_reports_path,
        )

        # 訂正報告書処理済みの最終版ファイルを作成
        print("\n訂正報告書処理済みの最終版ファイルを作成します")
        final_filtered_path = config.FINAL_SECURITIES_REPORTS_CSV
        final_df = pd.read_csv(filtered_securities_reports_path)

        # 訂正報告書が参照している元の報告書を正しく削除
        print("訂正報告書の元となる報告書を削除します")
        correction_mask = final_df["docDescription"].str.contains("訂正", na=False)
        correction_reports = final_df[correction_mask]

        # 訂正報告書が参照している元の報告書IDを取得
        reference_ids = correction_reports["parentDocID"].dropna().unique()
        print(f"訂正報告書が参照している元報告書ID: {reference_ids}")

        # 参照されている元の報告書をドロップ
        filtered_final_df = final_df[~final_df["docID"].isin(reference_ids)]

        # 統計情報
        original_count = len(final_df)
        filtered_count = len(filtered_final_df)
        removed_count = original_count - filtered_count

        print(f"処理結果:")
        print(f"- 元のデータ件数: {original_count}件")
        print(f"- フィルタリング後のデータ件数: {filtered_count}件")
        print(f"- 削除された元報告書の件数: {removed_count}件")

        # 最終データを保存
        filtered_final_df.to_csv(final_filtered_path, index=False, encoding="utf-8")
        print(f"最終版データを{final_filtered_path}に保存しました（{len(filtered_final_df)}件）")

    print(f"\n全ての処理が完了しました。")
    print(f"- すべてのデータ: {config.ALL_DOCUMENTS_CSV}")
    print(f"- フィルタリングしたデータ: {config.FILTERED_DOCUMENTS_CSV}")
    print(f"- 有価証券報告書（全企業）: {securities_reports_path}")
    print(f"- 有価証券報告書（指定企業のみ）: {config.FILTERED_SECURITIES_REPORTS_CSV}")
    print(f"- 有価証券報告書（最終版）: {final_filtered_path}")
    print(f"Pandasでの分析例: df = pd.read_csv('{final_filtered_path}')")
    return 0


# メイン処理（同期版のラッパー）
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
