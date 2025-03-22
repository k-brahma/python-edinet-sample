import asyncio
import io
import json
import os
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import aiohttp
import pandas as pd
from dotenv import load_dotenv

import config

# 環境変数のロード
load_dotenv()

# EDINET API Key
EDINET_API_KEY = os.getenv(config.API_KEY_ENV_NAME)
if not EDINET_API_KEY:
    print(
        f"警告: {config.API_KEY_ENV_NAME}が設定されていません。.envファイルに{config.API_KEY_ENV_NAME}=あなたのキーを設定してください。"
    )
    # サンプル用にダミーキーを設定（実際は機能しません）
    EDINET_API_KEY = "dummy_key"

# ダウンロード先ディレクトリ
DOWNLOAD_DIR = config.XBRL_DOWNLOAD_DIR
# 同時実行数
MAX_CONCURRENT_REQUESTS = config.XBRL_DOWNLOADER_MAX_CONCURRENT
# 共有スレッドプール
thread_pool = ThreadPoolExecutor(max_workers=10)


async def get_document_info(session, doc_id):
    """書類IDから書類の詳細情報を取得する（非同期版）"""
    url = f"https://api.edinet-fsa.go.jp/api/v1/documents/{doc_id}"
    headers = {"X-API-KEY": EDINET_API_KEY}

    try:
        async with session.get(url, headers=headers, timeout=config.REQUEST_TIMEOUT) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        print(f"書類情報の取得に失敗しました(ID: {doc_id}): {e}")
        return None


async def download_xbrl(session, doc_id, save_dir=None):
    """書類IDを使用してXBRLデータをダウンロードする（非同期版）"""
    url = f"https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}?type=1"
    params = {"Subscription-Key": EDINET_API_KEY}

    try:
        print(f"書類 {doc_id} をダウンロード中...")
        async with session.get(url, params=params, timeout=config.REQUEST_TIMEOUT) as response:
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

        # ZIPファイルを展開（I/O処理なのでスレッドプールで実行）
        loop = asyncio.get_running_loop()
        # 共有スレッドプールを使用
        await loop.run_in_executor(thread_pool, extract_zip, zip_path, save_dir)

        print(f"書類 {doc_id} を {save_dir} に保存しました")

        # XBRLファイルのパスを返す（I/O処理なのでスレッドプールで実行）
        xbrl_files = await loop.run_in_executor(thread_pool, find_xbrl_files, save_dir)
        return xbrl_files

    except Exception as e:
        print(f"XBRLダウンロードに失敗しました(ID: {doc_id}): {e}")
        return []


def extract_zip(zip_path, extract_dir):
    """ZIPファイルを展開する（スレッドプールで実行するためのヘルパー関数）"""
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(extract_dir)


def find_xbrl_files(directory):
    """ディレクトリ内のXBRLファイルを再帰的に検索"""
    xbrl_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".xbrl"):
                xbrl_files.append(os.path.join(root, file))
    return xbrl_files


async def extract_financial_indicators_async(xbrl_path):
    """XBRLファイルから特定の財務指標を抽出する（非同期版）"""
    loop = asyncio.get_running_loop()
    # 共有スレッドプールを使用
    return await loop.run_in_executor(thread_pool, extract_financial_indicators, xbrl_path)


def extract_financial_indicators(xbrl_path):
    """XBRLファイルから特定の財務指標を抽出する"""
    try:
        # ファイルを読み込む
        with open(xbrl_path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()

        # 会社名と決算期を抽出
        company_name_match = re.search(
            r"<jpdei_cor:CompanyName[^>]*>([^<]+)</jpdei_cor:CompanyName>", content
        )
        company_name = company_name_match.group(1) if company_name_match else "不明"

        period_end_match = re.search(
            r"<jpdei_cor:CurrentFiscalYearEndDate[^>]*>([^<]+)</jpdei_cor:CurrentFiscalYearEndDate>",
            content,
        )
        period_end = period_end_match.group(1) if period_end_match else "不明"

        # ISO形式の日付を日本の形式に変換
        if period_end != "不明" and len(period_end) == 10:  # YYYY-MM-DD
            try:
                date_obj = datetime.strptime(period_end, "%Y-%m-%d")
                period_end = date_obj.strftime("%Y年%m月%d日")
            except:
                pass

        financial_data = {
            "会社名": company_name,
            "決算期": period_end,
            "ファイル": os.path.basename(xbrl_path),
            "ファイルパス": xbrl_path,
        }

        # 対象とする財務指標のタグと日本語名
        indicators = [
            ("NetSales", "売上高"),
            ("GrossProfit", "売上総利益"),
            ("OperatingIncome", "営業利益"),
            ("OrdinaryIncome", "経常利益"),
            ("ProfitLoss", "当期純利益"),
            ("TotalAssets", "総資産"),
            ("NetAssets", "純資産"),
        ]

        # 財務指標が見つかったかどうかのフラグ
        found_any_indicator = False

        for tag, description in indicators:
            # タグを検索（コンテキストが CurrentYearDuration のものを優先）
            pattern = (
                rf"<[^>]*:{tag} [^>]*contextRef=\"CurrentYearDuration\"[^>]*>([^<]+)</[^>]*:{tag}>"
            )
            matches = re.findall(pattern, content)

            # CurrentYearDuration がない場合は他のコンテキストも検索
            if not matches:
                pattern = rf"<[^>]*:{tag}[^>]*>([^<]+)</[^>]*:{tag}>"
                matches = re.findall(pattern, content)

            if matches:
                # 数値のみを抽出して整数に変換
                values = []
                for m in matches:
                    try:
                        values.append(int(m))
                    except ValueError:
                        continue

                if values:
                    # 最も大きな値を取得（連結財務諸表の可能性）
                    value = max(values)
                    # 単位を適切に変換
                    if value >= 1000000000:
                        financial_data[description] = f"{value / 1000000000:.2f}十億円"
                    elif value >= 1000000:
                        financial_data[description] = f"{value / 1000000:.2f}百万円"
                    else:
                        financial_data[description] = f"{value:,}円"

                    # 生の数値も保存
                    financial_data[f"{description}_raw"] = value
                    found_any_indicator = True

        # 財務指標が1つも見つからなかった場合もとりあえず会社情報は返す
        return financial_data

    except Exception as e:
        print(f"解析エラー ({os.path.basename(xbrl_path)}): {str(e)}")
        return {
            "会社名": "不明",
            "決算期": "不明",
            "ファイル": os.path.basename(xbrl_path),
            "エラー": str(e),
        }


def save_to_csv(financial_data_list, output_file=None):
    """財務指標データをCSVファイルに保存"""
    if output_file is None:
        output_file = config.FINANCIAL_INDICATORS_CSV

    if not financial_data_list:
        print("保存するデータがありません")
        return False

    # pandas DataFrameに変換
    df = pd.DataFrame(financial_data_list)

    # 生のデータに基づいてソート（売上高の降順）
    if "売上高_raw" in df.columns:
        df = df.sort_values("売上高_raw", ascending=False)

    # CSVファイルに保存
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"データを {output_file} に保存しました")
    return True


async def process_securities_reports_async(csv_path, limit=None):
    """CSVファイルから有価証券報告書の情報を読み込み、XBRLをダウンロードして分析する（非同期版）"""
    # CSVファイルを読み込み
    df = pd.read_csv(csv_path)

    # ダウンロードディレクトリの作成
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    print(f"{len(df)}件の有価証券報告書データを処理します")

    # 処理対象の制限（デバッグ用）
    if limit and limit > 0:
        df = df.head(limit)
        print(f"処理対象を{limit}件に制限します")

    all_financial_data = []

    # 同時実行数を制限するセマフォを作成
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    # セッション作成
    async with aiohttp.ClientSession() as session:
        # タスクリストを作成
        tasks = []

        for i, row in enumerate(df.iterrows(), 1):
            row = row[1]  # pandas行オブジェクト
            doc_id = row["docID"]
            company = row["filerName"]
            doc_description = row["docDescription"]

            # 各書類の処理タスクを作成
            task = process_document(
                session, semaphore, i, len(df), doc_id, company, doc_description, row
            )
            tasks.append(task)

        # 全タスクを実行して結果を待機
        results = await asyncio.gather(*tasks)

        # 結果を集計
        for result in results:
            if result:
                all_financial_data.extend(result)

    # 結果をCSVファイルに保存
    if all_financial_data:
        output_file = config.FINANCIAL_INDICATORS_CSV
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        save_to_csv(all_financial_data, output_file)

        print(f"\n抽出された財務指標データ: {len(all_financial_data)}件")
        print(f"データは {output_file} に保存されました")

        # 会社ごとに年度別財務情報をピボットテーブルで表示
        await create_pivot_tables_async(all_financial_data)
    else:
        print("財務指標データが抽出できませんでした")


async def process_document(session, semaphore, index, total, doc_id, company, doc_description, row):
    """個別の書類を処理する（非同期版）"""
    async with semaphore:  # 同時実行数を制限
        try:
            print(f"\n[{index}/{total}] {company} - {doc_description} (ID: {doc_id})")

            # XBRLをダウンロード
            company_dir = os.path.join(DOWNLOAD_DIR, company, doc_id)
            xbrl_files = await download_xbrl(session, doc_id, company_dir)

            if not xbrl_files:
                print(f"XBRLファイルが見つかりませんでした: {doc_id}")
                return []

            # 有価証券報告書のXBRLファイルをフィルタリング
            report_files = [
                file for file in xbrl_files if "jpcrp" in os.path.basename(file).lower()
            ]

            if not report_files:
                print(f"有価証券報告書のXBRLファイルが見つかりませんでした: {doc_id}")
                # 他のxbrlファイルがあれば使用
                if xbrl_files:
                    report_files = xbrl_files

            financial_data_list = []

            # 財務指標の抽出タスクを作成
            extract_tasks = []
            for xbrl_path in report_files:
                extract_tasks.append(extract_financial_indicators_async(xbrl_path))

            # 全ての抽出タスクを実行
            financial_data_results = await asyncio.gather(*extract_tasks)

            for financial_data in financial_data_results:
                if financial_data:
                    # CSVからの情報を追加（重要: 会社名を必ず上書きする）
                    financial_data["会社名"] = company  # CSVの会社名を必ず使用
                    financial_data["docID"] = doc_id
                    financial_data["document_date"] = row.get("document_date", "")
                    financial_data["docDescription"] = doc_description

                    financial_data_list.append(financial_data)

                    # 財務指標を表示
                    indicators = ["売上高", "営業利益", "経常利益", "当期純利益"]
                    for indicator in indicators:
                        if indicator in financial_data:
                            print(f"  {indicator}: {financial_data[indicator]}")

            return financial_data_list

        except Exception as e:
            print(f"書類 {doc_id} の処理中にエラーが発生しました: {e}")
            return []


async def create_pivot_tables_async(financial_data_list):
    """会社ごとの年度別財務情報をピボットテーブルで作成（非同期版）"""
    loop = asyncio.get_running_loop()
    # 共有スレッドプールを使用
    await loop.run_in_executor(thread_pool, create_pivot_tables, financial_data_list)


def create_pivot_tables(financial_data_list):
    """会社ごとの年度別財務情報をピボットテーブルで作成"""
    df = pd.DataFrame(financial_data_list)

    # 日付フィールドを年に変換
    if "document_date" in df.columns:
        df["年度"] = pd.to_datetime(df["document_date"]).dt.year

    # 全企業の財務推移データを格納するDataFrame
    all_trends = []

    # 会社ごとに年度別の財務情報をピボットテーブルで表示
    for company in df["会社名"].unique():
        company_df = df[df["会社名"] == company]

        print(f"\n{company}の年度別財務情報:")

        # 売上高の推移
        if "売上高_raw" in company_df.columns:
            # ピボットテーブルの作成
            pivot = company_df.pivot_table(
                index="年度",
                values=["売上高_raw", "営業利益_raw", "経常利益_raw", "当期純利益_raw"],
                aggfunc="max",
            )

            # 単位を百万円に統一して表示用に変換
            pivot_display = pivot / 1000000
            print("単位: 百万円")
            print(pivot_display)

            # 各年の財務指標を取得して統合データに追加
            for year, row in pivot.iterrows():
                year_data = {
                    "会社名": company,
                    "年度": year,
                    "売上高_百万円": row.get("売上高_raw", 0) / 1000000,
                    "営業利益_百万円": row.get("営業利益_raw", 0) / 1000000,
                    "経常利益_百万円": row.get("経常利益_raw", 0) / 1000000,
                    "当期純利益_百万円": row.get("当期純利益_raw", 0) / 1000000,
                }
                all_trends.append(year_data)

    # 全企業の財務推移データをDataFrameに変換
    if all_trends:
        all_trends_df = pd.DataFrame(all_trends)

        # CSVに保存
        all_trends_file = config.FINANCIAL_TRENDS_CSV
        os.makedirs(os.path.dirname(all_trends_file), exist_ok=True)
        all_trends_df.to_csv(all_trends_file, index=False)
        print(f"\n全企業の財務推移データを {all_trends_file} に保存しました")

        # matplotlibでのグラフ作成
        create_comparison_charts(all_trends_df)
    else:
        print("財務推移データがありません")


def create_comparison_charts(df):
    """会社間の財務指標比較グラフを作成"""
    import matplotlib

    matplotlib.use("Agg")  # GUIバックエンドを使用しない設定
    import matplotlib.pyplot as plt
    import japanize_matplotlib  # 日本語フォントの設定

    # 売上高上位企業を抽出
    top_companies = df.sort_values(by="売上高_百万円", ascending=False)["会社名"].unique()[
        : config.CHART_TOP_COMPANIES
    ]

    # 出力ディレクトリ作成
    charts_dir = config.CHARTS_DIR
    os.makedirs(charts_dir, exist_ok=True)

    # -------- 売上高の比較 --------
    plt.figure(figsize=(12, 8))
    for company in top_companies:
        company_data = df[df["会社名"] == company]
        company_data_copy = company_data.copy()  # 明示的にコピーを作成
        company_data_copy = company_data_copy.sort_values("年度")
        plt.plot(
            company_data_copy["年度"], company_data_copy["売上高_百万円"], marker="o", label=company
        )

    plt.title("売上高推移比較（上位企業）")
    plt.xlabel("年度")
    plt.ylabel("売上高（百万円）")
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(charts_dir, "revenue_comparison.png"))
    plt.close()

    # -------- 営業利益の比較 --------
    plt.figure(figsize=(12, 8))
    for company in top_companies:
        company_data = df[df["会社名"] == company]
        company_data_copy = company_data.copy()  # 明示的にコピーを作成
        company_data_copy = company_data_copy.sort_values("年度")
        plt.plot(
            company_data_copy["年度"],
            company_data_copy["営業利益_百万円"],
            marker="o",
            label=company,
        )

    plt.title("営業利益推移比較（上位企業）")
    plt.xlabel("年度")
    plt.ylabel("営業利益（百万円）")
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(charts_dir, "operating_income_comparison.png"))
    plt.close()

    # -------- 当期純利益の比較 --------
    plt.figure(figsize=(12, 8))
    for company in top_companies:
        company_data = df[df["会社名"] == company]
        company_data_copy = company_data.copy()  # 明示的にコピーを作成
        company_data_copy = company_data_copy.sort_values("年度")
        plt.plot(
            company_data_copy["年度"],
            company_data_copy["当期純利益_百万円"],
            marker="o",
            label=company,
        )

    plt.title("当期純利益推移比較（上位企業）")
    plt.xlabel("年度")
    plt.ylabel("当期純利益（百万円）")
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(charts_dir, "net_income_comparison.png"))
    plt.close()

    # 個別企業のグラフを作成
    create_individual_company_charts(df)

    print(f"財務指標比較グラフを {charts_dir} に保存しました")


def create_individual_company_charts(df):
    """個別企業ごとの財務指標推移グラフを作成"""
    import matplotlib.pyplot as plt
    
    # 出力ディレクトリ作成
    charts_dir = config.CHARTS_DIR
    individual_charts_dir = os.path.join(charts_dir, "individual")
    os.makedirs(individual_charts_dir, exist_ok=True)
    
    # 各企業ごとにグラフを作成
    for company in df["会社名"].unique():
        company_data = df[df["会社名"] == company].copy()
        
        # データがない場合はスキップ
        if company_data.empty or len(company_data) < 2:
            continue
            
        company_data = company_data.sort_values("年度")
        
        # -------- 財務指標の推移グラフ --------
        plt.figure(figsize=(12, 8))
        
        # グラフの背景色を設定
        ax = plt.gca()
        ax.set_facecolor('#f0f8ff')  # 薄い青色の背景
        
        # 売上高の推移（主軸）
        ax1 = plt.gca()
        ax1.plot(company_data["年度"], company_data["売上高_百万円"], 'b-o', label="売上高", linewidth=2.5)
        ax1.set_xlabel("年度")
        ax1.set_ylabel("売上高（百万円）", color='b', fontweight='bold')
        ax1.tick_params(axis='y', labelcolor='b')
        ax1.set_ylim(bottom=0)  # Y軸の下限を0に設定
        
        # Y軸の上限を広げる（最大値の約1.2倍に設定）
        if not company_data["売上高_百万円"].empty:
            ymax = company_data["売上高_百万円"].max()
            ax1.set_ylim(top=ymax * 1.2)
        
        # 営業利益・当期純利益の推移（副軸）
        ax2 = ax1.twinx()
        ax2.plot(company_data["年度"], company_data["営業利益_百万円"], 'r-^', label="営業利益", linewidth=2.5)
        ax2.plot(company_data["年度"], company_data["当期純利益_百万円"], 'g-s', label="当期純利益", linewidth=2.5)
        ax2.set_ylabel("利益（百万円）", color='r', fontweight='bold')
        ax2.tick_params(axis='y', labelcolor='r')
        
        # Y軸の下限を0に設定（マイナスの場合は例外）
        if min(company_data["営業利益_百万円"].min(), company_data["当期純利益_百万円"].min()) >= 0:
            ax2.set_ylim(bottom=0)
            
            # Y軸の上限を広げる（最大値の約1.2倍に設定）
            profit_max = max(
                company_data["営業利益_百万円"].max(),
                company_data["当期純利益_百万円"].max()
            )
            ax2.set_ylim(top=profit_max * 1.2)
        
        # グリッド線を追加して見やすくする
        ax1.grid(True, linestyle='--', alpha=0.7)
        
        # 凡例の設定
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc='best', facecolor='white', framealpha=0.9)
        
        plt.title(f"{company}の財務指標推移", fontsize=16, fontweight='bold')
        
        # グラフ全体の外枠を追加
        plt.box(True)
        
        # ファイル名に使えない文字を置換
        safe_company_name = company.replace("/", "_").replace("\\", "_").replace(":", "_")
        
        plt.savefig(os.path.join(individual_charts_dir, f"{safe_company_name}_財務推移.png"), dpi=300, bbox_inches='tight')
        plt.close()
    
    print(f"個別企業の財務指標グラフを {individual_charts_dir} に保存しました")


async def main_async():
    print("有価証券報告書のXBRLデータ取得・解析ツール（非同期版）")

    # CSVファイルのパス
    csv_path = config.FINAL_SECURITIES_REPORTS_CSV

    if not os.path.exists(csv_path):
        print(f"CSVファイルが見つかりません: {csv_path}")

        # 他の候補ファイルを確認
        candidate_files = [
            config.FILTERED_SECURITIES_REPORTS_CSV,
            config.SECURITIES_REPORTS_CSV,
            config.FILTERED_DOCUMENTS_CSV,
            config.ALL_DOCUMENTS_CSV,
        ]

        for alt_path in candidate_files:
            if os.path.exists(alt_path):
                print(f"代替ファイルを使用します: {alt_path}")
                csv_path = alt_path
                break
        else:
            return 1

    try:
        # 処理を実行
        process_limit = None  # 制限を解除して全件処理
        await process_securities_reports_async(csv_path, limit=process_limit)
        return 0
    finally:
        # プログラム終了時にスレッドプールをシャットダウン
        thread_pool.shutdown(wait=True)


def main():
    """XBRLデータ取得・解析のメイン関数"""
    # Windows環境での非同期処理の対応
    if os.name == "nt":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # メイン処理を実行
    return asyncio.run(main_async())


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
