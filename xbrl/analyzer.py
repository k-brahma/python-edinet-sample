"""
EDINET XBRLデータの財務分析機能

このモジュールはXBRLデータから財務指標を抽出し、分析結果を可視化します。
"""

import asyncio
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from config import base_settings

# ロガーの設定
logger = logging.getLogger(__name__)


def extract_financial_indicators(xbrl_path: str) -> Dict[str, Any]:
    """XBRLファイルから特定の財務指標を抽出する

    Parameters
    ----------
    xbrl_path : str
        XBRLファイルのパス

    Returns
    -------
    dict
        抽出された財務指標データ
    """
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
        except ValueError:
            logger.warning(f"日付変換に失敗しました: {period_end}")

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

    if not found_any_indicator:
        logger.warning(f"財務指標が見つかりませんでした: {os.path.basename(xbrl_path)}")

    return financial_data


async def extract_financial_indicators_async(
    xbrl_path: str, thread_pool: Optional[ThreadPoolExecutor] = None
) -> Dict[str, Any]:
    """XBRLファイルから特定の財務指標を抽出する（非同期版）

    Parameters
    ----------
    xbrl_path : str
        XBRLファイルのパス
    thread_pool : ThreadPoolExecutor, optional
        ファイル操作に使用するスレッドプール

    Returns
    -------
    dict
        抽出された財務指標データ
    """
    loop = asyncio.get_running_loop()

    if thread_pool:
        return await loop.run_in_executor(thread_pool, extract_financial_indicators, xbrl_path)
    else:
        return await loop.run_in_executor(None, extract_financial_indicators, xbrl_path)


def save_financial_data_to_csv(
    financial_data_list: List[Dict[str, Any]], output_file: Optional[str] = None
) -> str:
    """財務指標データをCSVファイルに保存

    Parameters
    ----------
    financial_data_list : list
        保存する財務指標データのリスト
    output_file : str, optional
        出力ファイルパス

    Returns
    -------
    str
        保存されたファイルのパス
    """
    if output_file is None:
        output_file = base_settings.FINANCIAL_INDICATORS_CSV

    if not financial_data_list:
        logger.warning("保存するデータがありません")
        raise ValueError("保存するデータがありません")

    # pandas DataFrameに変換
    df = pd.DataFrame(financial_data_list)

    # 生のデータに基づいてソート（売上高の降順）
    if "売上高_raw" in df.columns:
        df = df.sort_values("売上高_raw", ascending=False)

    # CSVファイルに保存
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")
    logger.info(f"データを {output_file} に保存しました")

    return output_file


def create_pivot_tables(financial_data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """会社ごとの年度別財務情報をピボットテーブルで作成

    Parameters
    ----------
    financial_data_list : list
        財務指標データのリスト

    Returns
    -------
    dict
        年度別財務情報と財務推移データのパス
    """
    df = pd.DataFrame(financial_data_list)

    # 日付フィールドを年に変換
    if "document_date" in df.columns:
        df["年度"] = pd.to_datetime(df["document_date"]).dt.year

    # 全企業の財務推移データを格納するリスト
    all_trends = []

    # 会社ごとに年度別の財務情報をピボットテーブルで表示
    for company in df["会社名"].unique():
        company_df = df[df["会社名"] == company]

        logger.info(f"\n{company}の年度別財務情報:")

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
            logger.info("単位: 百万円")
            logger.info(str(pivot_display))

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
    result = {"trends_data": all_trends}

    if all_trends:
        all_trends_df = pd.DataFrame(all_trends)

        # CSVに保存
        all_trends_file = base_settings.ALL_COMPANIES_FINANCIAL_TRENDS_CSV
        os.makedirs(os.path.dirname(all_trends_file), exist_ok=True)
        all_trends_df.to_csv(all_trends_file, index=False)
        logger.info(f"\n全企業の財務推移データを {all_trends_file} に保存しました")

        result["trends_file"] = all_trends_file
    else:
        logger.warning("財務推移データがありません")

    return result


async def create_pivot_tables_async(
    financial_data_list: List[Dict[str, Any]], thread_pool: Optional[ThreadPoolExecutor] = None
) -> Dict[str, Any]:
    """会社ごとの年度別財務情報をピボットテーブルで作成（非同期版）

    Parameters
    ----------
    financial_data_list : list
        財務指標データのリスト
    thread_pool : ThreadPoolExecutor, optional
        ファイル操作に使用するスレッドプール

    Returns
    -------
    dict
        年度別財務情報と財務推移データのパス
    """
    loop = asyncio.get_running_loop()

    if thread_pool:
        return await loop.run_in_executor(thread_pool, create_pivot_tables, financial_data_list)
    else:
        return await loop.run_in_executor(None, create_pivot_tables, financial_data_list)


def create_comparison_charts(
    trends_df: pd.DataFrame, output_dir: Optional[str] = None
) -> List[str]:
    """会社間の財務指標比較グラフを作成

    Parameters
    ----------
    trends_df : pandas.DataFrame
        財務推移データを含むDataFrame
    output_dir : str, optional
        グラフの保存先ディレクトリ

    Returns
    -------
    list
        作成されたグラフファイルのパスのリスト
    """
    import matplotlib

    matplotlib.use("Agg")  # GUIバックエンドを使用しない設定
    import matplotlib.pyplot as plt

    try:
        import japanize_matplotlib  # 日本語フォントの設定
    except ImportError:
        logger.warning(
            "japanize_matplotlibがインストールされていません。日本語フォントが正しく表示されない可能性があります。"
        )

    # 出力ディレクトリの設定
    if output_dir is None:
        output_dir = base_settings.CHARTS_DIR

    # 出力ディレクトリ作成
    os.makedirs(output_dir, exist_ok=True)

    # 作成したグラフのパスを保存するリスト
    graph_files = []

    # 売上高上位企業を抽出
    top_companies = trends_df.sort_values(by="売上高_百万円", ascending=False)["会社名"].unique()[
        :10
    ]

    # -------- 売上高の比較 --------
    plt.figure(figsize=(12, 8))
    for company in top_companies:
        company_data = trends_df[trends_df["会社名"] == company]
        company_data_sorted = company_data.sort_values("年度")
        plt.plot(
            company_data_sorted["年度"],
            company_data_sorted["売上高_百万円"],
            marker="o",
            label=company,
        )

    plt.title("売上高推移比較（上位企業）")
    plt.xlabel("年度")
    plt.ylabel("売上高（百万円）")
    plt.legend()
    plt.grid(True)

    revenue_chart_path = os.path.join(output_dir, "revenue_comparison.png")
    plt.savefig(revenue_chart_path)
    plt.close()
    graph_files.append(revenue_chart_path)

    # -------- 営業利益の比較 --------
    plt.figure(figsize=(12, 8))
    for company in top_companies:
        company_data = trends_df[trends_df["会社名"] == company]
        company_data_sorted = company_data.sort_values("年度")
        plt.plot(
            company_data_sorted["年度"],
            company_data_sorted["営業利益_百万円"],
            marker="o",
            label=company,
        )

    plt.title("営業利益推移比較（上位企業）")
    plt.xlabel("年度")
    plt.ylabel("営業利益（百万円）")
    plt.legend()
    plt.grid(True)

    operating_income_chart_path = os.path.join(output_dir, "operating_income_comparison.png")
    plt.savefig(operating_income_chart_path)
    plt.close()
    graph_files.append(operating_income_chart_path)

    # -------- 当期純利益の比較 --------
    plt.figure(figsize=(12, 8))
    for company in top_companies:
        company_data = trends_df[trends_df["会社名"] == company]
        company_data_sorted = company_data.sort_values("年度")
        plt.plot(
            company_data_sorted["年度"],
            company_data_sorted["当期純利益_百万円"],
            marker="o",
            label=company,
        )

    plt.title("当期純利益推移比較（上位企業）")
    plt.xlabel("年度")
    plt.ylabel("当期純利益（百万円）")
    plt.legend()
    plt.grid(True)

    net_income_chart_path = os.path.join(output_dir, "net_income_comparison.png")
    plt.savefig(net_income_chart_path)
    plt.close()
    graph_files.append(net_income_chart_path)

    # 個別企業のグラフを作成
    individual_chart_paths = create_individual_company_charts(trends_df, output_dir)
    graph_files.extend(individual_chart_paths)

    logger.info(f"財務指標比較グラフを {output_dir} に保存しました")

    return graph_files


def create_individual_company_charts(
    trends_df: pd.DataFrame, output_dir: Optional[str] = None
) -> List[str]:
    """個別企業ごとの財務指標推移グラフを作成

    Parameters
    ----------
    trends_df : pandas.DataFrame
        財務推移データを含むDataFrame
    output_dir : str, optional
        グラフの保存先ディレクトリ

    Returns
    -------
    list
        作成されたグラフファイルのパスのリスト
    """
    import matplotlib.pyplot as plt

    # 出力ディレクトリの設定
    if output_dir is None:
        output_dir = base_settings.CHARTS_DIR

    # 個別企業グラフ用のディレクトリ
    individual_charts_dir = os.path.join(output_dir, "individual")
    os.makedirs(individual_charts_dir, exist_ok=True)

    # 作成したグラフのパスを保存するリスト
    graph_files = []

    # 各企業ごとにグラフを作成
    for company in trends_df["会社名"].unique():
        company_data = trends_df[trends_df["会社名"] == company].copy()

        # データがない場合はスキップ
        if company_data.empty or len(company_data) < 2:
            continue

        company_data = company_data.sort_values("年度")

        # -------- 財務指標の推移グラフ --------
        plt.figure(figsize=(12, 8))

        # グラフの背景色を設定
        ax = plt.gca()
        ax.set_facecolor("#f0f8ff")  # 薄い青色の背景

        # 売上高の推移（主軸）
        ax1 = plt.gca()
        ax1.plot(
            company_data["年度"],
            company_data["売上高_百万円"],
            "b-o",
            label="売上高",
            linewidth=2.5,
        )
        ax1.set_xlabel("年度")
        ax1.set_ylabel("売上高（百万円）", color="b", fontweight="bold")
        ax1.tick_params(axis="y", labelcolor="b")
        ax1.set_ylim(bottom=0)  # Y軸の下限を0に設定

        # Y軸の上限を広げる（最大値の約1.2倍に設定）
        if not company_data["売上高_百万円"].empty:
            ymax = company_data["売上高_百万円"].max()
            ax1.set_ylim(top=ymax * 1.2)

        # 営業利益・当期純利益の推移（副軸）
        ax2 = ax1.twinx()
        ax2.plot(
            company_data["年度"],
            company_data["営業利益_百万円"],
            "r-^",
            label="営業利益",
            linewidth=2.5,
        )
        ax2.plot(
            company_data["年度"],
            company_data["当期純利益_百万円"],
            "g-s",
            label="当期純利益",
            linewidth=2.5,
        )
        ax2.set_ylabel("利益（百万円）", color="r", fontweight="bold")
        ax2.tick_params(axis="y", labelcolor="r")

        # Y軸の下限を0に設定（マイナスの場合は例外）
        if min(company_data["営業利益_百万円"].min(), company_data["当期純利益_百万円"].min()) >= 0:
            ax2.set_ylim(bottom=0)

            # Y軸の上限を広げる（最大値の約1.2倍に設定）
            profit_max = max(
                company_data["営業利益_百万円"].max(), company_data["当期純利益_百万円"].max()
            )
            ax2.set_ylim(top=profit_max * 1.2)

        # グリッド線を追加して見やすくする
        ax1.grid(True, linestyle="--", alpha=0.7)

        # 凡例の設定
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(
            lines1 + lines2, labels1 + labels2, loc="best", facecolor="white", framealpha=0.9
        )

        plt.title(f"{company}の財務指標推移", fontsize=16, fontweight="bold")

        # グラフ全体の外枠を追加
        plt.box(True)

        # ファイル名に使えない文字を置換
        safe_company_name = company.replace("/", "_").replace("\\", "_").replace(":", "_")

        graph_path = os.path.join(individual_charts_dir, f"{safe_company_name}_財務推移.png")
        plt.savefig(graph_path, dpi=300, bbox_inches="tight")
        plt.close()

        graph_files.append(graph_path)

    logger.info(f"個別企業の財務指標グラフを {individual_charts_dir} に保存しました")

    return graph_files


async def create_charts_async(
    trends_df: pd.DataFrame,
    output_dir: Optional[str] = None,
    thread_pool: Optional[ThreadPoolExecutor] = None,
) -> List[str]:
    """財務指標チャートを作成する（非同期版）

    Parameters
    ----------
    trends_df : pandas.DataFrame
        財務推移データを含むDataFrame
    output_dir : str, optional
        グラフの保存先ディレクトリ
    thread_pool : ThreadPoolExecutor, optional
        グラフ生成に使用するスレッドプール

    Returns
    -------
    list
        作成されたグラフファイルのパスのリスト
    """
    loop = asyncio.get_running_loop()

    chart_creating_func = lambda: create_comparison_charts(trends_df, output_dir)

    if thread_pool:
        return await loop.run_in_executor(thread_pool, chart_creating_func)
    else:
        return await loop.run_in_executor(None, chart_creating_func)
