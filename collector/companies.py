"""
EDINET APIを利用して自動車メーカーの企業情報を取得・保存するモジュール

このモジュールはEDINETコードリストからデータを抽出し、特定の業種（輸送用機器）の
企業情報を取得してJSON形式で保存します。

関数:
    find_auto_manufacturers: 輸送用機器業種の企業を抽出
    save_company_info: 企業情報をJSONファイルとして保存
    main: メイン処理を実行
"""

import json
import logging
import os
import sys

import pandas as pd

from config import base_settings, settings

# ロガーの設定
logger = logging.getLogger(__name__)


def find_auto_manufacturers(csv_path):
    """自動車メーカーの情報を抽出する

    EDINETコードリストから輸送用機器業種の企業情報を抽出し、
    設定ファイルに定義された自動車メーカー情報を取得します。

    Parameters
    ----------
    csv_path : str
        EDINETコードリストZIPファイルのパス

    Returns
    -------
    list of dict
        企業情報のリスト。各辞書には次のキーが含まれます:
        - name: 企業名
        - edinetcode: EDINETコード
        - seccode: 証券コード
        - fiscal_year_end: 決算日

    Raises
    ------
    ValueError
        対象企業が見つからない場合
    """
    logger.info(f"EDINETコードリストから自動車メーカーを抽出します: {csv_path}")

    # ZIPファイル内のCSVファイルを読み込む
    df = pd.read_csv(csv_path, encoding="cp932", skiprows=[0])

    # 参考のためにカラム名を表示
    logger.debug(f"利用可能なカラム: {df.columns.tolist()}")

    # すべての対象企業を一度に取得
    target_companies = list(settings.COMPANY_FULL_NAMES.values())
    matches = df[(df["提出者業種"] == "輸送用機器") & (df["提出者名"].isin(target_companies))]

    logger.info(f"輸送用機器業種の対象企業数: {len(matches)}")

    # 結果を処理
    result = []
    for _, row in matches.iterrows():
        company_info = {
            "name": str(row["提出者名"]),
            "edinetcode": str(row["ＥＤＩＮＥＴコード"]),
            "seccode": str(row["証券コード"]),
            "fiscal_year_end": str(row["決算日"]),
        }

        result.append(company_info)
        logger.info(f"企業を見つけました: {row['提出者名']}")

    # 見つからなかった企業の情報をログに出力
    found_companies = set(matches["提出者名"].tolist())
    for company_name, full_name in settings.COMPANY_FULL_NAMES.items():
        if full_name not in found_companies:
            logger.warning(f"企業が見つかりませんでした: {company_name}")

    return result


def save_company_info(companies, output_file):
    """企業情報をJSONファイルに保存

    Parameters
    ----------
    companies : list of dict
        保存する企業情報のリスト
    output_file : str
        出力するJSONファイルのパス

    Returns
    -------
    None
    """
    # データが空の場合は保存しない
    if not companies:
        logger.warning("保存する企業情報がありません")
        return

    # 出力ディレクトリの作成
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # JSONファイルに保存
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    logger.info(f"{len(companies)}社の企業情報を {output_file} に保存しました")


def main():
    """企業情報取得のメイン処理

    EDINETコードリストから自動車メーカーの情報を取得し、
    JSONファイルに保存します。

    Returns
    -------
    int
        処理結果コード (0: 成功)

    Raises
    ------
    FileNotFoundError
        EDINETコードリストファイルが見つからない場合
    ValueError
        自動車メーカーの情報が取得できない場合
    """
    logger.info("主要企業の情報を取得します...")

    # 必要なディレクトリを確認・作成
    for directory in [base_settings.DATA_DIR, base_settings.RESULTS_DIR]:
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"ディレクトリを確認/作成しました: {directory}")

    # EDINETコードリストファイルの存在確認
    zip_path = base_settings.EDINETCODE_ZIP_PATH
    if not os.path.exists(zip_path):
        error_msg = f"EDINETコードリストが見つかりません: {zip_path}"
        logger.error(error_msg)
        logger.error("以下の手順でファイルを手動で準備してください：")
        logger.error("1. ブラウザでEDINET「コード一覧・EDINET提出書類公開サイト」ページにアクセス")
        logger.error("   https://disclosure.edinet-fsa.go.jp/")
        logger.error("2. 「EDINETコード一覧」をダウンロード")
        logger.error(f"3. ダウンロードしたZIPファイルを次の場所に配置: {zip_path}")
        raise FileNotFoundError(error_msg)

    # 自動車メーカーの情報を取得
    companies = find_auto_manufacturers(zip_path)

    if not companies:
        error_msg = "自動車メーカーの情報が取得できませんでした。"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # 取得結果を表示
    logger.info(f"\n{len(companies)}社の情報を取得しました:")
    for company in companies:
        logger.info(
            f"- {company['name']} (証券コード: {company['seccode']}, EDINETコード: {company['edinetcode']})"
        )

    # JSONファイルに保存
    save_company_info(companies, base_settings.COMPANY_INFO_JSON)

    return 0


# 実行して結果表示
if __name__ == "__main__":
    # ロギングの基本設定
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    exit_code = main()
    sys.exit(exit_code)
