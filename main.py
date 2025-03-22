"""
EDINET企業情報取得・有価証券報告書データ解析ツール
"""

import os
import sys
import time
import traceback

import companies
import config
import research
import xbrl_getter_async


def main():
    """
    各モジュールを順番に実行するメイン関数

    1. companies.py: 企業情報を取得してJSONに保存
    2. research.py: EDINETから企業の有価証券報告書情報を検索
    3. xbrl_getter_async.py: XBRLデータを取得して財務情報を抽出
    """
    print("===== EDINET企業情報取得・有価証券報告書データ解析ツール =====")

    try:
        # 1. 企業情報の取得（companies.py）
        print("\n[Step 1] 企業情報の取得と保存")
        companies_result = companies.main()

        if companies_result != 0:
            print(
                "警告: 企業情報の取得に問題がありました。処理を続行しますが、結果に問題がある可能性があります。"
            )
        else:
            print("\n企業情報の取得が完了しました。\n")

        # 2. 有価証券報告書の検索（research.py）
        print("\n[Step 2] 有価証券報告書の検索")

        # 企業情報ファイルの確認
        if not os.path.exists(config.COMPANY_INFO_JSON):
            print("警告: 企業情報ファイルが見つかりません。正しく処理できない可能性があります。")

        try:
            research_result = research.main()
            if research_result != 0:
                print(
                    "警告: 有価証券報告書の検索に問題がありました。処理を続行しますが、結果に問題がある可能性があります。"
                )
            else:
                print("\n有価証券報告書の検索が完了しました。\n")
        except Exception as e:
            print(f"有価証券報告書の検索中にエラーが発生しました: {e}")
            print("Step 3に進みます。")

        # 3. XBRLデータの取得と財務情報抽出（xbrl_getter_async.py）
        print("\n[Step 3] XBRLデータの取得と財務情報抽出")

        # 有価証券報告書データファイルの存在確認
        csv_files = [
            config.ALL_DOCUMENTS_CSV,
            config.FILTERED_DOCUMENTS_CSV,
            config.SECURITIES_REPORTS_CSV,
            config.FILTERED_SECURITIES_REPORTS_CSV,
            config.FIXED_FILTERED_SECURITIES_REPORTS_CSV,
        ]

        xbrl_input_file = None
        for file_path in csv_files:
            if os.path.exists(file_path):
                xbrl_input_file = file_path
                print(f"XBRLデータ解析の入力ファイルとして {file_path} を使用します。")
                break

        if not xbrl_input_file:
            print("警告: 有価証券報告書データファイルが見つかりません。")
            print("Step 3をスキップします。")
            return 1

        try:
            xbrl_result = xbrl_getter_async.main()
            if xbrl_result != 0:
                print("警告: XBRLデータの取得と財務情報抽出に問題がありました。")
            else:
                print("\nXBRLデータの取得と財務情報抽出が完了しました。\n")
        except Exception as e:
            print(f"XBRLデータの取得と財務情報抽出中にエラーが発生しました: {e}")
            print(traceback.format_exc())

        # 全処理の完了
        print("\n===== 処理が完了しました =====")
        print("結果ファイルは以下のディレクトリに保存されている可能性があります:")
        print(f"- 企業情報: {config.COMPANY_INFO_JSON}")
        print(f"- 有価証券報告書データ: {config.FIXED_FILTERED_SECURITIES_REPORTS_CSV}")
        print(f"- 財務指標データ: {config.FINANCIAL_INDICATORS_CSV}")
        print(f"- 財務推移データ: {config.ALL_COMPANIES_FINANCIAL_TRENDS_CSV}")
        print(f"- 財務チャート: {config.CHARTS_DIR}")

        return 0

    except Exception as e:
        print(f"\n全体の処理中にエラーが発生しました: {e}")
        print(traceback.format_exc())
        return 1


if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\n\nプログラムが中断されました。")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nエラーが発生しました: {e}")
        traceback.print_exc()
        sys.exit(1)
