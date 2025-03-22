"""
EDINETコードリストをダウンロードして、企業名からEDINETコードを検索する
"""

import json
import os
import zipfile

import config
import pandas as pd
import requests


def find_company_in_edinetcode(company_name):
    # 一時ディレクトリ作成
    os.makedirs(config.TEMP_DIR, exist_ok=True)

    # EDINETコードリストのダウンロードとCSV抽出
    zip_path = config.EDINETCODE_ZIP_PATH
    extract_dir = config.TEMP_DIR
    csv_path = config.EDINET_CODE_LIST_CSV

    # 既存のZIPファイルがなければダウンロード
    if not os.path.exists(zip_path) or os.path.getsize(zip_path) < 10000:  # サイズが小さすぎる場合も再ダウンロード
        print(f"{company_name}の検索のため、EDINETコードリストをダウンロードします...")
        try:
            response = requests.get(config.EDINETCODE_ZIP_URL, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()  # エラーチェック
            
            # レスポンスがHTMLではなくZIPファイルであることを確認
            content_type = response.headers.get('Content-Type', '')
            if 'application/zip' not in content_type and 'application/x-zip' not in content_type:
                print(f"エラー: EDINETからZIPファイルが取得できませんでした。Content-Type: {content_type}")
                print("ブラウザで直接アクセスしてZIPファイルをダウンロードしてください。")
                return f"EDINETコードリストのダウンロードに失敗しました ({company_name}の検索失敗)"
            
            with open(zip_path, "wb") as f:
                f.write(response.content)

            # ZIPファイルのチェック
            try:
                with zipfile.ZipFile(zip_path) as zip_test:
                    files = zip_test.namelist()
                    if not files:
                        print("エラー: ダウンロードしたZIPファイルは空です。")
                        return f"ダウンロードしたZIPファイルは空です ({company_name}の検索失敗)"
                    print(f"ZIPファイル内のファイル: {files}")
            except zipfile.BadZipFile:
                print("エラー: ダウンロードしたファイルはZIPファイルではありません。")
                return f"EDINETコードリストがZIPファイルではありません ({company_name}の検索失敗)"

            # 解凍
            with zipfile.ZipFile(zip_path) as zip_f:
                zip_f.extractall(extract_dir)
                
            print(f"EDINETコードリストをダウンロードして解凍しました: {zip_path}")
        except Exception as e:
            print(f"EDINETコードリストのダウンロードに失敗しました: {e}")
            return f"EDINETコードリストのダウンロードに失敗: {e} ({company_name}の検索失敗)"
    else:
        print(f"既存のEDINETコードリストを使用して{company_name}を検索します...")
        try:
            # 既存のZIPファイルが有効か確認
            with zipfile.ZipFile(zip_path) as zip_test:
                pass
        except zipfile.BadZipFile:
            print("エラー: 既存のファイルはZIPファイルではありません。再ダウンロードが必要です。")
            # 無効な場合はファイルを削除
            os.remove(zip_path)
            return f"既存のEDINETコードリストが無効です ({company_name}の検索失敗)"

    # CSVファイルを見つける
    if not os.path.exists(csv_path):
        csv_path = None
        for file in os.listdir(extract_dir):
            if file.endswith(".csv"):
                csv_path = os.path.join(extract_dir, file)
                break

    if not csv_path:
        return f"CSVファイルが見つかりませんでした ({company_name}の検索失敗)"

    # CSV読み込みと企業検索
    try:
        df = pd.read_csv(csv_path, encoding="cp932", skiprows=[0])
        company_rows = df[df["提出者名"].str.contains(company_name, na=False)]

        # 決算日情報も含める
        if "決算日" in df.columns:
            return company_rows[
                ["ＥＤＩＮＥＴコード", "提出者名", "提出者業種", "証券コード", "決算日"]
            ]
        else:
            # 決算日フィールドがなければ確認のためカラム名を表示
            print(f"利用可能なカラム: {df.columns.tolist()}")
            return company_rows[["ＥＤＩＮＥＴコード", "提出者名", "提出者業種", "証券コード"]]
    except Exception as e:
        print(f"CSVファイルの読み込みエラー: {e}")
        return f"CSVファイルの読み込みに失敗しました: {e} ({company_name}の検索失敗)"


def find_auto_manufacturers(csv_path=None):
    """CSVファイルから自動車メーカーの情報を抽出"""
    if csv_path is None:
        csv_path = config.EDINET_CODE_LIST_CSV

    # CSVファイルが存在しない場合
    if not os.path.exists(csv_path):
        print("CSVファイルが見つかりませんでした。")
        return []

    try:
        # 輸送用機器業種の企業を取得
        df = pd.read_csv(csv_path, encoding="cp932", skiprows=[0])
        transport = df[df["提出者業種"] == "輸送用機器"]

        # 主要自動車メーカーを検索
        result = []
        for company_name in config.AUTO_MANUFACTURERS:
            # 完全一致で検索する名前がある場合はそれを使用
            if company_name in config.COMPANY_FULL_NAMES:
                full_name = config.COMPANY_FULL_NAMES[company_name]
                matches = transport[transport["提出者名"] == full_name]
            else:
                # 通常の部分一致検索
                matches = transport[transport["提出者名"].str.contains(company_name, na=False)]
            
            if not matches.empty:
                row = matches.iloc[0]
                company_info = {
                    "name": str(row["提出者名"]),
                    "edinetcode": str(row["ＥＤＩＮＥＴコード"]),
                    "seccode": str(row["証券コード"]),
                }

                # 決算日情報があれば追加
                if "決算日" in row:
                    company_info["fiscal_year_end"] = str(row["決算日"])
                elif "期末日" in row:
                    company_info["fiscal_year_end"] = str(row["期末日"])

                result.append(company_info)
                print(f"企業を見つけました: {row['提出者名']} (EDINETコード: {row['ＥＤＩＮＥＴコード']})")
            else:
                print(f"企業が見つかりませんでした: {company_name}")

        return result
    except Exception as e:
        print(f"自動車メーカー情報の抽出中にエラーが発生しました: {e}")
        return []


def save_company_info(companies, output_file=None):
    """企業情報をJSONファイルに保存"""
    if output_file is None:
        output_file = config.COMPANY_INFO_JSON

    # データが空の場合は保存しない
    if not companies:
        print("保存する企業情報がありません")
        return

    # 出力ディレクトリの作成
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # JSONファイルに保存
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(companies, f, ensure_ascii=False, indent=2)

    print(f"{len(companies)}社の企業情報を {output_file} に保存しました")


def download_edinetcode_list():
    """EDINETコードリストをダウンロードする"""
    # 一時ディレクトリ作成
    os.makedirs(config.TEMP_DIR, exist_ok=True)

    # 既存のZIPファイルがなければダウンロード
    zip_path = config.EDINETCODE_ZIP_PATH
    if not os.path.exists(zip_path) or os.path.getsize(zip_path) < 10000:  # 小さすぎる場合も再ダウンロード
        print("EDINETコードリストをダウンロードします...")
        try:
            # 単純なリクエスト
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            }
            response = requests.get(config.EDINETCODE_ZIP_URL, headers=headers, timeout=config.REQUEST_TIMEOUT)
            response.raise_for_status()
            
            # ファイルを保存
            with open(zip_path, "wb") as f:
                f.write(response.content)
                
            print(f"EDINETコードリストをダウンロードしました: {zip_path}")
            
            # ZIPファイルを解凍
            try:
                with zipfile.ZipFile(zip_path) as zip_f:
                    zip_f.extractall(config.TEMP_DIR)
                print("ZIPファイルを解凍しました")
                return True
            except zipfile.BadZipFile:
                print("エラー: ダウンロードしたファイルはZIPファイルではありません")
                print("ブラウザで直接ダウンロードして、手動で配置してください")
                return False
                
        except Exception as e:
            print(f"EDINETコードリストのダウンロードに失敗しました: {e}")
            return False
    else:
        print(f"既存のEDINETコードリストファイルを使用します: {zip_path}")
        
        # ZIPファイルが有効か確認
        try:
            with zipfile.ZipFile(zip_path) as zip_f:
                # CSVファイルがなければ解凍
                if not os.path.exists(config.EDINET_CODE_LIST_CSV):
                    zip_f.extractall(config.TEMP_DIR)
                    print("既存のZIPファイルから解凍しました")
            return True
        except zipfile.BadZipFile:
            print("エラー: 既存のファイルはZIPファイルではありません。再ダウンロードします")
            os.remove(zip_path)
            return download_edinetcode_list()  # 再帰的に呼び出し


def find_edinetcode_csv():
    """EDINETコードリストのCSVファイルを見つける"""
    # 直接指定されたパスが存在する場合はそれを使用
    if os.path.exists(config.EDINET_CODE_LIST_CSV):
        return config.EDINET_CODE_LIST_CSV

    # TEMPディレクトリ内のCSVファイルを探す
    try:
        for file in os.listdir(config.TEMP_DIR):
            if file.endswith(".csv"):
                csv_path = os.path.join(config.TEMP_DIR, file)
                print(f"EDINETコードリストCSVが見つかりました: {csv_path}")
                return csv_path
    except Exception as e:
        print(f"ディレクトリの読み取りに失敗しました: {e}")

    print("EDINETコードリストCSVが見つかりませんでした")
    return None


def main():
    """企業情報取得のメイン処理"""
    print("主要企業の情報を取得します...")

    # 必要なディレクトリを確認・作成
    for directory in [config.DATA_DIR, config.TEMP_DIR, config.RESULTS_DIR]:
        os.makedirs(directory, exist_ok=True)
        print(f"ディレクトリを確認/作成しました: {directory}")

    # EDINETコードリストをダウンロード
    if not download_edinetcode_list():
        print("EDINETコードリストのダウンロードに失敗しました。処理を中止します。")
        print("※ブラウザで直接ダウンロードして、手動で以下のパスに配置することも可能です:")
        print(f"  配置先: {config.EDINETCODE_ZIP_PATH}")
        return 1

    # CSVファイルのパス
    csv_path = find_edinetcode_csv()
    if not csv_path:
        print("EDINETコードリストが見つかりません。処理を中止します。")
        return 1

    # 自動車メーカーの情報を取得
    companies = find_auto_manufacturers(csv_path)

    if not companies:
        print("自動車メーカーの情報が取得できませんでした。")
        return 1

    # 取得結果を表示
    print(f"\n{len(companies)}社の情報を取得しました:")
    for company in companies:
        print(
            f"- {company['name']} (証券コード: {company['seccode']}, EDINETコード: {company['edinetcode']})"
        )

    # JSONファイルに保存
    save_company_info(companies)

    return 0


# 実行して結果表示
if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
