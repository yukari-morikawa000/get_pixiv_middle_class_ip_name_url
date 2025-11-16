# unified_pixiv_search.py

from playwright.sync_api import sync_playwright # type: ignore
import urllib.parse
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import time
import random
import unicodedata
import re # 正規表現
import os

# middle_class_ip_nameを整形
from clean_name import clean_ip_name 

# PROJECT_ID = "hogeticlab-legs-prd"
# DATASET_ID = "z_personal_morikawa"  
# digファイルから渡される環境変数
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DATASET_ID = os.environ.get("BIGQUERY_DATASET")

# マスターIPリストのテーブル
MASTER_TABLE = "master_spreadsheet.master_ip_list_twitter_account_tb"
MASTER_COLUMN = "ip_name" 
# すでにピクシブ百科事典からURLを取得済みのテーブル
PROCESSED_TABLE = f"{DATASET_ID}.pixiv_search_middle_class_ip_name_url"
PROCESSED_COLUMN = "middle_class_ip_name" 
# 結果をロードする一時テーブル
BQ_TARGET_TABLE = f"{DATASET_ID}.pixiv_search_results"

# 全角半角の判別
def normalize(text):
    return unicodedata.normalize("NFKC", text.strip().lower())

# BigQueryでの処理
def get_bq_names(table_id: str, column_name: str) -> set:

    print(f"BigQuery: {table_id} から {column_name} を取得中...")
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"SELECT DISTINCT {column_name} FROM `{PROJECT_ID}.{table_id}`"
        df = client.query(query).to_dataframe()
        
        # NaNを除外し、ユニークな文字列のセットを返す
        unique_names = set(df[column_name].dropna().astype(str))
        print(f"-> {table_id} から {len(unique_names)} 件のユニークデータを取得しました。")
        return unique_names
        
    except Exception as e:
        print(f"BigQueryからのデータ取得中にエラーが発生しました: {e}")
        return set()

#  Pixiv検索関数
def search_pixiv_dic(keyword):
    """
    PlaywrightでPixiv百科事典を検索し、完全一致するタイトルとURL、
    および検索結果の件数を取得する。
    """
    search_url = "https://dic.pixiv.net/search?query=" + urllib.parse.quote(keyword)
    results = []
    hit_count = 0 # 件数カウント
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            try:
                page.goto(search_url, timeout=60000)

                # 件数取得
                try:
                    info_locator = page.locator("header .info")
                    if info_locator.count() > 0:
                        info_text = info_locator.nth(0).inner_text().strip()
                        # '検索結果：XXX件' の形式から数値を抽出
                        m = re.search(r"検索結果：(\d+)件", info_text)
                        if m:
                            hit_count = int(m.group(1))
                except Exception as e:
                    print(f"件数取得失敗: {e}")

                # 完全一致のタイトルとURLを取得
                articles = page.locator("article")
                count = articles.count()
                for i in range(count):
                    title_el = articles.nth(i).locator("div.info h2 a")
                    if title_el.count() == 0:
                        continue

                    title = title_el.inner_text().strip()
                    href = title_el.get_attribute("href")
                    
                    if normalize(title) == normalize(keyword): 
                        full_url = urllib.parse.urljoin("https://dic.pixiv.net", href)
                        results.append((title, full_url))
                
                browser.close()

            except Exception as e:
                print(f"Playwright/検索エラー: {e}")
                return [], 0 # 内部エラー時、結果と件数0を返す
            
    except Exception as e:
        print(f"全体エラー: {e}")
        return [], 0 # 全体エラー時、結果と件数0を返す

    return results, hit_count # 結果と件数を返す

# メイン処理
def main():
    print("--- BigQuery差分抽出・Pixiv検索・BQロード処理 開始 ---")
    
    bq_client = bigquery.Client(project=PROJECT_ID)
    
    # BQからデータ取得
    master_names = get_bq_names(MASTER_TABLE, MASTER_COLUMN)
    processed_names = get_bq_names(PROCESSED_TABLE, PROCESSED_COLUMN)

    # 差分を抽出
    new_names = master_names - processed_names

    if not new_names:
        print("\n✨ 新規に追加・未処理のIP名はありませんでした。処理を終了します。")
        return

    print("-" * 30)
    print(f"✨ {len(new_names)} 件の新規/未処理のIP名が見つかりました。")
    
    # 検索タスクの準備と整形
    tasks = []
    for original_name in sorted(list(new_names)):
        cleaned_name = clean_ip_name(original_name)
        if cleaned_name:
            tasks.append({
                "original_name": original_name,
                "cleaned_name": cleaned_name
            })
            
    if not tasks:
        print("整形後、処理対象となるデータはありませんでした。")
        return
        
    print(f"整形後、検索対象となるデータ: {len(tasks)} 件")

    # --- テスト用（本番実行の場合はここはコメントアウトする） ----
    TEST_LIMIT = 5
    if len(tasks) > TEST_LIMIT:
        tasks = tasks[:TEST_LIMIT]
        print(f"テストのため、検索対象を先頭の {TEST_LIMIT} 件に制限します。")
    # --- ここまで ------

    # Pixiv検索とBQデータ準備
    rows_to_insert = []
    
    for i, task in enumerate(tasks):
        original_name = task['original_name']
        cleaned_name = task['cleaned_name']
        
        print(f"({i + 1}/{len(tasks)}) 検索: {cleaned_name} (元: {original_name})")

        try:
            # 件数を受け取るように変更
            candidates, hit_count = search_pixiv_dic(cleaned_name) 
            # 更新日
            today_date = datetime.now().date().isoformat()
            
            if candidates:
                for title, link in candidates:
                    print(f"  ヒット: {title} → {link}")
                    rows_to_insert.append({
                        "middle_class_ip_name": original_name,
                        "clean_middle_class_ip_name": cleaned_name,
                        "URL": link,
                        "pixiv_search_result_ip": title,
                        "search_hit_num": None, # ヒットする場合は必要なし
                        "update": today_date,
                    })
            else:
                print(f"該当なし (検索結果件数: {hit_count}件)")
                rows_to_insert.append({
                    "middle_class_ip_name": original_name,
                    "clean_middle_class_ip_name": cleaned_name,
                    "URL": "",
                    "pixiv_search_result_ip": "該当なし",
                    "search_hit_num": hit_count, # 件数を追加
                    "update": today_date,
                })

        except Exception as e:
            print(f"  重大エラー発生（BigQuery送信対象）：{e}")
            rows_to_insert.append({
                "middle_class_ip_name": original_name,
                "clean_middle_class_ip_name": cleaned_name,
                "URL": str(e),
                "pixiv_search_result_ip": "ERROR",
                "search_hit_num": -1, # エラー時は-1などを設定
                "update": today_date,
            })

        # サイトへの負荷を考慮し、処理ごとに待機
        time.sleep(random.uniform(5, 10)) 

 # BigQueryに一括送信 (全件洗い替え WRITE_TRUNCATE を使用)
    if rows_to_insert:
        print("-" * 30)
        print(f"合計 {len(rows_to_insert)} 件の検索結果をBigQueryにロード中...")
        
        # 挿入用のDataFrameに変換
        result_df = pd.DataFrame(rows_to_insert)
        
        job_config = bigquery.LoadJobConfig(
            # 更新の際は、テーブルの全データを削除して上書き
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        try:
            # DataFrameをBigQueryにロード
            job = bq_client.load_table_from_dataframe(
                result_df, BQ_TARGET_TABLE, job_config=job_config
            )
            job.result()  # ロードジョブの完了を待機
            
            print(f"🎉 BigQuery一時テーブル ({BQ_TARGET_TABLE}) へのロードが完了しました。")
            print(f" (テーブルは全 {len(result_df)} 件で上書きされました)")
            
        except Exception as e:
            print(f" BigQueryへのロード中にエラーが発生しました: {e}")
            
    else:
        print("検索結果が得られたデータがないため、BigQueryへのロードはスキップされました。")


if __name__ == "__main__":
    main()