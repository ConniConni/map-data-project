import psycopg2
import psycopg2.extras
from itertools import islice
import csv
import argparse
import datetime

# DB接続情報を定義
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "gisdb",
    "user": "postgres",
    "password": "postgres",
}

# 東京都に絞り、id,都道府県,市区町村名,管理コード,エリアテキストを表示するSQL文
SQL_QUERY = """
SELECT
    id,
	prefecture,
	city_name,
	admin_code,
	ST_AsText(geom) AS area
FROM
    V_JAPAN_ADMIN
WHERE
    prefecture = %s;
"""


def main(prefecture_name):
    conn = None  # connを初期化
    try:
        # DB接続を行う
        conn = psycopg2.connect(**DB_CONFIG)
        print("#### DBに接続しました。 ####")

        # カーソルを作成
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            print("#### カーソルを作成しました。 ####")

            # SQLを実行
            cur.execute(SQL_QUERY, (prefecture_name,))

            # 実行結果をまとめて取得する
            rows = cur.fetchall()

            # 実行結果がない場合、コンソールへの表示をスキップしてDB接続を閉じる
            if not rows:
                print(f"{prefecture_name}に該当するデータが見つかりませんでした。")

            else:
                # コンソールに実行結果を表示（結果は１件のみ表示）
                print("=== 実行結果表示（１件のみ） ===")
                for row in islice(rows, 1):
                    print(f"市区町村名: {row['city_name']},面積: {row['area']}")

                # csvファイルは現在時刻で yyyymmdd_hhmmss_areas.csv とする
                now_str = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = now_str + "_areas.csv"

                # 結果をcsvに書き込み（30件のみ）
                try:
                    with open(file_name, "w", encoding="utf-8") as f:
                        header = rows[0].keys()
                        writer = csv.writer(f)
                        writer.writerow(header)

                        # 各行のデータを書き込む
                        for row in islice(rows, 29):
                            writer.writerow(row)

                        print(f"#### {file_name}に保存しました。 ####")

                except IOError as e:
                    print(f"ファイル書き込みエラー：{e}")

        print("#### カーソルを閉じました。 ####")

    except psycopg2.Error as e:
        print(f"データベースエラー: {e}")

    finally:
        if conn is not None:
            # DB接続を閉じる
            conn.close()
            print("#### DB接続を閉じました。 ####")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="都道府県を指定して、該当の市区長村名と面積をCSV形式で取得する。(指定なしの場合は東京都が指定される)"
    )
    parser.add_argument(
        "-p",
        "--prefecture",
        type=str,
        default="東京都",
        help="都道府県名を入力してください。",
        metavar="PREFECTURE_NAME",  # ヘルプメッセージでの表示名を指定
    )
    args = parser.parse_args()
    main(args.prefecture)
