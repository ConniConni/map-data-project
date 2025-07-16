import psycopg2
import psycopg2.extras
from itertools import islice
import csv


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
    prefecture = '岩手県';
"""


def main():
    conn = None  # connを初期化
    try:
        # DB接続を行う
        conn = psycopg2.connect(**DB_CONFIG)
        print("#### DBに接続しました。 ####")

        # カーソルを作成
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            print("#### カーソルを作成しました。 ####")

            # SQLを実行
            cur.execute(SQL_QUERY)

            # 実行結果をまとめて取得する
            rows = cur.fetchall()

            # 実行結果がない場合、コンソールへの表示をスキップしてDB接続を閉じる
            if not rows:
                print("データが見つかりませんでした。")
                return

            # コンソールに実行結果を表示（結果は１件のみ表示）
            print("=== 実行結果表示 ===")
            for row in islice(rows, 1):
                print(f"市区町村名: {row['city_name']},面積: {row['area']}")

            # 結果をcsvに書き込み（30件のみ）
            try:
                with open("iwate_areas.csv", "w", encoding="utf-8") as f:
                    header = rows[0].keys()
                    writer = csv.writer(f)
                    writer.writerow(header)

                    # 各行のデータを書き込む
                    for row in islice(rows, 29):
                        writer.writerow(row)

                    print("#### csvファイルに保存しました。 ####")

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
    main()
