import psycopg2
import psycopg2.extras

"""
流れ
1. DB接続情報を定義
2. クエリを作成
3. psycpg2でDB接続
4. with構文を使い、DBと対話可能な状態にする
5. エラーハンドリング
6. DB切断
"""


# データベース接続情報
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "dbname": "gisdb",
    "user": "postgres",
    "password": "postgres",
}

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
    prefecture = '東京都';
"""


def fetch_data_with_psycopg2():
    print("psycopg2を使ってデータを取得します...")
    """
    1. データベース接続
    2. データベースと対話的に処理を行うためカーソル作成
    3. カードルは使用が終わったら閉じて、リソースを解放する(with構文使用)
    4. 取得データを扱いやすくするために、conn.cursorの引数にcursor_factory=psycopg2.extras.DictCursorを指定する
    5. 使用後はデータベースの接続を閉じる
    """

    conn = None  # 接続オブジェクトを初期化
    try:
        # データベースに接続
        conn = psycopg2.connect(**DB_CONFIG)

        # カーソル（SQLを実行するための道具）を作成
        # cursor_factoryを使うと、結果を辞書形式で受け取れる
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # SQLを実行
            cur.execute(SQL_QUERY)

            # 実行結果をすべて取得
            results = cur.fetchall()

            print("--- 取得結果 ---")
            for row in results:
                print(f"市区町村名: {row['city_name']}, 面積: {row['area']}")

    except psycopg2.Error as e:
        print(f"データベースエラー: {e}")
    finally:
        # 接続を必ず閉じる
        if conn is not None:
            conn.close()
            print("データベース接続を閉じました。")


if __name__ == "__main__":
    fetch_data_with_psycopg2()
