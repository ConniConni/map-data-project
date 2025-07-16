import psycopg2


# DB接続情報を定義
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "gisdb",
    "user": "postgres",
    "password": "postgres",
}


def main():
    conn = None  # connを初期化
    try:
        # DB接続を行う
        conn = psycopg2.connect(**DB_CONFIG)
        print("#### DBに接続しました。 ####")

        # カーソルを作成
        cur = conn.cursor()
        print("#### カーソルを作成しました。 ####")

        # カーソルを削除
        cur.close()
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
