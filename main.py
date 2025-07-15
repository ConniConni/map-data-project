import psycopg2
import psycopg2.extras
import argparse
import pandas as pd
from sqlalchemy import create_engine
import csv
import datetime

"""
流れ ~psycopg2~
1. DB接続情報を定義
2. クエリを作成
3. psycpg2でDB接続
4. with構文を使い、DBと対話可能な状態にする
5. CSV書き込み
6. エラーハンドリング
7. DB切断

流れ ~psycopg2 & pandas~
1. DB接続情報を定義
2. クエリを作成
3. sqlalchemyでデータベースへの接続エンジンを作成
4. SQLを実行し、結果を直接PandasのDataFrameに読み込む
5. CSV書き込み
6. エラーハンドリング
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
    prefecture = %s;
"""


def main(prefecture_name):
    print(f"処理を開始します: 都道府県名 = {prefecture_name}")
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
            cur.execute(SQL_QUERY, (prefecture_name,))

            # 実行結果をすべて取得
            results = cur.fetchall()

            if not results:  # resultsリストが空の場合
                print(
                    f"エラー: '{prefecture_name}' に該当するデータが見つかりませんでした。"
                )
                print(
                    "正しい都道府県名(例: '北海道','東京都','福岡県')を入力してください。"
                )
                # この後の処理は行わないように、ここで関数を終了させる
                return

            now = datetime.datetime.now()
            formatted_string = now.strftime("%Y%m%d_%H%M%S")

            output_filename = f"{formatted_string}_areas.csv"
            try:
                with open(output_filename, "w", encoding="utf-8-sig", newline="") as f:
                    # ヘッダーをDictCursorのキーから動的に取得
                    header = results[0].keys()
                    writer = csv.writer(f)
                    writer.writerow(header)  # ヘッダーを書き込む

                    # 各行のデータを書き込む
                    for row in results:
                        writer.writerow(row)  # DictRowはそのままwriterowに渡せる

                print(f"データを'{output_filename}'に保存しました。")

            except IOError as e:
                print(f"ファイル書き込みエラー: {e}")

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

    # pandasを使用
    print("pandasを使ってデータを取得し、CSVに保存します...")

    try:
        # データベースへの接続エンジンを作成
        # f-stringで接続文字列を作成
        engine_str = (
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
            f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )
        print(engine_str)
        engine = create_engine(engine_str)

        params = (prefecture_name,)

        # SQLを実行し、結果を直接PandasのDataFrameに読み込む
        df = pd.read_sql_query(SQL_QUERY, engine, params=params)

        print("--- 取得したデータ（先頭5件） ---")
        print(df.head())

        # DataFrameをCSVファイルに出力
        output_filename = f"{formatted_string}_areas_pandas.csv"
        df.to_csv(output_filename, index=False, encoding="utf-8-sig")

        print(f"\nデータを'{output_filename}'に保存しました。")

    except Exception as e:
        print(f"エラーが発生しました: {e}")


if __name__ == "__main__":
    # パーサーを作成
    parser = argparse.ArgumentParser(
        description="指定された都道府県の市区町村データをDBから取得し、CSVに出力します。"
    )

    # 受け付ける「オプション引数」を定義
    parser.add_argument(
        "-p",
        "--prefecture",  # 短い名前 (-p) と長い名前 (--prefecture)
        type=str,  # 型は文字列
        required=True,  # このオプションは必須である、と指定
        help="取得したい都道府県名を指定します (例: '東京都')",
        metavar="PREFECTURE_NAME",  # ヘルプメッセージでの表示名を指定
    )

    # コマンドライン引数を解析
    args = parser.parse_args()

    # 解析結果を使って、メインの関数を呼び出す
    # args.prefecture のように、長い方の名前でアクセスする
    main(args.prefecture)
