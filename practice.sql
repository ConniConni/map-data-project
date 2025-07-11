-- 1-1. ビューを設定

CREATE VIEW V_JAPAN_ADMIN AS
SELECT
    id,
    geom,
    "N03_001" AS prefecture,
    "N03_003" AS county_or_seirei_city,
    "N03_004" AS city_name,
    "N03_007" AS admin_code
FROM
    japan_admin_areas;

-- 1-2. ビューの削除

DROP VIEW v_japan_admin;


-- 2. 東京都の図形情報をテキストで見てみる

SELECT 
    id,
	prefecture,
	city_name,
	admin_code,
	ST_AsText(geom)
FROM
    V_JAPAN_ADMIN
WHERE
    prefecture = '東京都';

-- 3. 東京都の区を面積の大きい順に並べる

SELECT     
    id,
	prefecture,
	city_name,
	admin_code,
	ST_Area(geom) AS area
FROM
    V_JAPAN_ADMIN
WHERE 
    prefecture = '東京都'
AND 
    city_name LIKE '%区'
ORDER BY
    area DESC;

-- 4. 東京駅はどの区にある？（空間検索）

SELECT     
    id,
	prefecture,
	city_name,
	admin_code
FROM
    V_JAPAN_ADMIN
WHERE
    -- ST_Contains 対象のポリゴン同士が内側で重なった場合Trueを返す
	-- ST_SetSRID 対象のジオメトリにID(WGS 84：4326)のラベルを貼る
    ST_Contains(geom, ST_SetSRID(
	    -- Pointオブジェクトを生成する
	    ST_MakePoint(139.7671, 35.6812), 4326));

-- テーブルのジオメトリカラムのSRIDを変更するSQLを実行

SELECT UpdateGeometrySRID('japan_admin_areas', 'geom', 4326);
