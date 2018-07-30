-- быстрая загрузка полигонов через ODBC + обработка больших геометрий
SELECT POLY_ID, 
CASE 
	WHEN LENGTH(CAST(POLYGON.ST_ASTEXT() AS VARCHAR(32000))) = 32000 
	THEN CAST(POLYGON.SimplifyPreserveTopology(0.001).ST_ASTEXT() AS VARCHAR(32000)) 
	ELSE CAST(POLYGON.ST_ASTEXT() AS VARCHAR(32000))
END AS POLY
FROM UAT_DM.GEO_POLYGON
WHERE POLY_ID = 'L45517' AND PART = 2 -- Moscow
-- initial polygon char length == 155933
-- simplified polygon char length == 12185