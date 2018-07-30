-- построение буффера в метрах
SELECT POLY_ID, POLYGON_BUF
FROM
(
  SELECT POLY_ID,
  POLYGON.ST_Transform(t2.SRTEXT, t3.SRTEXT) AS POLYGON_TRANS, -- преобразование в метрическую систему координат
  POLYGON_TRANS.ST_Buffer(100) AS BUFFER_TRANS, -- построение буффера на 100 метров
  BUFFER_TRANS.ST_Transform(t3.SRTEXT, t2.SRTEXT) AS POLYGON_BUF -- обратное преобразование в широту/долготу
  FROM
  (
    SELECT POLY_ID, POLYGON, POLYGON.ST_Centroid().ST_X() AS POLY_CENTR_X
    FROM UAT_DM.GEO_POLYGON
    WHERE POLY_ID = 'L20985' -- Ekb
  ) t1,
  UAT_DM.gi_utm_proj t2,
  (
    SELECT * 
    FROM SYSSPATIAL.SPATIAL_REF_SYS 
    WHERE AUTH_SRID = 4326 -- WGS84 projection
  ) t3
  WHERE t1.POLY_CENTR_X BETWEEN t2.LON_MIN AND t2.LON_MAX -- get an appropriate UTM projection
) t4