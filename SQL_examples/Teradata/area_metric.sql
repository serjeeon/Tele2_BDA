-- вычисление площади полигона в метрической системе координат
SELECT POLY_ID, POLYGON.ST_Transform(t2.SRTEXT, t3.SRTEXT).ST_Area() / 1000000 AS AREA_CALC -- km^2
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