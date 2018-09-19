SELECT
  t.DatabaseName,
  t.TableName,
  -- currently used diskspace in GB
  SUM(t.CurrentPerm) / 1024 ** 3 (DEC(9, 2)) AS CurrentPermGB,
  -- currently needed diskspace in GB to store this table as standalone (due to Skew)
  MAX(t.CurrentPerm) / 1024 ** 3 * (HASHAMP() + 1) (DEC(9, 2)) AS SkewedPermGB,
  SkewedPermGB - CurrentPermGB AS WastedPermGB,
  -- AMP with highest disk usage
  MAX(t.MaxPermAMP) AS SkewedAMP,
  -- skew factor, 1 = even distribution, 1.1 = max AMP needs 10% more space than the average AMP
  MAX(t.CurrentPerm) / NULLIF(AVG(t.CurrentPerm), 0) (DEC(5, 2)) AS SkewFactor,
  -- skew factor, between 0 and 99.  Same calculation as WinDDI/ TD Administrator
  (100 - (AVG(t.CurrentPerm) / NULLIF(MAX(t.CurrentPerm), 0) * 100)) (DEC(3, 0)) AS SkewFactor_WINDDI
FROM
(
  SELECT
    DatabaseName,
    TableName,
    CurrentPerm,
    CASE WHEN CurrentPerm = MAX(CurrentPerm) OVER (PARTITION BY DatabaseName, TableName)
      THEN vproc END AS MaxPermAMP
    FROM dbc.TableSizeV
  WHERE 1=1
    AND DatabaseName = 'UAT_DM'
    AND tablename like 'XX%'		-- !!! change XX to your prefix !!!
) AS t
GROUP BY 1, 2
ORDER BY WastedPermGB DESC
;