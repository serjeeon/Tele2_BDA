-- Есть более короткий запрос, но он обрезает названия колонок - этот вроде бы работает правильно
SELECT FieldName
FROM DBC.TVFIELDS WHERE TABLEID =
(SELECT 
TVMID 
FROM DBC.TVM 
WHERE TVMNAME = 'AL_SUBS_PRICE_VITRINE'
AND DATABASEID = 
(SELECT DATABASEID FROM DBC.DBASE WHERE DATABASENAME = 'UAT_DM'))