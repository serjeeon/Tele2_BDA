-- Ручное завершение пользовательских сессий в Терадате
-- Например, на случай зависания сессии в Питоне
EXEC sysdba.ABORTSESSIONS('XXXXXXX'); -- SESSION ID from ViewPoint