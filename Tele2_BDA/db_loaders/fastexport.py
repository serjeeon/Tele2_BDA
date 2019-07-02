"""Import data from a Teradata table to a csv file using Teradata FastExport utility
"""

import pandas as pd
from turbodbc import connect, DatabaseError
import subprocess

def td_export_tab(fexp_path, outmod_dll, out_file, driver, host, uid, pwd, table_name, 
                  log_table, columns=None, where_clause=''):
    """
    Import data from a Teradata table to a csv file using FastExport utility

    Parameters
    ----------
    fexp_path: path to Teradata fexp.exe utility, could be 'C:\\Program Files (x86)\\Teradata\\Client\\16.10\\bin\\fexp.exe'
    outmod_dll: path to OUTMOD DLL file, which should be used in the FastExport utility to process beginning two bytes in VARCHAR columns 
    out_file: output file name
    driver: Teradata database ODBC driver name, should be == 'Teradata Database ODBC Driver 16.10' unless it has a different version on your PC
    host: Teradata server info - name or IP address
    uid: Teradata username
    pwd: Teradata password
    table_name: Teradata tablename which we'd like to import from
    log_table: restart log table name for the FastExport checkpoint information
    columns (optional): list of columns to import
    where_clause (optional): SQL expression to filter rows
    
    Examples:
    ---------
    >>>td_export_tab(fexp_path=r'C:\Program Files (x86)\Teradata\Client\16.10\bin\fexp.exe',
              outmod_dll='outmod_vchar.dll',
              out_file='PRD2_DIC_V.PRICE_PLAN.csv',
              driver='Teradata Database ODBC Driver 16.10',
              host='*****',
              uid='*****', 
              pwd='*****', 
              table_name='PRD2_DIC_V.PRICE_PLAN', 
              log_table='UAT_DM.SP_FE_LOG')

    >>>td_export_tab(fexp_path=r'C:\Program Files (x86)\Teradata\Client\16.10\bin\fexp.exe',
              outmod_dll='outmod_vchar.dll',
              out_file='PRD2_MDS_V.DMSC.csv',
              driver='Teradata Database ODBC Driver 16.10',
              host='*****',
              uid='*****', 
              pwd='*****', 
              table_name='PRD2_MDS_V.DMSC', 
              log_table='UAT_DM.SP_FE_LOG',
              columns=['MSISDN', 'lifetime', 'bc_lifetime', 'dev_park_size', 'av_dev_use'],
              where_clause="REPORT_DATE = DATE'2019-06-01' AND FLASH_CODE IN ('New', 'Active', 'Reconnect')")
    """
    if (where_clause != ''):
        where_clause = 'WHERE ' + where_clause
    outmod_dll = 'OUTMOD {}'.format(outmod_dll) if outmod_dll else ''
        
    type_dict = {'CF': 'CHAR', 'CV': 'VARCHAR', 'F': 'FLOAT', 'D': 'DECIMAL', 
                 'TS': 'TIMESTAMP', 'I': 'INTEGER', 'I8': 'BIGINT', 'I1': 'SMALLINT', 'DA': 'DATE', 
                 'YR': 'INTERVAL YEAR', 'MO': 'INTERVAL MONTH', 'I1': 'BYTEINT',
                 'DY': 'INTERVAL DAY'}
        
    columns_dt = help_columns(driver, host, uid, pwd, table_name)
    if columns:
        columns_dt = columns_dt[columns_dt['Column Name'].str.strip().isin(columns)]
    
    all_columns_description = ''
    column_names = ''
    for i in range(columns_dt.shape[0]):
        column_name = str(columns_dt.iloc[i]['Column Name']).strip()
        column_names += "'{}'".format(column_name)
        column_type = type_dict[str(columns_dt.iloc[i]['Type']).strip()]
        column_length = int(columns_dt.iloc[i]['Max Length'])
    
        column_description = ''
        if column_type == 'CHAR':
            column_description = 'CAST({} AS VARCHAR({}))'.format(column_name, int(column_length))
        elif column_type == 'VARCHAR':
            column_description = 'CAST({} AS VARCHAR({}))'.format(column_name, int(column_length / 2))
        elif column_type == 'FLOAT':
            column_description = "CAST({} AS VARCHAR(20))".format(column_name)
        elif column_type == 'DECIMAL':
            column_description = "CAST({} AS VARCHAR(20))".format(column_name)
        elif column_type == 'TIMESTAMP':
            column_description = 'CAST({} AS VARCHAR(20))'.format(column_name)
        elif column_type == 'INTEGER':
            column_description = 'CAST({} AS VARCHAR(11))'.format(column_name)
        elif column_type == 'BIGINT':
            column_description = 'CAST({} AS VARCHAR(20))'.format(column_name)           
        elif column_type == 'SMALLINT':
            column_description = 'CAST({} AS VARCHAR(6))'.format(column_name)
        elif column_type == 'BYTEINT':
            column_description = 'CAST({} AS VARCHAR(4))'.format(column_name)
        elif column_type == 'DATE':
            column_description = 'CAST({} AS VARCHAR(20))'.format(column_name)
        elif column_type == 'INTERVAL YEAR':
            column_description = "CAST({} AS VARCHAR(5))".format(column_name)
        elif column_type == 'INTERVAL MONTH':
            column_description = "CAST({} AS VARCHAR(5))".format(column_name)
        elif column_type == 'INTERVAL DAY':
            column_description = "CAST({} AS VARCHAR(5))".format(column_name)            
        else:
            column_description = ''
        all_columns_description += column_description
        if i < columns_dt.shape[0] - 1:
            all_columns_description += ", \n"
            column_names +=  " || '|' || "
        
    column_names_query = 'SELECT * FROM (SELECT CAST({} AS VARCHAR({}))  as x) dt;'.format(column_names, len(column_names))
    
    query = """.LOGTABLE {log_table};
        .logon {host}/{uid}, {pwd};
        .BEGIN EXPORT SESSIONS 2;
        .EXPORT OUTFILE {file} 
            {outmod_dll}
            MODE RECORD FORMAT TEXT;
        {column_names_query}
        SELECT {columns}
        FROM {tab} {where_clause};
        .END EXPORT;
        .LOGOFF;""".format(log_table=log_table,
                   host=host, uid=uid, pwd=pwd,
                   column_names_query=column_names_query,
                   file=out_file,
                   outmod_dll=outmod_dll,
                   columns=all_columns_description,
                   tab=table_name, where_clause=where_clause)
    print(query)
    
    process = subprocess.Popen([fexp_path, "-c", "UTF8"], 
                               stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    process.stdin.write(query.encode('utf-8'))
    print(process.communicate()[0])
    process.stdin.close()


def help_columns(driver, host, uid, pwd, table_name):
    try:
        connection = connect(driver=driver,
                      DBCName=host, 
                      uid=uid,
                      pwd=pwd)
        try:
            query = str.format("""HELP COLUMN {}.*""", table_name)
            cursor = connection.cursor()
            cursor.execute(query)
            dt_columns = pd.DataFrame(cursor.fetchallnumpy())
            return dt_columns
        except DatabaseError as ex:
            print(ex)
            return None
        finally:
            connection.close()
    except DatabaseError as ex:
        print('Error establishing a database connection.')
        return None