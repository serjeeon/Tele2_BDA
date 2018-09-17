import os

def ready_write(host='', login='', password='', cols=[], file_name='', table_name='', checkpoint = '100000', fastload_file_name='', separator='\\t',
                skip_header=False):
    """
    Prepares and writes fastloader import file.
    
    host, login, password - logon into Teradata;
    cols - list of columns;
    file_name - data from this file will be loaded into Teradata;
    table_name - table name into which data will be imported;
    checkpoint - checkpoint value in Teradata;
    fastload_file_name - name of the resulting file;
    
    """
    print('Beginning...')
    for_writing = ["SET SESSION CHARSET 'UTF8';"]
    for_writing.append(f'.logon {host}/{login},{password};')
    for i in ['',
     'SESSIONS 16;',
     '',
     "SET QUERY_BAND = 'UtilityDataSize=SMALL;' UPDATE for session; ",
     '',
     f'.SET RECORD VARTEXT "{separator}";',
     '',
     '' if skip_header == False else 'RECORD 2;',
     'DEFINE',
     '']:
        for_writing.append(i)
    
    for col in cols:
        for_writing.append(f'{col} (VARCHAR(255)),')
        
    for_writing.append('')
    
    for_writing.append(f'FILE {os.getcwd()}\\{file_name};')
    for_writing.append('')
    

    for_writing.append(f'BEGIN LOADING {table_name} ERRORFILES {table_name}_e1 , {table_name}_e2')
    for_writing.append('')
    
    for_writing.append(f'CHECKPOINT {checkpoint};')
    for_writing.append('')
    for_writing.append(f'INSERT INTO {table_name}')
    for_writing.append('VALUES')
    for_writing.append('(')
    
    for col in cols:
        if col != cols[-1]:
            for_writing.append(f':{col},')
        else:
            for_writing.append(f':{col}')
            
    for i in [');',
     '',
     '',
     'END LOADING;',
     '.LOGOFF;',
     '.QUIT;']:
        for_writing.append(i)

    with open(fastload_file_name, 'w') as f:
        f.write('\n'.join(for_writing))
    print('Done writing import file')
    
    bat_text = f"""cd Program Files (x86)\Teradata\Client\\16.10\\bin
fastload<{os.getcwd()}\\{fastload_file_name} -i UTF8
pause"""
    with open(fastload_file_name.replace('txt', 'bat'), 'w') as f:
        f.write(bat_text)
    print('Done writing bat')
    
def run_bat_file(fastload_file_name):
    os.startfile(fastload_file_name.replace('txt', 'bat'))