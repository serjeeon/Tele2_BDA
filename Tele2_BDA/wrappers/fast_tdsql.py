import numpy as np
import pandas as pd
import teradatasql as td

def set_connection(u, pas, h='td2800.corp.tele2.ru'):
    """
    Connect to a Teradata database server

    Parameters:
    -----------
    u - Teradata user;
    pas - Teradata password;
    h - Teradata host;
    """
    
    return td.connect(None, host=h, user=u, password=pas)

def select(con, q, params=None, infer_types=True, col_case='upper', shape=True, dtypes=False, head=0):
    """
    Make a select query

    Parameters:
    -----------
    con - connection to the database;
    q - query string;
    params - optional sequence of ? parameter values;
    infer_types - infer types by the database driver or set all columns as string;
    col_case - 'upper'/'lower'/None;
    shape - print shape of the obtained dataframe;
    dtypes - print dtypes of the obtained dataframe;
    head - print top n rows from the obtained dataframe;
    """

    if not con:
        print('Connection is not defined!')
        return None
    if not q:
        print('Query is not defined!')
        return None
    with con.cursor() as cur:
        cur.execute(q, params)
        rows = cur.fetchall()
        columns = np.array(cur.description)[:, 0]
        types = np.array(cur.description)[:, 1]
        # если запрос вернул 0 резлуьтатов, нужно поставить None, а не []
        df = pd.DataFrame(np.array(rows) if rows else None, columns=columns, dtype='str')
        # поменять типы колонок на те, которые вывела teradatasql
        if infer_types:
            df = df.astype(dict(zip(columns, types)))
        # перевести названия колонок в верхний/нижний регистр
        if col_case == 'upper':
            df.columns = df.columns.str.upper()
        elif col_case == 'lower':
            df.columns = df.columns.str.lower()
        # вывод дополнительной информации по результатам
        if shape:
            print(df.shape)
        if dtypes:
            if shape:
                print('-----------')
            print(df.dtypes)
        if head and head > 0:
            if shape or dtypes:
                print('-----------')
            print(df.head(head))
        return df