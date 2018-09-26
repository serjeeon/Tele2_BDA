"""Load a pandas dataframe into Teradata using Turbodbc
"""

import numpy as np
import pandas as pd
from turbodbc import connect, make_options, DatabaseError

# TODO 
#1. Varchar length for every string column instead of max - this requires a change in logic
#2. Change dsn and table_name order and add default dsn
#3. Add underscore before every function except the main one

def td_load_df(df, dsn, table_name, index=None): #This is the main function
    """
    Load pandas dataframe to teradata (using turbodbc)
    This function's intended use is to quickly load small to relatively large dataframes 
    into the database without thinking about it
    
    The function is NOT recommended for use with very large dataframes (over 10 million cells)
    
    Parameters
    ----------
    df: a pandas dataframe, must only used allowed dtypes
    dsn: datasource name, should be == 'Teradata' unless it has a different name on your PC
    table_name: Teradata tablename (where to load)
    index (optional): string column name of the index column, if None attempts to guess
    
    Examples:
    ---------
    >>>test_1 = pd.DataFrame({'ints_1':[0, 1, 2, 3]
                          ,'ints_2':[0, 3, 4, 5]
                          ,'ints_3':[-1, -2, -3, -4]})
    >>>td_load_df(test_1, dsn='Teradata', table_name='ar_test_turbodbc_1')
    
    Current limitations:
    --------------------
    0. Largely untested
    1. String column length is equal to the length of the largest string column
    2. Datetime columns are loaded as several integer columns (year, month, day)
	3. Does not support NA values at this time (throws an error)
    """
    if df.isnull().values.any():
        raise ValueError("""This function currently does not support NA values,
    please fill them before loading""")
    df.columns = [col.upper() for col in df.columns]
    cat_columns = df.select_dtypes(['category']).columns
    for col in cat_columns:
        df[col] = df[col].astype(str)
    df_datetime_to_text(df)
    table_name_clean = 'UAT_DM.' + table_name.upper().replace('UAT_DM.', '')
    options = make_options(autocommit=True)
    connection = connect(dsn=dsn,turbodbc_options=options)
    cursor = connection.cursor()
    try:
        cursor.execute('DROP TABLE '+table_name_clean)
    except DatabaseError:
        pass
    cursor.execute(sql_create_statement(df, table_name_clean, guess_index(df, index)))
    cursor.executemanycolumns(sql_insert_statement(df, table_name_clean), 
                         [df[col].values for col in df.columns])
    connection.close()
    print("Loaded your dataframe successfully")

def dtypes_pd_to_td(df): #maps pandas dtypes to what 
    longest_str = get_longest_string(df)
    if longest_str > int(3e3): # This should maybe be a parameter? 
        raise Exception(
            """
            One of the string columns has a value that is over the allowed size limit
            """
        )
    type_dict = {
    'int16':'SMALLINT', 'int32':'INTEGER', 'int64':'BIGINT'
    ,'float16':'FLOAT', 'float32':'FLOAT', 'float64':'FLOAT'
    ,'object':'VARCHAR('+str(longest_str) + ')'
            }
    dtypes = df.dtypes.astype(str)
    if not set(dtypes).issubset(set(type_dict)):
        raise Exception("""
        One of the columns is of an illegal dtype
        , please use one the following types:""" + " ".join(type_dict))
    return zip(df.columns, [type_dict[dtype] for dtype in dtypes])

def get_longest_string(df):
    col_long_str = [df[col].str.len().max() for col in df.select_dtypes(
        include=['object', 'category'])]
    return int(max(col_long_str+[0])) #returns 0 if no str columns, doesn't matter

def sql_create_statement(df, table_name, index): #SQL Create Table Statement
    if index is None or index not in df.columns:
        index = df.columns[0]
    td_types = dtypes_pd_to_td(df)
    sql_cols = ",".join(['{} {}'.format(k,v) for k,v in dtypes_pd_to_td(df)])
    return """
    CREATE MULTISET TABLE {} 
    ,NO FALLBACK 
     ,NO BEFORE JOURNAL
     ,NO AFTER JOURNAL
     ,CHECKSUM = DEFAULT (""".format(table_name) + sql_cols + \
    ") PRIMARY INDEX ({});".format(index)
    
def sql_insert_statement(df, table_name): 
    return "INSERT INTO " + table_name + " VALUES (" + \
            ','.join(["?" for col in df.columns]) + ")"

def df_datetime_to_text(df): #Datetime columns to several integer columns
    date_cols = df.select_dtypes(include = np.datetime64).columns
    for col in date_cols:
        df[col+'_year'] = df[col].dt.year
        df[col+'_month'] = df[col].dt.month
        df[col+'_day'] = df[col].dt.day
        df.drop(col, axis=1, inplace=True)

def guess_index(df, index=None): # tries to guess index
    index_res = df.columns[0]
    if index is None and 'MSISDN' in df.columns:
        index_res = 'MSISDN'
    if index is None and 'SUBS_ID' in df.columns:
        index_res = 'SUBS_ID'
    if index in df.columns:
        index_res=index
    return index_res        
	
def test(): #Test everything
    test_1 = pd.DataFrame({'ints_1':[0, 1, 2, 3]
                          ,'ints_2':[0, 3, 4, 5]
                          ,'ints_3':[-1, -2, -3, -4]})

    test_2 = pd.DataFrame({'str_1':['aa', 'bb', 'cc']
                          ,'float_1':[0., 0.1, 0.2]
                          ,'int_1':[0, 1, 2]})
    test_3 = pd.DataFrame({'ints_1':[0, 1, 2, 3]
                          ,'dt_1':[pd.Timestamp('20130101'),
                                  pd.Timestamp('20130102'),
                                  pd.Timestamp('20130103'),
                                  pd.Timestamp('20130104')]})
    test_4 = pd.DataFrame({'ints_1':[0, 1, 2, 3]
                          ,'MSISDN':['79168007000', '79168007001'
                                    ,'79168007002', '79168007003']})
    test_5 = pd.DataFrame({'ints_1':[0, 1, 2, 3]
                          ,'SUBS_ID':['200000900800', '200000900801'
                                    ,'200000900802', '200000900803']})
    test_6 = pd.DataFrame({'ints_1':[0, 1, 2, 3]
                          ,'MSISDN':['79168007000', '79168007001'
                                    ,'79168007002', '79168007003']
                          ,'SUBS_ID':['200000900800', '200000900801'
                                    ,'200000900802', '200000900803']})
    assert(guess_index(test_1, 'ints_2')=='ints_2')
    assert(guess_index(test_2, 'ints_2')=='float_1')
    assert(guess_index(test_2)=='float_1')
    assert(guess_index(test_4)=='MSISDN')
    assert(guess_index(test_5)=='SUBS_ID')
    assert(guess_index(test_6)=='SUBS_ID')
    assert(guess_index(test_6, 'MSISDN')=='MSISDN')
    
    td_load_df(test_1, dsn='Teradata', table_name='ar_test_turbodbc_1')
    td_load_df(test_2, dsn='Teradata', table_name='ar_test_turbodbc_2')
    td_load_df(test_3, dsn='Teradata', table_name='ar_test_turbodbc_3')
    connection = connect(dsn='Teradata')
    cursor = connection.cursor()
    sql = """
SEL * FROM UAT_DM.ar_test_turbodbc_1
   """
    cursor.execute(sql)
    load_1 = pd.DataFrame(cursor.fetchallnumpy())
    sql = """
SEL * FROM UAT_DM.ar_test_turbodbc_2
   """
    cursor.execute(sql)
    load_2 = pd.DataFrame(cursor.fetchallnumpy())
    sql = """
SEL * FROM UAT_DM.ar_test_turbodbc_3
   """
    cursor.execute(sql)
    load_3 = pd.DataFrame(cursor.fetchallnumpy())
    assert(load_3.shape==(4,4))
    pd.testing.assert_frame_equal(test_1.reset_index().drop(labels='index', axis=1)
                                 ,load_1.sort_values(
                                     by=load_1.columns[0]).reset_index().drop(
                                     labels='index', axis=1))
    pd.testing.assert_frame_equal(test_2.reset_index().drop(labels='index', axis=1)
                                 ,load_2.sort_values(
                                     by=load_2.columns[1]).reset_index().drop(
                                     labels='index', axis=1))
    print("Passed all tests successfully")
									 
def test_big(power = 6):
    test_frame = pd.DataFrame({'int_col':[7]*np.power(10, power)
                 ,'float_col':[1.3]*np.power(10, power)
                 ,'string_col':['test_string']*np.power(10, power)
                    ,'cat_col':pd.Categorical(["test","train"]*(np.power(10,power)//2))
                              })
    td_load_df(test_frame, dsn='Teradata', table_name='ar_test_turbodbc')
 
if __name__ == "__main__":
    test()
