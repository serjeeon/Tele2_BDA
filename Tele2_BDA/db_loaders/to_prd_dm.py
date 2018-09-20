"""Load data into prd_dm."""

import turbodbc
import configparser

class ModelLoader(object):
    """
    Load model predictions into PRD_DM.

    This class was created to make loading data into PRD_DM more convenient.
    Suggested steps to use:
    - create class instance;
    - use create_buckets_table() method;
    - use insert_tables() method;

    Examples:
    ---------
    >>>>loader = ModelLoader()
    >>>>loader.create_buckets_table(model_version=1, probability_column='probability', segment_id=1, load_id=0, condition='WHERE 1=1', primary_index='subs_id', partition_string='')
    >>>>loader.insert_tables(model_name='Прикольные абоненты', segment_id=1, segment_name='Умные абоненты')
    """
    def __init__(self, path='', bucket_table_name='al_', source_table='al_', ones_pediction=False, create_with_data=False):
        """
        Init.
        
        Pass parameters and read settings from .ini file.

        Parameters
        ----------
        path - path to .ini file;
        bucket_table_name - name of table which will have buckets;
        source_table - table with predictions, which is Teradata;
        ones_pediction - if you don't predict something, but simply need to load a list of subs_id. Probabilities and bucket values will be equal to 1;
        create_with_data - create table with buckets with "WITH DATA", use only for little tables. NOT RECOMMENDED for big tables!!!

        """

        params = {'path': path,
                  'bucket_table_name': bucket_table_name,
                  'source_table': source_table,
                  'ones_pediction': ones_pediction,
                  'create_with_data': create_with_data}

        self._set_params(**params)
        self._get_max_model_id()

    def _set_params(self, **params):
        """Set parameters and options."""

        for key, value in params.items():
            setattr(self, key, value)

        self.config = configparser.ConfigParser()
        self.config.read(os.path.join(self.path, 'settings.ini'), encoding='utf-8')
        for key, value in self.config['TERADATA'].items():
            setattr(self, key, self.config.getint('TERADATA', key))

    def _get_max_model_id(self):
        """Get max model_id from table and increment."""

        connection_prd_dm = turbodbc.connect(dsn=self.dsn_prd_dm)
        cursor = connection_prd_dm.cursor()
        sql = 'select max(model_id) from PRD_DM.MODEL_DESC'
        cursor.execute(sql)
        model_id = cursor.fetchall()[0][0]
        self.model_id = model_id + 1
        cursor.close()
        connection_prd_dm.close()

    def _get_used_date(self):
       """Automatically get data which is loaded into tables."""

        connection = turbodbc.connect(dsn=self.dsn)
        cursor = connection.cursor()
        sql = f'select report_date from UAT_DM.{self.bucket_table_name}'
        cursor.execute(sql)
        self.report_date = cursor.fetchall()[0][0]
        cursor.close()
        connection.close()

    def create_buckets_table(self, model_version=1, probability_column='colname', segment_id=1, load_id=0, condition='WHERE', primary_index='subs_id',
                             partition_string=''):
        """Create bucket table.

        Parameters
        ----------
        model_version - 

        PARTITION BY RANGE_N(report_date  BETWEEN DATE '2018-09-10' AND DATE '2018-09-20' EACH INTERVAL '1' DAY )
        """
        self.model_version = model_version
        if self.ones_pediction:
            probability_column = "1"
            probability_string = "1 AS rnk"
            bucket_string = "1"
        else:
            probability_string = "ROW_NUMBER() OVER (ORDER BY {probability_column} DESC) AS rnk"
            bucket_string = "QUANTILE(99, rnk) + 1"

        if self.create_with_data:
            with_data_string = "WITH DATA"
        else:
            with_data_string = "WITH NO DATA"

        if partition_dates_period != None:
            date_partition

        sql = f"""
        CREATE MULTISET TABLE UAT_DM.{self.bucket_table_name}
        ,NO FALLBACK
        ,NO BEFORE JOURNAL
        ,NO AFTER JOURNAL
        as (
        SELECT report_date, subs_id, model_id, model_version, probability,
               {bucket_string} AS BUCKET_VALUE, segment_id, load_id
          FROM (
                SELECT CURRENT_DATE - 3 AS report_date,
                       subs_id,
                       {self.model_id} as model_id,
                       {self.model_version} as model_version,
                       {probability_column} AS probability,
                       {segment_id} as segment_id,
                       {load_id} as load_id, 
                       {probability_string}
                  FROM UAT_DM.{self.source_table}
                {condition}) t1
        ) {with_data_string}
        PRIMARY INDEX ({primary_index})
        {partition_string};
        """

        connection = turbodbc.connect(dsn=self.dsn)
        cursor = connection.cursor()
        cursor.execute(sql)
        cursor.close()
        connection.close()
        print('Bucket table created.')

        if self.create_with_data == False:
            sql = f"""
                    INSERT INTO UAT_DM.{self.bucket_table_name}
                    SELECT report_date, subs_id, model_id, model_version, probability,
                           {bucket_string} AS BUCKET_VALUE, segment_id, load_id
                      FROM (
                            SELECT CURRENT_DATE - 3 AS report_date,
                                   subs_id,
                                   {self.model_id} as model_id,
                                   {model_version} as model_version,
                                   {probability_column} AS probability,
                                   {segment_id} as segment_id,
                                   {load_id} as load_id, 
                                   {probability_string}
                              FROM UAT_DM.{self.source_table}
                            {condition}) t1
                    """
            cursor = connection.cursor()
            cursor.execute(sql)
            cursor.close()
            connection.close()
            print('Data inserted.')

        self._get_used_date()

    def insert_tables(self, model_name='', segment_id=1, segment_name=''):
        """Insert tables.

        """
        connection_prd_dm = turbodbc.connect(dsn=self.dsn_prd_dm)
        cursor = connection_prd_dm.cursor()
        sql="INSERT INTO PRD_DM.MODEL_DESC VALUES (?, ?)"
        cursor.executemanycolumns(sql, [np.array([model_id]), np.array([model_name])])
        connection_prd_dm.commit()
        cursor.close()
        print('Inserted into MODEL_DESC.')

        cursor = connection_prd_dm.cursor()
        sql="INSERT INTO PRD_DM.segment_desc VALUES (?, ?, ?)"
        cursor.executemanycolumns(sql, [np.array([model_id]), np.array([segment_id]),np.array([segment_name])])
        connection_prd_dm.commit()
        cursor.close()
        print('Inserted into segment_desc.')

        cursor = connection_prd_dm.cursor()
        sql=f"INSERT INTO prd_dm.scoring SELECT * FROM uat_dm.{self.bucket_table_name}"
        cursor.execute(sql)
        connection_prd_dm.commit()
        cursor.close()
        print('Inserted into scoring.')

        cursor = connection_prd_dm.cursor()
        sql="INSERT INTO PRD_DM.scoring_to_load VALUES (?, ?, ?)"
        cursor.executemanycolumns(sql, [np.array([self.report_date]), np.array([self.model_id]),np.array([self.model_version])])
        connection_prd_dm.commit()
        cursor.close()
        connection_prd.close()
        print('Inserted into scoring_to_load.')