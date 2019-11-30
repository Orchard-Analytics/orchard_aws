import pandas as pd
import logging
import psycopg2
import json
import time
import uuid

from .s3 import s3
import sql_generator

log = logging.getLogger('Redshift Conn')


class Redshift(object):
    """
        This class is the main driver for interacting with a redshift instance.

        TODO:
            - Logger
            - Do we need to lock tables when we upsert? See generate_lock_query()
            - Write 'grant' permissions functions to run after upserts
            - Create util function to clean up old S3 files
            - Include manifest file?

        Parameters
        -------
        credentials: dict
            dictionary of redshift credentials:
                {
                    'dbname': value,
                    'host': value,
                    'port': value,
                    'user': value,
                    'password': value
                }
        s3_credentials: dict
            dictionary of s3 credentials:
                {
                    'access_key': value,
                    'secret_key': value,
                    'bucket': value
                }
    """

    def __init__(self, dbname, host, port, user, password, access_key=None, secret_key=None, s3_bucket=None):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.conn = None
        try:
            self.conn = self.connect()
        except (Exception, psycopg2.Error) as error:
            log.info('Could not connect to {}. Trying once more.'.format(self.dbname))
            time.sleep(5)
            self.conn = self.connect()

        if access_key is not None:
            self.s3_conn = s3.s3(access_key=access_key, secret_key=secret_key, s3_bucket=s3_bucket)

    def connect(self):
        """
            Returns the connection to redshift
        """
        self.conn = psycopg2.connect(dbname=self.dbname,
                                     host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password)
        return self.conn

    def close(self):
        log.debug('Closing redshift connection')
        self.conn.close()

    def execute_and_fetch(self, query, return_dataframe=False, return_json=False):
        """
            Returns results from a query in either json or a dataframe.
        """
        cur = None
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            resp = cur.fetchall()
            if cur is not None:
                cur.close()
            if return_dataframe == return_json:
                raise Exception('return_dataframe and return_json are mutually exclusive.')
            if return_dataframe is True:
                columns = [column[0] for column in cur.description]
                return pd.DataFrame(resp, columns=columns)
            elif return_json is True:
                columns = [column[0] for column in cur.description]
                return json.loads(pd.DataFrame(resp, columns=columns).to_json(orient='records'))
            else:
                return response
        except Exception as e:
            log.info('Encountered an error while executing. Closing connection')
            self.close()
            raise
        finally:
            if cur is not None:
                cur.close()

    def execute(self, query):
        """
            Executes SQL against redshift.
        """
        cur = None
        try:
            cur = self.conn.cursor()
            cur.execute(query)
            self.conn.commit()
        except Exception as e:
            log.info('Encountered an error while executing. Closing connection')
            self.close()
            raise
        finally:
            if cur is not None:
                cur.close()

    def df_to_redshift(self,
                       df,
                       schema_and_table,
                       sortkey='',
                       columns=None,
                       load_type='full-refresh',
                       diststyle='auto',
                       primary_keys=[],
                       bucket=None,
                       subdirectory='automated_loads',
                       encoding='utf-8',
                       keep_s3_backup=False):
        """
            Upserts a DataFrame to a redshift table. The high level design is as follows:
                1. Upload the DataFrame as csv to a directory in s3
                2. If load_type == 'full-refresh', drop the table in redshift
                3. If table doesn't exist, create an empty table deriving the table defintion from the DataFrame.
                4. Copy the file from s3 into a temporary staging table
                5. If load_type = 'incremental', delete rows from production table that exist in staging table using primary keys.
                6. Insert staging table into production table and drop the staging table.

            Parameters
            -------
            df: DataFrame

            schema_and_table: str
                name of the table in production, e.g. public.order_facts

            sortkey: str
                str of column names to use as sortkey(s). To use multiple sortkeys pass a str of commma seperated names.
                E.g. sortkey = 'id, created_at'
                Default: ''

            load_type: str
                either 'full-refresh' or 'incremental'. If 'incremental', primary_keys parameter is required.

            primary_keys: list of strings
                list of column names to perform incremental load with.

            bucket: str
                name of s3 bucket to store files to
                Default: s3 bucket defined in connect.s3.Conn()

            subdirectory: str
                name of s3 subdirectory to store and copy files from
                Default: 'automated_loads'

            keep_s3_backup: bool
                By default we delete the s3 file after completing the upsert. When True, we keep the file in s3.
                Default: False

        """
        if load_type == 'incremental' and primary_keys == []:
            raise Exception('Must pass primary keys for incremental loads.')
        table = schema_and_table.split('.')[1]
        csv_name = '{}-{}.csv'.format(table, uuid.uuid4())
        log.info('Uploaded {} to s3 directory {}'.format(csv_name, subdirectory))
        key = self.s3_conn.df_to_s3(df, csv_name, bucket=bucket, subdirectory=subdirectory, encoding=encoding)
        df = None

        # populate the table from the data in s3
        self.s3_to_redshift(key=key,
                            schema_and_table=schema_and_table,
                            sortkey=sortkey,
                            columns=columns,
                            primary_keys=primary_keys,
                            encoding=encoding)
        # delete the file
        if not keep_s3_backup:
            self.s3_conn.delete_file(key)

    def s3_to_redshift(self,
                       key,
                       schema_and_table,
                       sortkey,
                       columns,
                       load_type='full-refresh',
                       primary_keys=[],
                       bucket=None,
                       diststyle='auto',
                       encoding='utf-8'):

        if load_type == 'full-refresh':
            drop_table_query = sql_generator.get_drop_table_query(schema_and_table)
            self.execute(drop_table_query)

        df = self.s3_conn.s3_to_df(bucket=bucket, key=key)
        if not self.check_table_exists(schema_and_table=schema_and_table):
            self.create_table_from_df(schema_and_table=schema_and_table,
                                      df=df,
                                      diststyle=diststyle,
                                      sortkey=sortkey)
        log.info('Performing Upsert')
        self.upsert_from_s3(schema_and_table, key, bucket, primary_keys)

    def upsert_from_s3(self, schema_and_table, key, bucket, primary_keys):
        """
            Create a temporary staging table in redshift. Copy data from s3 file into
            temporary staging table. Upsert (insert/update) rows from temporary table into
            destination table. Delete temporary table.
        """
        temp_table = self.create_temp_staging_table(schema_and_table)

        # Copy s3 file into temp table
        temp_s3_path = "{}/{}".format(self.s3_conn.bucket, key)
        temp_copy_parameters = ["CSV DELIMITER AS ',' NULL AS 'NaN' BLANKSASNULL", "COMPUPDATE OFF"]
        log.info('Copying {} into {}'.format(temp_s3_path, temp_table))
        self.copy_from_s3(schema_and_table=temp_table, s3_path=temp_s3_path, extra_params=temp_copy_parameters)

        # upsert temp table into prod table
        self.upsert(source=temp_table, dest=schema_and_table, primary_keys=primary_keys)
        log.info('Dropping temp table {}'.format(temp_table))
        drop_temp_table_query = sql_generator.get_drop_table_query(temp_table)
        self.execute(drop_temp_table_query)

    def upsert(self, source, dest, primary_keys):
        """
            Manually does a delete, insert:
                1. If primary keys are passed, delete and insert rows in destination where the primary
                key exists in source
                2. Insert source into destination
        """
        if primary_keys:
            delete_dest_rows_query = sql_generator.get_delete_from_dest_using_source_query(
                source, dest, primary_keys)
            log.info('Deleting records from {} using: {}'.format(dest, primary_keys))
            self.execute(delete_dest_rows_query)

        log.info('Inserting records from {}'.format(source))
        insert_rows_from_source_query = sql_generator.get_insert_from_source_into_dest_query(source, dest)
        self.execute(insert_rows_from_source_query)

    def copy_from_s3(self, schema_and_table, s3_path, extra_params, columns=''):
        """
            Executes a copy statement to load s3 into a redshift table. Note, this copies all columns by default.
        """
        copy_query = sql_generator.get_copy_from_s3_query(schema_and_table=schema_and_table,
                                                          columns=columns,
                                                          s3_path=s3_path,
                                                          extra_params=extra_params)
        log.debug('Copy query:\n{}'.format(copy_query))
        copy_query_with_creds = copy_query.format(access_key=self.s3_conn.access_key,
                                                  secret_key=self.s3_conn.secret_key)
        self.execute(copy_query_with_creds)

    def create_temp_staging_table(self, schema_and_table):
        """
            Creates a temporary staging with the same schema as a destination table.

            Parameters
            -------
            schema_and_table: str
                name of table to copy schema from
        """
        table = schema_and_table.split('.')[1]
        temp_table = "{}__tmp".format(table)

        # ensure temp does not exist
        drop_temp_table_query = sql_generator.get_drop_table_query(temp_table)
        self.execute(drop_temp_table_query)

        # create temp staging table
        temp_table_query = sql_generator.get_create_temp_staging_table_query(temp_table, schema_and_table)
        log.info('Creating temp staging table {}'.format(temp_table))
        self.execute(temp_table_query)
        return temp_table

    def check_table_exists(self, schema_and_table=None, schema=None, table=None):
        """
            Returns
            -------
            bool: table exists in redshift
        """
        if schema_and_table is not None:
            schema, table = schema_and_table.split('.')
        query = sql_generator.get_table_exists_query(schema, table)
        resp = self.execute_and_fetch(query, return_json=True)
        return resp[0]['count'] > 0

    def create_table_from_df(self, schema_and_table, df, diststyle, sortkey, add_updated_column=False):
        """
            Creates a schema if the schema does not already exist and creates an empty table based on a df.
        """
        schema = schema_and_table.split('.')[0]
        create_schema_query = sql_generator.get_create_schema_query(schema)
        create_table_query = sql_generator.create_table_ddl_from_df(schema_and_table,
                                                                    df,
                                                                    add_updated_column=add_updated_column,
                                                                    diststyle=diststyle,
                                                                    sortkey=sortkey)
        log.info(f'Creating schema and table: {schema_and_table}')
        log.info(create_schema_query)
        self.execute(create_schema_query)
        log.info(create_table_query)
        self.execute(create_table_query)

    def generate_lock_query(self, table_list):
        existing_tables = [table for table in table_list if self.check_table_exists(table)]
        if existing_tables:
            return 'LOCK {};'.format(','.join(existing_tables))
        else:
            return None