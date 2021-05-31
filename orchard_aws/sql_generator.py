import logging
import pandas as pd
from .string_utils import *
import yaml
from . import config

log = logging.getLogger('SQL Generator')


def get_table_exists_query(schema, table):
    query = """
        select
            count(*)
        from INFORMATION_SCHEMA.TABLES
        where table_schema = '{schema}' and table_name = '{table}';
    """.format(schema=schema, table=table)
    return query


def get_copy_from_s3_query(schema_and_table,
                           columns,
                           s3_path,
                           extra_params=[]):
    if not extra_params:
        extra_params = ''
    else:
        extra_params = list_to_string(string_list=extra_params, delimiter=' ')
    query = """
        copy {schema_and_table}{columns}
        from 's3://{s3_path}'
        ignoreheader 1
        credentials 'aws_access_key_id={{access_key}};aws_secret_access_key={{secret_key}}'
        {extra_params}
        """.format(schema_and_table=schema_and_table,
                   columns=columns,
                   s3_path=s3_path,
                   extra_params=extra_params)
    return query


def get_delete_from_dest_using_source_query(source, dest, primary_keys):
    """
        Returns the delete query used for incremental load pipelines.

        Parameters
        -------
            source: str
            source table name

            dest: str
            destination table name, i.e. table to execute the delete statement on

            primary_keys: [List of strings]
            List of column names that create a primary key for each row

        Example
        -------
        get_delete_from_dest_using_source_query('orders', 'orders__temp', ['id'] )
        >> 'delete from orders using order__temp where 1=1 and orders.id = order__temp.id'
    """
    where_clause = 'where 1=1 '
    for key in primary_keys:
        where_clause += 'and {source}."{key}" = {dest}."{key}" '.format(
            source=source, dest=dest, key=key)
    query = '''delete from {dest} using {source}
        {where_clause}'''.format(dest=dest,
                                 source=source,
                                 where_clause=where_clause)
    log.debug('delete from dest using source query:\n{}'.format(query))
    return query


def get_insert_from_source_into_dest_query(source, dest):
    query = '''insert into {dest}
        select * from {source}'''.format(dest=dest, source=source)
    log.debug('Insert from source into dest query:\n{}'.format(query))
    return query


def get_create_schema_query(schema):
    return 'CREATE SCHEMA IF NOT EXISTS {}'.format(schema)


def get_drop_table_query(schema_and_table):
    query = 'DROP TABLE IF EXISTS {}'.format(schema_and_table)
    log.debug('drop table query: {}'.format(query))
    return query


def get_create_temp_staging_table_query(temp_table, schema_and_table):
    query = 'CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS)'.format(
        temp_table, schema_and_table)
    log.debug('create temp table query:\n{}'.format(query))
    return query


def get_pandas_datatype_frame(df):
    """
        Returns a dataframe with a row for each column in the original datagrame:

        Returns
        ----------

            index |  pandas_type
            -----   ----
            col1  | col1 datatype
    """
    return df.dtypes.to_frame(name='pandas_type').reset_index()


def add_redshift_type_column(df, type_map):
    """Creates a column, that is the redhsift version of `pandas_type`"""
    df['redshift_type'] = df['pandas_type'].astype(str).map(type_map)
    return df


def add_column_ddl(df):
    """Creates column with the definition for each column in table"""
    df['column_ddl'] = '"' + df['index'] + '" ' + df['redshift_type'] + ","
    return df


def get_ddl_string(df):
    """
        Joins each column ddl into one ddl string.

        Returns
        -------
        '"col1" varchar(500), "col2" timestamp, "col3" bigint'
    """
    return ' '.join(df.column_ddl)[:-1]


def get_table_config_string(diststyle, sortkey):
    """
        Returns a string to be appended to the ddl statment describing the
        distribution style and sortkeys
    """
    config = ''
    if diststyle is not None:
        config = config + 'diststyle {} '.format(diststyle)
    if sortkey is not None:
        config = config + 'sortkey({})'.format(sortkey)
    return config


def get_ddl_base_string(add_updated_column):
    if add_updated_column:
        base = 'CREATE TABLE IF NOT EXISTS %s (%s, "__updated_at" TIMESTAMP DEFAULT SYSDATE) %s'
    else:
        base = 'CREATE TABLE IF NOT EXISTS %s (%s) %s'
    return base


def create_table_ddl_from_df(schema_and_table,
                             df,
                             add_updated_column=False,
                             diststyle='auto',
                             sortkey=None):
    """
        Tales a schema.table and a dataframe and returns a ddl (create table) statement.

        Parameters
        ----------
        schema_and_table : str

        df : pd.DataFrame

        add_updated_column: bool (optional)
            When true, add an __updated_at column to the create table statement
    """
    type_map = config.get('pandas_redshift_datatypes')
    df = get_pandas_datatype_frame(df)
    df = add_redshift_type_column(df, type_map)
    df = add_column_ddl(df)
    ddl_string = get_ddl_string(df)
    base_string = get_ddl_base_string(add_updated_column)
    table_config_string = get_table_config_string(diststyle, sortkey)
    create_table_ddl = base_string % (schema_and_table, ddl_string,
                                      table_config_string)
    log.info('create table ddl: {}'.format(create_table_ddl))
    return create_table_ddl
