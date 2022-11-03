import requests
import pandas as pd
import sqlalchemy as db
from typing import Optional, Dict, List


from airflow.models import Variable

def create_upsert_method(meta: db.MetaData, extra_update_fields: Optional[Dict[str, str]]):
    """
    Create upsert method that satisfied the pandas's to_sql API.
    """

    def method(table, conn, keys, data_iter):
        # select table that data is being inserted to (from pandas's context)
        sql_table = db.Table(table.name, meta, autoload=True)

        # list of dictionaries {col_name: value} of data to insert
        values_to_insert = [dict(zip(keys, data)) for data in data_iter]

        # create insert statement using postgresql dialect
        insert_stmt = db.dialects.postgresql.insert(sql_table, values_to_insert)

        # create update statement for excluded fields on conflict
        update_stmt = {exc_k.key: exc_k for exc_k in insert_stmt.excluded if exc_k.key != 'created_at'}
        if extra_update_fields:
            update_stmt.update(extra_update_fields)

        # create upsert statement
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=sql_table.primary_key.columns,  # index elements are primary keys of a table
            set_=update_stmt  # the SET part of an INSERT statement
        )

        # execute upsert statement
        conn.execute(upsert_stmt)

    return method


class Upsert:
    """
    Operator init

    :param *args: {dict} Airflow defaults
    :param *kwargs: {dict}  Airflow defaults
    """

    def __init__(self, *args, **kwargs):

        self.db_string = Variable.get('DB_STRING')

        # create db engine
        self.db_engine = db.create_engine(self.db_string)

    def data_transform_and_load(
            self,
            df_to_load: pd.DataFrame,
            table_name: str,
            list_of_col_names: List,
            schema_name: str = "public",
            rename_mapper: Optional[Dict[str, str]] = None,
            extra_update_fields: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Prep given df_to_load and load it to table_name
        :param df_to_load{Dataframe}: Dataframe to Load
        :param table_name{str}: Name of table
        :param list_of_col_names List{str}: List of column names to load
        :param schema_name{str}: Optional, Table schema name
        :param rename_mapper{Dict[str,str]}: Optional, Columns that need to be renamed
        :param extra_update_fields{Dict[str,str]}: Optional, Metadata load timestamp update field name

        :returns: None

        #Import
        from plugins.upsert import Upsert
        pg_upsert = Upsert() #instantiating SendEmail

        #Invocation
        pg_upsert.data_transform_and_load(params)

        """

        # check if DataFrame contains any data, if it doesn't - skip the rest
        if df_to_load.empty:
            return False

        # change json column names to match table column names
        if rename_mapper:
            df_to_load = df_to_load.rename(columns=rename_mapper, inplace=False)

        # include only necessary columns
        df_to_load = df_to_load.filter(list_of_col_names)

        # create DB metadata object that can access table names, primary keys, etc.
        meta = db.MetaData(self.db_engine, schema=schema_name)

        # create upsert method that is accepted by pandas API
        upsert_method = create_upsert_method(meta, extra_update_fields)

        # perform upsert of DataFrame values to the given table
        df_to_load.to_sql(
            name=table_name,
            con=self.db_engine,
            schema=schema_name,
            index=False,
            if_exists='append',
            chunksize=200,  # it's recommended to insert data in chunks
            method=upsert_method
        )

        # if it got that far without any errors - notify a successful completion
        print(f"{schema_name}.{table_name} Upsert Completed Successfully!")
