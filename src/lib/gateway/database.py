import pandas as pd

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable

spark = SparkSession.getActiveSession()

class InterfaceDatabaseGateway(ABC):

    @abstractmethod
    def execute_query(self, sql: str) -> list:
        pass
    
    @abstractmethod
    def get_surrogate_id(self, catalog_name: str, schema_name, table_name: str, 
                         base_values: dict, surrogate_column: str) -> int:
        pass

    @abstractmethod
    def get_max_value(self, catalog_name: str, schema_name: str, table_name: str, column: str) -> int:
        pass

    @abstractmethod
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, surrogate_column: str):
        pass

    @abstractmethod
    def get_table_metadata(self, catalog_name: str, schema_name: str, table_name: str) -> list:
        pass

    @abstractmethod
    def get_table_detail_metadata(self, table_id: int) -> list:
        pass

class AbstractSQLDatabaseGateway(InterfaceDatabaseGateway):

    def parse_base_values(self, base_values) -> dict:

        parsed_base_values = {}
        for key, value in base_values.items():
            
            if pd.isnull(value):
                raise Exception('Base value cannot be null')

            str_value = str(value).strip()
            if str_value == '':
                raise Exception('Base value cannot be empty')

            parsed_base_values[key] = str_value
        
        return parsed_base_values
        
    def generate_conditions_string(self, parsed_base_values):

        conditions_string = []

        for key, value in parsed_base_values.items():
            conditions_string.append(f"CAST({key} AS STRING)='{value}'")
        
        conditions_string = ' AND '.join(conditions_string)
        return conditions_string
    
    def generate_surrogate_fetch_query(self, catalog_name, schema_name, table_name, conditions_string, surrogate_column):

        sql = f"""SELECT {surrogate_column} AS identifier 
                  FROM {catalog_name}.{schema_name}.{table_name} 
                  WHERE {conditions_string}"""

        return sql

    @abstractmethod
    def execute_query(self, sql: str) -> list:
        pass
    
    def get_surrogate_id(self, catalog_name: str, schema_name: str, table_name: str, 
                         base_values: dict, surrogate_column: str) -> int:
        
        parsed_base_values = self.parse_base_values(base_values)
        conditions_string = self.generate_conditions_string(parsed_base_values)
        
        sql = self.generate_surrogate_fetch_query(catalog_name, schema_name, table_name, 
                                                  conditions_string, surrogate_column)
        
        result = self.execute_query(sql)

        if len(result) > 1:
            raise Exception('Unicity violation on surrogate key search')

        elif len(result) == 1:
            return result[0]['identifier']

        else:
            return None

    def get_max_value(self, catalog_name: str, schema_name: str, table_name: str, column: str): 

        sql = f"""SELECT MAX({column}) AS max_value  
                  FROM {catalog_name}.{schema_name}.{table_name}""" 

        result = self.execute_query(sql)
        return result[0]['max_value']
    
    @abstractmethod
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, surrogate_column: str):
        pass

    def get_table_metadata(self, catalog_name: str, schema_name: str, table_name: str):
        
        sql = f"""SELECT t.*, s.schema_name
                  FROM governance_prod.metadata.tables t
                  INNER JOIN governance_prod.metadata.schemas s ON t.schema_id = s.schema_id
                  INNER JOIN governance_prod.metadata.catalogs c ON s.catalog_id = c.catalog_id
                  WHERE table_name = '{table_name}' AND current_flag = True
                      AND s.schema_name = '{schema_name}'
                      AND c.catalog_name = '{catalog_name}' LIMIT 1"""

        result = self.execute_query(sql)
        return result
    
    def get_table_detail_metadata(self, table_id: int):

        sql = f'SELECT * FROM governance_prod.metadata.tables_detail WHERE table_id = {table_id}'

        result = self.execute_query(sql)
        return result
    
class SparkSQLDatabaseGateway(AbstractSQLDatabaseGateway):

    def __init__(self):
        pass

    def execute_query(self, sql: str):
        return spark.sql(sql).collect()
    
    def merge_dataframe(self, dataframe: DataFrame, catalog_name: str, schema_name: str, 
                        table_name: str, surrogate_column: str):
        
        delta_table = DeltaTable.forName(spark, f'{catalog_name}.{schema_name}.{table_name}')
    
        delta_table.alias('target').merge(dataframe.alias('source'),
            f'target.{surrogate_column}=source.{surrogate_column}'
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
          
