import pandas as pd

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.getActiveSession()

#INTERFACE AND IMPLEMENTATIONS FOR READING FILES
class InterfaceTableReader(ABC):

    @abstractmethod
    def read_table(self) -> DataFrame:
        pass

class AbstractFileReader(InterfaceTableReader):

    def __init__(self, file_path: str):
        self.file_path = file_path

    @abstractmethod
    def read_table(self) -> DataFrame:
        pass

class CsvFileReader(AbstractFileReader):

    def __init__(self, file_path: str, separator: str = ','):
        super().__init__(file_path)
        self.separator = separator

    def read_table(self) -> DataFrame:
        return spark.read.format('csv').option('header', 'true').option('sep', self.separator).load(self.file_path)