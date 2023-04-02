# Pyspark import
import pyspark
import pyspark.sql.functions as F

# Python import
from dataclasses import dataclass
from typing import Any

# Local import
from .utils import log_utils, spark_utils

logger = log_utils.get_logger(app_name=__name__)


class BCGQueryAnalysis:
    components = {}

    @classmethod
    def register_component(cls, component_name):
        def decorator(subclass):
            cls.components[component_name] = subclass
            return subclass

        return decorator

    @classmethod
    def create(cls, component_name, params):
        if component_name not in cls.components:
            raise ValueError('Bad component type {}'.format(component_name))
        return cls.components[component_name](**params)

@dataclass
class DataPipeline(BCGQueryAnalysis):
    config: Any
    component: str
    spark: Any = None

    def __post_init__(self):
        """
        Define global variable
        """
        self.spark = spark_utils.get_spark_session(app_name=self.component)
    
    def read_data(self, input_path, file_read_kwargs=None):
        """
        Read data from the input file path mentioned
        """
        spark = self.spark
        file_read_kwargs = file_read_kwargs or {} 
        df = spark_utils.read_data(spark, input_path, **file_read_kwargs)
        if df is not None:
            logger.info(f'shape : \n {df.count()}\n'
                        f'sample: \n {df.show(3)}')
        return df 
    