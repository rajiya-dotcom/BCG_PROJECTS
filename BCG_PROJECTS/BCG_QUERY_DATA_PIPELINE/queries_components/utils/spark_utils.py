# Pyspark import
import pyspark
from pyspark.sql import SparkSession

def read_data(spark, path, format='csv',
              nrows=None,
              header=True,
              sep=',',
              **options):
    """
    Read data as Spark dataframe
    Arguments:
        spark: Spark Session
        path: Input file path
        format: Input file format
        nrows: Max number of rows expected in the Spark dataframe
        header: Boolean value representing the presence of header in the input
        options: Options supported by Spark dataframe reader
    """
    data = spark.read.format(format).load(path, header=header, sep=sep, InferSchema='true', **options)
    if nrows:
        data = data.limit(nrows)
    return data


def write_data(data, path, format='csv', mode='overwrite', header=True, sep='\t', **options):
    """
    Write spark dataframe to a specified location
    Arguments:
        data: Spark dataframe
        path: Destination path
        format:
        mode: Accepts values as that of spark dataframe writer
        header:
        options: Options supported by Spark dataframe writer
    """
    data.coalesce(1).write.save(path, format=format, mode=mode, header=header, sep=sep, **options)
    # data.write.save(path, format=format, mode=mode, header=header, sep=sep, **options)

def get_spark_session(app_name: str):
    """
    Arguments:
        app_name: App name to be set
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("DEBUG")
    return spark

