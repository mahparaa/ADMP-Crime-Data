from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, substring, when
from pyspark.sql.types import IntegerType


def clean(df):
    df = df \
    .filter(col('offence_subgroup') != ' etc"') \
    .withColumn('year', substring('financial_year', 1, 4)) \
    .drop('financial_year') \
    .dropDuplicates()
    return df

