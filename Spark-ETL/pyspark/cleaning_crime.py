from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, lower, expr, format_string, lit, concat, col, to_date, substring, when
from pyspark.sql.types import IntegerType

negative_outcomes = [
    'under investigation', 
    'unable to prosecute suspect', 
    'status update unavailable', 
    'awaiting court outcome',
    'action to be taken by another organisation', 
    'investigation complete; no suspect identified', 
    'court result unavailable'
    'formal action is not in the public interest', 
    'further action is not in the public interest', 
    'further investigation is not in the public interest'
]
''' Removing empty values and filling reported by '''
def crime_detail_clean(df):
    df = df.fillna(value = 'UNKOWN', subset = ["reported_by", "falls_within", "crime_type"]) \
    .withColumn('month', to_date('month', 'yyyy-mm')) \
    .withColumn("is_postive", 
        when(lower(col('last_outcome_category')) \
        .isin(negative_outcomes), False) \
        .otherwise(True)
    ) \
    .dropDuplicates()

    return df


def crime_outcome_clean(df):
    df = df.dropDuplicates() \
    .fillna(value = 'UNKOWN', subset = ["reported_by", "falls_within", "crimelocation"]) \
    .withColumn("month", \
        expr("to_date(substring(month, 1, 7), 'yyyy-MM')")) \
    .dropDuplicates()
    return df

def cleaning_crime_arrest(df):
    df = df.withColumn('year', substring('financial_year', 1, 4)) \
        .withColumn('total_arrest', when(col('arrests') == '-', 0).otherwise(col('arrests').cast(IntegerType()))) \
        .drop('arrests') \
        .filter(col('year') >= 2019) \
        .filter(lower(col('gender')) != 'london') \
        .filter(lower(col('gender')) != 'gender') \
        .filter(col('ethnic_group_self_defined') != 'Ethnic Group (self-defined)') \
        .withColumn("ethnic_group_self_defined", lower(regexp_replace('ethnic_group_self_defined', "2019/20 onwards - ", ''))) \
        .filter(col('reason_for_arrest') != '10 - 17 years') \
        .filter(col('reason_for_arrest') != '18 - 20 years')\
        .filter(col('reason_for_arrest') != '21 years and over')\
        .withColumn("reason_for_arrest", regexp_replace('reason_for_arrest', "2015/16 onwards - ", '')) \
        .withColumn("Force_Name", \
        when(col("Force_Name") == 'Avon and Somerset', concat(col("Force_Name"), lit(' Constabulary'))) \
        .when(col("Force_Name") == 'Cumbria', concat(col("Force_Name"), lit(' Constabulary'))) \
        .when(col("Force_Name") == 'Metropolitan Police', concat(col("Force_Name"), lit(' Service'))) \
        .otherwise(concat(col("Force_Name"), lit(" Police")))) \
        .dropDuplicates()

    return df



def cleaning_police_region(df):
    df = df \
        .drop('ID') \
        .drop('ID2') \
        .dropDuplicates()
    return df
