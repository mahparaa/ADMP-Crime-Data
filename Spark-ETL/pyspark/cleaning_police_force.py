from pyspark.sql.functions import lower, format_string, lit, concat, col, to_date, substring, when
from pyspark.sql.types import IntegerType


def police_absent_cleaning(df):
    df = df.filter(lower(col('Sex')) != 'london') \
        .filter(lower(col('Sex')) != 'sex') \
    .withColumn('headcount', when(col('headcount') == ' -', 0).otherwise(col('headcount').cast(IntegerType()))) \
    .withColumn('FTE', when(col('FTE') == ' -', 0).otherwise(col('FTE').cast(IntegerType()))) \
    .filter(col('year').cast(IntegerType()) >= 2019) \
    .withColumn("Force_Name", \
        when(col("Force_Name") == 'Avon and Somerset', concat(col("Force_Name"), lit(' Constabulary'))) \
        .when(col("Force_Name") == 'Cumbria', concat(col("Force_Name"), lit(' Constabulary'))) \
        .when(col("Force_Name") == 'Metropolitan Police', concat(col("Force_Name"), lit(' Service'))) \
        .otherwise(concat(col("Force_Name"), lit(" Police")))) \
        .dropDuplicates()
    
    return df

def police_leaver_cleaning(df):
    df = df.filter(lower(col('Sex')) != 'london') \
        .filter(lower(col('Sex')) != 'sex') \
    .withColumn('headcount', when(col('headcount') == ' -', 0).otherwise(col('headcount').cast(IntegerType()))) \
    .withColumn('FTE', when(col('FTE') == ' -', 0).otherwise(col('FTE').cast(IntegerType()))) \
    .withColumn('year', substring('Year', 1, 4)) \
    .filter(col('year').cast(IntegerType()) >= 2019) \
    .withColumn("Force_Name", \
        when(col("Force_Name") == 'Avon and Somerset', concat(col("Force_Name"), lit(' Constabulary'))) \
        .when(col("Force_Name") == 'Cumbria', concat(col("Force_Name"), lit(' Constabulary'))) \
        .when(col("Force_Name") == 'Metropolitan Police', concat(col("Force_Name"), lit(' Service'))) \
        .otherwise(concat(col("Force_Name"), lit(" Police")))) \
    .dropDuplicates()

    return df
