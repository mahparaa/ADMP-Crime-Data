from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging

log = logging.getLogger(__name__)



warehouse_location = abspath('spark-warehouse')
spark = SparkSession \
    .builder \
    .appName("SparkByExamples.com") \
    .master("local[*]") \
    .config("spark.driver.host", "localhost") \
    .config("spark.executor.memory", "16g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.memory.offHeap.enabled", True) \
    .config("spark.memory.offHeap.size", "80g") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()


crime_sql = """select * 
    from stage.crime"""

crime_arrest_sql = """select *
    from stage.arrest 
    """
police_absence_sql = """
SELECT * FROM stage.police_absence
"""
police_leaver_sql = """
SELECT * FROM stage.police_leaver
"""

police_transfer_cancel_sql = """
SELECT * FROM stage.crime_can_trans
"""

region_sql = """
SELECT * FROM stage.region
"""

# crime_df = spark.sql(crime_sql)

validate_failed = False
## Validating Crime Data
null_count = crime_df.filter(\
     col('Month').isNull() | \
     col('crime_id').isNull() | \
     col('reported_by').isNull() | \
     col('last_outcome_category').isNull() | \
     col('lsoa_code').isNull()).count()
duplicates = crime_df.dropDuplicates().count()
total_count = crime_df.count()
if total_count == 0:
    log.warn("No crime data found!")
    validate_failed = True
if null_count > 0:
    log.warn("Crime data found null!")
    validate_failed = True
if duplicates != total_count:
    log.warn("Crime data duplicates found!")
    validate_failed = True

## Validating Arrest data
arrest_df = spark.sql(crime_arrest_sql)
null_count = arrest_df.filter( \
    col('force_name').isNull() | \
    col('Year').isNull() | \
    col('region').isNull()) \
    .count()
total_count = arrest_df.count()
duplicates = arrest_df.dropDuplicates().count()
if total_count == 0:
    log.warn('No arrest data found!')
    validate_failed = True
if null_count > 0:
    log.warn('Arrest data found null!')
    validate_failed = True
if total_count != duplicates:
    log.warn('Arrest data duplicates found!')
    validate_failed = True

## Validating Absent and Leaver
absent_df = spark.sql(police_absence_sql)
null_count = absent_df.filter( \
    col('year').isNull() | \
    col('Force_Name').isNull()).count()
duplicates = absent_df.dropDuplicates().count()
total_count = absent_df.count()
if total_count == 0:
    log.warning('No absent data found!')
    validate_failed = True
if null_count > 0:
    log.warning('Absent data found null!')
    validate_failed = True
if total_count != duplicates:
    log.warning('Absent data duplicates found!')
    validate_failed = True

leaver_df = spark.sql(police_leaver_sql)
null_count = leaver_df.filter( \
    col('year').isNull() | \
    col('Force_Name').isNull()).count()
duplicates = leaver_df.dropDuplicates().count()
total_count = leaver_df.count()
if total_count == 0:
    log.warning('No leaver data found!')
    validate_failed = True
if null_count > 0:
    log.warning('Leaver data found null!')
    validate_failed = True
if total_count != duplicates:
    log.warning('Leaver data duplicates found!')
    validate_failed = True

can_tran_df = spark.sql(police_transfer_cancel_sql)
null_count = can_tran_df.filter(
    col('year').isNull() | \
    col('Force_Name').isNull() | \
    col('status').isNull()).count()
duplicates = can_tran_df.dropDuplicates().count()
total_count = leaver_df.count()
if total_count == 0:
    log.warning('No cancel transfer data found!')
    validate_failed = True
if null_count > 0:
    log.warning('Cancel transfer data found null!')
    validate_failed = True
if False: # or duplicates != total_count:
    log.warning('Cancel transfer found duplicates')
    validate_failed = True

if validate_failed:
    log.warning('VALIDATION FAILED!')
    raise Exception('Spark Job Terminated! Check logs for reason')
else:
    log.warning('SUCCESS VALIDATED!')