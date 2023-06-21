import cleaning_crime
import cleaning_police_force
import cleaning_crime_transferred_cancelled
import transform_crime
from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark import StorageLevel

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

### CLEANING
crime_outcome_sql = """select * 
    from crime.crime_outcome 
    where crime_id != '' or crime_id is not null"""
crime_detail_sql = """select * 
    from crime.crime_detail
    where crime_id != '' or crime_id is not null  and lower(crime_type) != 'crime type' and lower(last_outcome_category) != 'last outcome category' 
    """
crime_arrest_sql = """select *
    from crime.crime_arrest 
    """
police_region_sql = """
SELECT * FROM crime.region where region != 'region' or country != 'country'
"""

# arrest_df = spark.sql(crime_arrest_sql)
#detail_df = spark.sql(crime_detail_sql)
#outcome_df = spark.sql(crime_outcome_sql)
# police_region_df = spark.sql(police_region_sql)
#detail_df = cleaning_crime.crime_detail_clean(detail_df)
#outcome_df = cleaning_crime.crime_outcome_clean(outcome_df)
# arrest_df = cleaning_crime.cleaning_crime_arrest(arrest_df)
# police_region_df = cleaning_crime.cleaning_police_region(police_region_df)

police_absent_sql = "SELECT * FROM crime.police_force_absent"
police_leaver_sql = "SELECT * FROM crime.police_force_leaver"
absent_df = spark.sql(police_absent_sql)
leaver_df = spark.sql(police_leaver_sql)
absent_df = cleaning_police_force.police_absent_cleaning(absent_df)
leaver_df = cleaning_police_force.police_leaver_cleaning(leaver_df)

cancelled_transferred_sql = """SELECT *
    FROM crime.transferred_cancelled_crime
"""
# tc_df = spark.sql(cancelled_transferred_sql)
# tc_df = cleaning_crime_transferred_cancelled.clean(tc_df)


## TRANFORM AND LOAD
#joined_df = transform_crime.join_crime_detail_outcome(detail_df, outcome_df)
#joined_df.write.saveAsTable("stage.crime", mode = 'overwrite')

# crime_tc = transform_crime.parse_crime_transferred_cancelled(tc_df)
# crime_tc.write.saveAsTable("stage.crime_can_trans", mode = 'overwrite')

# arrest_df.write.saveAsTable("stage.arrest", mode = 'overwrite')
absent_df.write.saveAsTable("stage.police_absence")
leaver_df.write.saveAsTable("stage.police_leaver")
# police_region_df.write.saveAsTable("stage.region")
