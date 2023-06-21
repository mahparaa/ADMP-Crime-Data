from pyspark.sql.functions import col, when, concat, lit

def join_crime_detail_outcome(detail_df, outcome_df):
    joined_df = detail_df.alias('detail') \
        .join(outcome_df.alias('outcome'), \
    detail_df.crime_id == outcome_df.crime_id,  \
    "inner")

    return joined_df \
    .select("detail.crime_id", "detail.falls_within", \
    "detail.lsoa_code", "detail.lsoa_name", "outcome.month", \
    "detail.reported_by", \
    "crimelocation", "last_outcome_category", \
    "crime_type", "outcome_type", "location",  \
    "is_postive")


def parse_crime_transferred_cancelled(tc_df):
    df = tc_df \
        .withColumn("status", when(tc_df['Transfered_And_Cancelled_Description'].like('Cancel%'), 'CANCEL') \
        .otherwise('TRANSFER')) \
        .withColumn("Force_Name", \
            when(col("Force_Name") == 'Avon and Somerset', concat(col("Force_Name"), lit(' Constabulary'))) \
            .when(col("Force_Name") == 'Cumbria', concat(col("Force_Name"), lit(' Constabulary'))) \
            .when(col("Force_Name") == 'Metropolitan Police', concat(col("Force_Name"), lit(' Service'))) \
            .otherwise(concat(col("Force_Name"), lit(" Police")))) \
        

    return df