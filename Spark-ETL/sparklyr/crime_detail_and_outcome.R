library(sparklyr)
library(dplyr)
library(DBI)
library(odbc)

con <- dbConnect(odbc::odbc(),
                 .connection_string = "Driver={Cloudera ODBC Driver for Apache Hive};Uid=hive;Pwd=hive;Host=sandbox-hdp.hortonworks.com;Port=10000;SCHEMA=crime;")


sc <- spark_connect(master = "local")

sc <- spark_connect(master = "local")
detail_df <- dbReadTable(con, "crime_detail")
detail_df <- sdf_copy_to(sc, detail_df, "spark_crime_detail", overwrite = TRUE)
outcome_df <- dbReadTable(con, "crime_outcome")
outcome_df <- sdf_copy_to(sc, outcome_df, "spark_crime_outcome", overwrite = TRUE)

negative_outcomes <- c ("under investigation",
                        "unable to prosecute suspect",
                        "status update unavialable",
                        "awaiting court outcome",
                        "action to be taken by another organisaion",
                        "investigation complete; no suspect identified",
                        "court restult unavailable",
                        "formal action is not in the public interest",
                        "further action is not in the public interest",
                        "further investigation is not in the public interest")

## Cleaning And Transforming Crime Detail and Crime Outcome
cleaned_detail_df <- detail_df %>%
  sparklyr::filter(!is.na(Crime_ID)) %>%
  mutate(Reported_by = if_else(is.na(Reported_by), "UNKNOWN", Reported_by),
         Falls_within = if_else(is.na(Falls_within), "UNKNOWN", Falls_within),
         Crime_type = if_else(is.na(Crime_type), "UNKNOWN", Crime_type),
         Month = sql("DATE_FORMAT(Month, 'yyyy-MM')"),
         is_postive = if_else(tolower(Last_outcome_category) %in% negative_outcomes, FALSE, TRUE)) %>%
  sdf_drop_duplicates(c("Reported_by", "Falls_within", "Crime_type", "Crime_ID", "Month"))


total <- detail_df %>% count()
print("Total Rows")
total %>% collect()

cleaned <- cleaned_detail_df %>% count()
print("Cleaned Rows")
cleaned %>% collect()


## Cleaning And Transforming Crime Detail and Crime Outcome
cleaned_outcome_df <- outcome_df %>%
  sparklyr::filter(!is.na(Crime_ID)) %>%
  mutate(Reported_by = if_else(is.na(Reported_by), "UNKNOWN", Reported_by),
         Falls_within = if_else(is.na(Falls_within), "UNKNOWN", Falls_within),
         Month = sql("DATE_FORMAT(Month, 'yyyy-MM')")) %>%
  sdf_drop_duplicates(c("Reported_by", "Falls_within", "Crime_ID", "Month"))


total <- outcome_df %>% count()
print("Total Rows")
total %>% collect()

cleaned <- cleaned_outcome_df %>% count()
print("Cleaned Rows")
cleaned %>% collect()



## TRANSFORMING

join_outcome_detail_df <- cleaned_detail_df %>% inner_join(cleaned_outcome_df, by = "Crime_ID") %>%
  select(Crime_ID, Falls_within_x, LSOA_code_x, LSOA_name_x,
          Month_x, Reported_by_x, Last_outcome_category,
          Crime_type, Outcome_type, Location_x, is_postive)

join_outcome_detail_df %>% arrange(Crime_ID) %>% filter(row_number() <= 5) %>% collect()


dbExecute(con, "use stage")
dbWriteTable(con, "stage.arrest", as.data.frame(join_outcome_detail_df), overwrite = TRUE)
spark_disconnect(sc)
