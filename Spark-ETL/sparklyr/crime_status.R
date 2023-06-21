library(sparklyr)
library(dplyr)
library(DBI)
library(odbc)


sc <- spark_connect(master = "local")

crime_status_df <- dbReadTable(con, "transferred_cancelled_crime")
crime_status_df <- sdf_copy_to(sc, leaver_df, "spark_transferred_cancelled_crime", overwrite = TRUE)


crime_status_df %>% count()

cleaned_crime_status_df <- crime_status_df %>%
  filter(!starts_with(Offence_subgroup, " etc\"")) %>%
  mutate(year = sql("substring(Financial_year, 1, 4)")) %>%
  mutate(year = sql("substring(Quarter, 2, 2)")) %>%
  select(-Financial_year) %>%
  mutate(status = if_else(Transfered_and_Cancelled_description %like% "Cancel:", "CANCEL", "TRANSFER"),
         Force_Name = case_when(
           Force_Name == "Avon and Somerset" ~ paste(Force_Name, "Constabulary", sep = " "),
           Force_Name == "Cumbria" ~ paste(Force_Name, "Constabulary", sep = " "),
           Force_Name == "Metropolitan Police" ~ paste(Force_Name, "Service", sep = " "),
           TRUE ~ paste(Force_Name, "Police", sep = " ")))
distinct_all()

cleaned_crime_status_df %>% count()



dbExecute(con, "use stage")
dbWriteTable(con, "stage.crime_can_trans", as.data.frame(crime_arrest_df), overwrite = TRUE)
