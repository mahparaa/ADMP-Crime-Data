library(sparklyr)
library(dplyr)
library(DBI)
library(odbc)

con <- dbConnect(odbc::odbc(),
                 .connection_string = "Driver={Cloudera ODBC Driver for Apache Hive};Uid=hive;Pwd=hive;Host=sandbox-hdp.hortonworks.com;Port=10000;SCHEMA=crime;")

sc <- spark_connect(master = "local")
crime_arrest_df <- dbReadTable(con, "crime_arrest")

crime_arrest_df <- sdf_copy_to(sc, crime_arrest_df, "spark_crime_arrest", overwrite = TRUE)

cleaned_crime_arrest_df <- crime_arrest_df %>% 
  mutate(Year = sql("substring(Financial_Year, 1, 4)"),
         Total_arrest = if_else(Arrests == "-", 0, as.integer(Arrests)),
          Reason_for_arrest_offence_group = gsub("2015/16 onwards - ", "", Reason_for_arrest_offence_group),
         Force_Name = case_when(
           Force_Name == "Avon and Somerset" ~ paste(Force_Name, "Constabulary", sep = " "),
           Force_Name == "Cumbria" ~ paste(Force_Name, "Constabulary", sep = " "),
           Force_Name == "Metropolitan Police" ~ paste(Force_Name, "Service", sep = " "),
           TRUE ~ paste(Force_Name, "Police", sep = " ")
         )) %>% 
  select(-Arrests, -Financial_Year) %>% 
  filter(Year >= 2019 |
           tolower(Gender) != "london" |
           tolower(Gender) != "gender" |
           Reason_for_arrest_offence_group != "10 - 17 years" |
           Reason_for_arrest_offence_group != "18 - 20 years" |
           Reason_for_arrest_offence_group != "21 years and over") %>% 
  sdf_drop_duplicates(c("Year", "Force_Name", "Reason_for_arrest_offence_group"))

total <- crime_arrest_df %>% count()
print("Total Rows")
total %>% collect()

cleaned <- cleaned_crime_arrest_df %>% count()
print("Cleaned Rows")
cleaned %>% collect()

cleaned_crime_arrest_df %>% collect()


spark_disconnect(sc)
