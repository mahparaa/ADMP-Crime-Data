library(sparklyr)
library(dplyr)
library(DBI)
library(odbc)

con <- dbConnect(odbc::odbc(),
                 .connection_string = "Driver={Cloudera ODBC Driver for Apache Hive};Uid=hive;Pwd=hive;Host=sandbox-hdp.hortonworks.com;Port=10000;SCHEMA=crime;")

sc <- spark_connect(master = "local")

leaver_df <- dbReadTable(con, "police_force_leaver")
leaver_df <- sdf_copy_to(sc, leaver_df, "spark_leaver", overwrite = TRUE)

cleaned_leaver_df <- leaver_df %>%
  filter(Sex != 'London') %>%
  filter(Sex != 'Sex') %>%
  mutate(Total_headcount = if_else(Total_headcount == ' -', 0, as.integer(Total_headcount)),
         Total_FTE = if_else(Total_FTE == ' -', 0, as.integer(Total_FTE)),
         Year = sql("substring(Financial_Year, 1, 4)")) %>%
  filter(as.integer(Year) >= 2019) %>%
  mutate(Force_Name = case_when(
    Force_Name == 'Avon and Somerset' ~ paste(Force_Name, 'Constabulary', sep = ' '),
    Force_Name == 'Cumbria' ~ paste(Force_Name, 'Constabulary', sep = ' '),
    Force_Name == 'Metropolitan Police' ~ paste(Force_Name, 'Service', sep = ' '),
    TRUE ~ paste(Force_Name, 'Police', sep = ' ')
  )) %>%
  distinct_all()
  
total <- leaver_df %>% count()
print("Total Police Leaver Rows")
total %>% collect()

cleaned <- cleaned_leaver_df %>% count()
print("Cleaned Police Leaver Rows")
cleaned %>% collect()


#### Absence

absent_df <- dbReadTable(con, "police_force_absent")
absent_df <- sdf_copy_to(sc, absent_df, "spark_absence", overwrite = TRUE)

cleaned_absent_df <- absent_df %>%
  filter(Sex != 'London') %>%
  filter(Sex != 'Sex') %>%
  mutate(headcount = if_else(Total_headcount == ' -', 0, as.integer(Total_headcount)),
         Total_FTE = if_else(Total_FTE == ' -', 0, as.integer(Total_FTE)),
         Year = sql("substring(As_at_31_March_, 1, 4)")) %>%
  filter(as.integer(Year) >= 2019) %>%
  mutate(Force_Name = case_when(
    Force_Name == 'Avon and Somerset' ~ paste(Force_Name, 'Constabulary', sep = ' '),
    Force_Name == 'Cumbria' ~ paste(Force_Name, 'Constabulary', sep = ' '),
    Force_Name == 'Metropolitan Police' ~ paste(Force_Name, 'Service', sep = ' '),
    TRUE ~ paste(Force_Name, 'Police', sep = ' ')
  )) %>%
  distinct_all()

total <- absent_df %>% count()
print("Total Police Absent Rows")
total %>% collect()

cleaned <- cleaned_absent_df %>% count()
print("Cleaned Police Absent Rows")
cleaned %>% collect()


dbExecute(con, "use stage")
dbWriteTable(con, "stage.police_leaver", as.data.frame(cleaned_leaver_df), overwrite = TRUE)
dbWriteTable(con, "stage.police_absent", as.data.frame(cleaned_absent_df), overwrite = TRUE)
spark_disconnect(sc)
