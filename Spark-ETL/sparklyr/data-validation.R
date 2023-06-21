library(sparklyr)
library(dplyr)
library(DBI)
library(odbc)


con <- dbConnect(odbc::odbc(),
                 .connection_string = "Driver={Cloudera ODBC Driver for Apache Hive};Uid=hive;Pwd=hive;Host=sandbox-hdp.hortonworks.com;Port=10000;SCHEMA=crime;")


sc <- spark_connect(master = "local")

validate_failed <- FALSE

## Validating Crime Data
crime <- dbReadTable(con, "crime")
crime_df <- copy_to(sc, crime, "spark_crime", overwrite = TRUE)
# Missing/Empty Check
null_count <- crime_df %>% 
  filter(is.null(Month) | is.null(crime_df$crime_crime_id) | 
           is.null(Reported_by) | 
           is.null(Last_outcome_category) | is.null(LSOA_code)) %>% 
  count()

if (null_count > 0) {
  warning("Crime data found null!")
  validate_failed <- TRUE
}

## Check for duplicate CRIME ID
duplicates <- crime_df %>% 
  group_by(Crime_ID) %>% 
  filter(n() > 1) %>%
  count()

if (duplicates > 0) {
  warning("Crime data: CRIME ID found null!")
  validate_failed <- TRUE
}

## Check if is_positive is boolean
is_positives <- crime_df %>%
  filter(!is.numeric(Is_Positive))
  .count()
if (is_positives > 0) {
  warning("Crime data: Is Positive found non boolean data")
  validate_failed <- TRUE
}  
## Check if data is not empty 
total_count <- crime_df %>% 
  count()
if (total_count == 0) {
  warning("No crime data found!")
  validate_failed <- TRUE
}

## Validating Arrest data
arrest <- dbReadTable(con, "arrest")
arrest_df <- copy_to(sc, arrest, "spark_arrest", overwrite = TRUE)

null_count <- arrest_df %>% 
  filter(is.null(Force_Name) | is.null(Year) | 
           is.null(Region)) %>% 
  count()
if (null_count > 0) {
  warning('Arrest data found null!')
  validate_failed <- TRUE
}

## Check if Data Type is Integer
is_numeric_total_arrests <- arrest_df %>% filter(!is.numeric(Total_arrest))%>% 
  count()
if (is_numeric_total_arrests > 0) {
  warning("Arrest data: total arrest non integer found!")
  validate_failed <- TRUE
}
is_numeric_year <- arrest_df %>% filter(!is.numeric(Year))%>% 
  count()
if (is_numeric_year > 0) {
  warning("Arrest data: year non integer found!")
  validate_failed <- TRUE
}
contains_police_words <- arrest_df %>% 
    filter(Force_Name %like% "%Service" | Force_Name %like% "%Police" | Force_Name %like% "%Constabulary") %>%
    count()
if (contains_police_words > 0) {
  warning("Arrest data: force name invalid data found!")
  validate_failed <- TRUE
}
total_count <- arrest_df %>% 
  count()
if (total_count == 0) {
  warning('No arrest data found!')
  validate_failed <- TRUE
}

## Validating Absent and Leaver
absent_df <- spark_read_table(sc, "stage.police_absence")
null_count <- absent_df %>% 
  filter(is.null(year) | is.null(Force_Name)) %>% 
  count()
if (null_count > 0) {
  warning('Absent data found null!')
  validate_failed <- TRUE
}
total_count <- absent_df %>% 
  count()
if (total_count == 0) {
  warning('No absent data found!')
  validate_failed <- TRUE
}
contains_police_words <- absent_df %>% 
  filter(Force_Name %like% "%Service" | Force_Name %like% "%Police" | Force_Name %like% "%Constabulary") %>%
  count()
if (contains_police_words > 0) {
  warning("Police Absent: force name invalid data found!")
  validate_failed <- TRUE
}
is_numeric_year <- absent_df %>% filter(!is.numeric(Year))%>% 
  count()
if (is_numeric_year > 0) {
  warning("Police Absent: year non integer found!")
  validate_failed <- TRUE
}
is_numeric_fte <- absent_df %>% filter(!is.numeric(FTE))%>% 
  count()
if (is_numeric_fte > 0) {
  warning("Police Absent: fte non integer found!")
  validate_failed <- TRUE
}
is_numeric_headcount <- absent_df %>% filter(!is.numeric(Headcount))%>% 
  count()
if (is_numeric_headcount > 0) {
  warning("Police Absent: headcount non integer found!")
  validate_failed <- TRUE
}

## VALIDATING LEAVER DATA
leaver_df <- spark_read_table(sc, "stage.police_leaver")

null_count <- leaver_df %>% 
  filter(is.null(year) | is.null(Force_Name)) %>% 
  count()
if (null_count > 0) {
  warning('Leaver data found null!')
  validate_failed <- TRUE
}
contains_police_words <- leaver_df %>% 
  filter(Force_Name %like% "%Service" | Force_Name %like% "%Police" | Force_Name %like% "%Constabulary") %>%
  count()
if (contains_police_words > 0) {
  warning("Police Leaver: force name invalid data found!")
  validate_failed <- TRUE
}
total_count <- leaver_df %>% 
  count()
if (total_count == 0) {
  warning('No leaver data found!')
  validate_failed <- TRUE
}
is_numeric_year <- leaver_df %>% filter(!is.numeric(Year))%>% 
  count()
if (is_numeric_year > 0) {
  warning("Police Leaver data: year non integer found!")
  validate_failed <- TRUE
}
is_numeric_fte <- leaver_df %>% filter(!is.numeric(FTE))%>% 
  count()
if (is_numeric_fte > 0) {
  warning("Police Leaver data: fte non integer found!")
  validate_failed <- TRUE
}
is_numeric_headcount <- leaver_df %>% filter(!is.numeric(Headcount))%>% 
  count()
if (is_numeric_headcount > 0) {
  warning("Police Leaver data: headcount non integer found!")
  validate_failed <- TRUE
}

## VALIDATING CRIME CANCEL TRANSFER DATA
crime_can_trans_df <- spark_read_table(sc, "stage.crime_can_trans")
null_count <- can_tran_df %>%
  filter(is.null(year) | is.null(Force_Name) | is.null(status) | is.null(Transfered_and_cancelled_description)) %>%
  count()
if (null_count > 0) {
  warning('Cancel transfer data found null!')
  validate_failed <- TRUE
}
total_count = crime_can_trans_df %>% count()
if (total_count == 0) {
  warning('Crime Cancel Transfer: No cancel transfer data found!')
  validate_failed <- TRUE
}
contains_police_words <- crime_can_trans_df %>% 
  filter(Force_Name %like% "%Service" | Force_Name %like% "%Police" | Force_Name %like% "%Constabulary") %>%
  count()
if (contains_police_words > 0) {
  warning("Crime cancel transfer data: force name invalid data found!")
  validate_failed <- TRUE
}

is_numeric_year <- crime_can_trans_df %>% filter(!is.numeric(Year))%>% 
  count()
if (is_numeric_year > 0) {
  warning("Crime cancel transfer data: year non integer found!")
  validate_failed <- TRUE
}
is_numeric_quarter <- crime_can_trans_df %>% filter(!is.numeric(Quarter))%>% 
  count()
if (is_numeric_quarter > 0) {
  warning("Crime cancel transfer data: year non integer found!")
  validate_failed <- TRUE
}
is_numeric_offence_count <- crime_can_trans_df %>% filter(!is.numeric(Offence_count))%>% 
  count()
if (is_numeric_offence_count > 0) {
  warning(" cancel transferdata: offence count non integer found!")
  validate_failed <- TRUE
}

if (validate_failed) {
  cat('VALIDATION FAILED!')
  stop('Spark Job Terminated! Check logs for reason')
} else {
  cat('SUCCESS VALIDATED!')
}

