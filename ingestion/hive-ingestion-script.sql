drop table crime.crime_arrest;
drop table crime.crime_detail;
drop table crime.crime_outcome;
drop table crime.police_force_leaver;
drop table crime.police_force_absent;
drop table crime.transferred_cancelled_crime;
drop table crime.region;

create table crime.crime_arrest (
    Financial_Year STRING,
	Geocode STRING,
	Force_Name STRING,
	Region STRING,
	Gender STRING,
	Ethnic_Group_self_defined STRING,
	Age_group STRING,
	Reason_for_arrest STRING,
	Arrests STRING
) row format delimited fields terminated by ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");


-- CRIME DETAIL (STREET) TABLE 

CREATE TABLE crime.crime_detail (
	Crime_ID STRING,
	Month STRING,
	Reported_by STRING,
	Falls_within STRING,
	Longitude DOUBLE,
	Latitude DOUBLE,
	Location STRING,
	LSOA_Code STRING,
	LSOA_Name STRING,
	Crime_Type STRING,
	Last_outcome_category STRING,
	Context STRING
) row format delimited fields terminated by ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");


-- CRIME TYPE
CREATE TABLE crime.crime_outcome (
	Crime_ID STRING,
	Month STRING, 
	Reported_by	STRING,
	Falls_within STRING, 
	Longitude DOUBLE,
	Latitude DOUBLE, 
	CrimeLocation STRING,
	LSOA_code STRING, 
	LSOA_name STRING,
	Outcome_type STRING
) row format delimited fields terminated by ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");


-- POLICE FORCE LEAVERS
create table crime.police_force_leaver (
    Year STRING,
    Geocode STRING,
    Force_Name STRING,
	Region	STRING,
    Sex STRING,
	Rank_description STRING,
	Worker_type STRING,
	Workaround STRING,
    Ethnic_group STRING,
	Leaver_type STRING,
	headcount STRING,
	FTE STRING
)  row format delimited fields terminated by ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

-- POLICE FORCE ABSENT
create table crime.police_force_absent (
    Year STRING,
    Geocode STRING,
    Force_Name STRING,
	Region	STRING,
    Sex STRING,
	Rank_description STRING,
	Worker_type STRING,
	Ethnic_group STRING,
	Absence_type STRING,
	headcount STRING,
	FTE STRING
)  row format delimited fields terminated by ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");


CREATE TABLE crime.transferred_cancelled_crime (
	Financial_year	STRING,
	Financial_quarter STRING,
	Force_Name	STRING,
	Offence_Description	STRING,
	Offence_Group STRING,
	Offence_Subgroup STRING,
	Offence_Code STRING,
	Transfered_and_Cancelled_type STRING,
	Transfered_and_Cancelled_description STRING,
	Offence_count INTEGER
) row format delimited fields terminated by ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");


CREATE TABLE crime.region (
	Id STRING,
	Id2 STRING,
	LSOA STRING,
	Region STRING,
	Country STRING
) row format delimited fields terminated by ','
STORED AS TEXTFILE
tblproperties("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/home/mahpara/share/ADMP/crime-data/crime/crime_arrest.csv' OVERWRITE INTO TABLE crime.crime_arrest;
LOAD DATA LOCAL INPATH '/home/maria_dev/crime-data/crime/detail/*/**.csv' OVERWRITE into table crime.crime_detail;
LOAD DATA LOCAL INPATH '/home/maria_dev/crime-data/crime/outcome/*/**.csv' OVERWRITE into table crime.crime_outcome;

LOAD DATA LOCAL INPATH '/home/maria_dev/crime-data/crime/Police_Leavers_Dataset.csv' OVERWRITE into table crime.police_force_leaver;
LOAD DATA LOCAL INPATH '/home/maria_dev/crime-data/crime/Police_Absent_Dataset.csv' OVERWRITE into table crime.police_force_absent;
LOAD DATA LOCAL INPATH '/home/maria_dev/crime-data/crime/case-transferred-or-cancelled/*.csv' OVERWRITE into table crime.transferred_cancelled_crime;
LOAD DATA LOCAL INPATH '/home/maria_dev/crime-data/crime/regions.csv' OVERWRITE into table crime.region;drop table crime.crime_arrest;

