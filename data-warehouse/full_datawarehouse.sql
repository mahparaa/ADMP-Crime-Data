
drop table full_datawarehouse.police_leaver_dim;
drop table full_datawarehouse.police_absence_dim;
drop table full_datawarehouse.police_leaver_absence_fact;
drop table full_datawarehouse.transfer_cancel_status_dim;
drop table full_datawarehouse.crime_status_fact;
drop table full_datawarehouse.time_dim;
drop table full_datawarehouse.crime_dim;
drop table full_datawarehouse.quarter_dim;
drop table full_datawarehouse.year_dim;
drop table full_datawarehouse.crime_fact;
drop table full_datawarehouse.reported_crime_fact;
drop table full_datawarehouse.arrest_fact;
drop table full_datawarehouse.region_dim;
drop table full_datawarehouse.police_force_dim;
drop table full_datawarehouse.reported_by_dim;

create table full_datawarehouse.crime_dim (
    ID STRING,
    CRIME_TYPE STRING,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.time_dim (
    ID STRING,
    MONTH INT,
    YEAR INT,
    QUARTER INT,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.country_dim (
	ID STRING, 
	COUNTRY STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.region_dim (
	ID STRING,
	COUNTRY_ID STRING,
	REGION STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_region_country_dim_id FOREIGN KEY (COUNTRY_ID) REFERENCES full_datawarehouse.country_dim (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.transfer_cancel_status_dim (
	ID STRING,
	DESCRIPTION STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);


create table full_datawarehouse.outcome_dim (
    ID STRING,
    FORCE_NAME STRING,
    OUTCOME_TYPE STRING,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.year_dim (
	ID STRING,
	YEAR INT,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);


create table full_datawarehouse.quarter_dim (
	ID STRING,
	YEAR_ID STRING,
	QUARTER INT,
	PRIMARY KEY (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_time_dim_year FOREIGN KEY (YEAR_ID) REFERENCES full_datawarehouse.year_dim (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.time_dim (
    ID STRING,
	QUARTER_ID STRING,
    MONTH INT,
    PRIMARY KEY (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_time_dim_quarter FOREIGN KEY (QUARTER_ID) REFERENCES full_datawarehouse.quarter_dim (ID) DISABLE NOVALIDATE
);


create table full_datawarehouse.police_leaver_dim (
	ID STRING,
	FORCE_NAME STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.police_absence_dim (
	ID STRING,
	FORCE_NAME STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table full_datawarehouse.police_force_dim (
	ID STRING,
	FORCE_LEAVER_ID STRING,
	FORCE_ABSNECE_ID STRING,
	FORCE_NAME STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_police_force_dim_absence_id FOREIGN KEY (FORCE_ABSNECE_ID) REFERENCES full_datawarehouse.police_absence_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_police_force_dim_leaver_id FOREIGN KEY (FORCE_LEAVER_ID) REFERENCES full_datawarehouse.police_leaver_dim (ID) DISABLE NOVALIDATE
);


create table full_datawarehouse.reported_crime_fact (
	CRIME_ID STRING,
	STATUS_ID STRING,
	OUTCOME_ID STRING,
	TIME_ID STRING,
	REGION_ID STRING,
	TOTAL_CRIME_TYPE INT,
	TOTAL_OFFENECE INT,
	TOTAL_NEGATIVE_OUTCOME INT,
    TOTAL_POSITIVE_OUTCOME INT,
	TOTAL_ARREST INT,
	CONSTRAINT fk_reported_crime_fact_crime_id FOREIGN KEY (CRIME_ID) REFERENCES full_datawarehouse.crime_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_reported_crime_fact_status_id FOREIGN KEY (STATUS_ID) REFERENCES full_datawarehouse.crime_status_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_reported_crime_fact_outcome_id FOREIGN KEY (OUTCOME_ID) REFERENCES full_datawarehouse.outcome_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_reported_crime_fact_region_id FOREIGN KEY (REGION_ID) REFERENCES full_datawarehouse.region_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_reported_crime_fact_time_id FOREIGN KEY (TIME_ID) REFERENCES full_datawarehouse.time_dim (ID) DISABLE NOVALIDATE
);


create table full_datawarehouse.police_leaver_absence_fact (
	FORCE_ID STRING,
	REGION_ID STRING,
	TIME_ID STRING,
	TOTAL_FTE INT,
	TOTAL_HEADCOUNT INT,
	CONSTRAINT fk_police_leaver_absence_fact_force_id FOREIGN KEY (FORCE_ID) REFERENCES full_datawarehouse.police_force_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_police_leaver_absence_fact_region_id FOREIGN KEY (REGION_ID) REFERENCES full_datawarehouse.region_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_police_leaver_absence_fact_time_id FOREIGN KEY (TIME_ID) REFERENCES full_datawarehouse.time_dim (ID) DISABLE NOVALIDATE
);
