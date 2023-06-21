
drop table datawarehouse.outcome_dim;
drop table datawarehouse.outcome_fact;

create table datawarehouse.outcome_dim (
    ID STRING,
    FORCE_NAME STRING,
    OUTCOME_TYPE STRING,
    REGION STRING,
    COUNTRY STRING,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.time_dim (
    ID STRING,
    MONTH INT,
    YEAR INT,
    QUARTER INT,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.outcome_fact (
    OUTCOME_ID STRING,
    TIME_ID STRING,
    TOTAL_NEGATIVE_OUTCOME INT,
    TOTAL_POSITIVE_OUTCOME INT
);

insert into datawarehouse.outcome_dim (ID, FORCE_NAME, OUTCOME_TYPE, REGION, COUNTRY)
select UUID(), c.reported_by, c.LAST_OUTCOME_CATEGORY, r.region, r.country
from stage.crime c
inner join stage.region r
on c.lsoa_code = r.lsoa
group by c.reported_by, c.LAST_OUTCOME_CATEGORY, r.region, r.country;

insert into datawarehouse.OUTCOME_FACT (OUTCOME_ID, TIME_ID, TOTAL_NEGATIVE_OUTCOME, TOTAL_POSITIVE_OUTCOME)
select cd.ID, t.ID, COUNT(CASE WHEN c.is_postive = true THEN 1 END) AS positive_count,
    COUNT(CASE WHEN c.is_postive = false THEN 1 END) AS negative_count
from stage.crime c
inner join datawarehouse.crime_dim cd
on c.reported_by = cd.force_name
inner join datawarehouse.time_dim t
on t.year = Year(c.Month) and t.Month = Month(c.Month) and t.QUARTER = QUARTER(c.MONTH)
group by cd.ID, t.ID;


select * 
from datawarehouse.OUTCOME_FACT;