
drop table datawarehouse.crime_dim;
drop table datawarehouse.time_dim;
drop table datawarehouse.crime_fact;
drop table datawarehouse.region_dim;


create table datawarehouse.crime_dim (
    ID STRING,
    CRIME_TYPE STRING,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.time_dim (
    ID STRING,
    MONTH INT,
    YEAR INT,
    QUARTER INT,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);


create table datawarehouse.region_dim (
	ID STRING,
	REGION STRING,
	COUNTRY STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.crime_fact (
    CRIME_ID STRING,
    TIME_ID STRING,
	REGION_ID STRING,
    TOTAL_CRIME_TYPE INT,
	CONSTRAINT fk_crime_fact_crime_type FOREIGN KEY (CRIME_ID) REFERENCES datawarehouse.crime_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_crime_fact_time FOREIGN KEY (TIME_ID) REFERENCES datawarehouse.time_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_crime_fact_region FOREIGN KEY (REGION_ID) REFERENCES datawarehouse.region_dim (ID) DISABLE NOVALIDATE
);


-- INSERT VALIDATED DATA
insert into datawarehouse.crime_dim (ID, CRIME_TYPE)
select UUID(), c.CRIME_TYPE
from stage.crime c
group by c.crime_type;

insert into datawarehouse.time_dim(ID, MONTH, YEAR, QUARTER)
select UUID(), Month(c.Month), Year(c.Month), QUARTER(c.MONTH)
from stage.crime c
group by c.Month;


insert into datawarehouse.region_dim (ID, REGION, COUNTRY) 
select UUID(), REGION, COUNTRY
from stage.region r
group by r.region, r.country;


insert into datawarehouse.crime_fact (CRIME_ID, TIME_ID, REGION_ID, TOTAL_CRIME_TYPE)
select c.ID, t.ID, rd.ID, COUNT(s.crime_type)
from stage.crime s
inner join datawarehouse.crime_dim c
on s.CRIME_TYPE = c.CRIME_TYPE
inner join  stage.region r
on s.lsoa_code = r.lsoa
INNER JOIN datawarehouse.region_dim rd
on r.region = rd.region
inner join datawarehouse.time_dim t
on t.YEAR = YEAR(s.MONTH) and t.MONTH = MONTH(s.MONTH) and t.QUARTER = QUARTER(s.MONTH)
group by c.ID, t.ID, rd.ID;


--- RESULT
select date_format(concat(YEAR, '-', MONTH, '-01'), 'yyyy-MM'), COUNTRY, REGION, SUM(TOTAL_CRIME_TYPE) total_crime
from datawarehouse.crime_fact c
inner join datawarehouse.region_dim r
on c.REGION_ID = r.ID
inner join datawarehouse.time_dim t
on t.ID = c.TIME_ID
group by t.YEAR, t.MONTH, r.COUNTRY, r.REGION
order by total_crime desc;
