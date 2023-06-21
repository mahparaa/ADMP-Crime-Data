drop table datawarehouse.police_force_dim;
drop table datawarehouse.reported_by_dim;

create table datawarehouse.police_force_dim (
	ID STRING,
	FORCE_NAME STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.reported_by_fact (
    TIME_ID STRING,
	FORCE_ID STRING,
	REGION_ID STRING,
    TOTAL_CRIME_TYPE INT,
	CONSTRAINT fk_reported_by_fact_reported_by FOREIGN KEY (FORCE_ID) REFERENCES datawarehouse.police_force_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_reported_by_fact_time FOREIGN KEY (TIME_ID) REFERENCES datawarehouse.time_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_reported_by_fact_region FOREIGN KEY (REGION_ID) REFERENCES datawarehouse.region_dim (ID) DISABLE NOVALIDATE
);


-- INSERTING VALID DATA
insert into datawarehouse.police_force_dim (ID, FORCE_NAME)
select UUID(), REPORTED_BY
from stage.crime
group by reported_by;


insert into datawarehouse.reported_by_fact(CRIME_ID, TIME_ID, FORCE_ID, REGION_ID, TOTAL_CRIME_TYPE)
select c.ID, t.ID, f.ID, rd.ID, COUNT(sc.CRIME_TYPE)
from stage.crime sc
inner join datawarehouse.crime_dim c
on sc.CRIME_TYPE = c.CRIME_TYPE
inner join  stage.region r
on sc.lsoa_code = r.lsoa
INNER JOIN datawarehouse.region_dim rd
on r.region = rd.region
inner join datawarehouse.time_dim t
on t.YEAR = YEAR(sc.MONTH) and t.MONTH = MONTH(sc.MONTH) and t.QUARTER = QUARTER(sc.MONTH)
inner join datawarehouse.police_force_dim f
on f.force_name = sc.reported_by
group by c.ID, t.ID, f.ID, rd.ID;



-- RESULT
select date_format(concat(YEAR, '-', MONTH, '-01'), 'yyyy-MM'), COUNTRY, REGION, FORCE_NAME, CRIME_TYPE, SUM(TOTAL_CRIME_TYPE) total_crime
from datawarehouse.reported_by_fact c
inner join datawarehouse.region_dim r
on c.REGION_ID = r.ID
inner join datawarehouse.time_dim t
on t.ID = c.TIME_ID
inner join datawarehouse.police_force_dim f
on f.ID = c.FORCE_ID
inner join datawarehouse.crime_dim cd
on cd.ID = c.CRIME_ID
group by t.YEAR, t.MONTH, r.COUNTRY, r.REGION, f.FORCE_NAME, cd.CRIME_TYPE
order by total_crime desc;
