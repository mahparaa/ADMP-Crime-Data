drop table datawarehouse.arrest_fact;
drop table datawarehouse.arrest_reason;

create table datawarehouse.arrest_reason (
	ID STRING,
	REASON STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.arrest_fact (
	FORCE_ID STRING,
	REGION_ID STRING,
	TIME_ID STRING,
	TOTAL_ARREST INT,
	CONSTRAINT fk_arrest_fact_force_id FOREIGN KEY (FORCE_ID) REFERENCES datawarehouse.police_force_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_arrest_fact_region_id FOREIGN KEY (REGION_ID) REFERENCES datawarehouse.region_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_arrest_fact_time_id FOREIGN KEY (TIME_ID) REFERENCES datawarehouse.time_dim (ID) DISABLE NOVALIDATE
);

-- INSERT VALID DATA


insert into datawarehouse.arrest_fact (FORCE_ID, REGION_ID, TIME_ID, TOTAL_ARREST)
select f.ID, r.ID, t.ID, SUM(TOTAL_ARREST)
from stage.arrest a
inner join datawarehouse.police_force_dim f
on a.force_name = f.force_name
inner join datawarehouse.region r
on r.region = a.region
inner join datawarehouse.time_dim t
on t.YEAR = a.YEAR
group by f.ID, r.ID, t.ID;


-- RESULT

select pf.FORCE_NAME, r.COUNTRY, r.REGION, t.YEAR, SUM(total_arrest) total_arrest
from datawarehouse.arrest_fact a
inner join datawarehouse.police_force_dim pf
on a.FORCE_ID = pf.ID
inner join datawarehouse.region_dim r
on r.ID = a.REGION_ID
inner join datawarehouse.time_dim t
on t.ID = a.TIME_ID
group by pf.FORCE_NAME, r.COUNTRY, r.REGION, t.YEAR
order by total_arrest desc;
