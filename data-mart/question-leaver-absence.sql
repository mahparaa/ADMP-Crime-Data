drop table datawarehouse.police_leaver_dim;
drop table datawarehouse.police_absence_dim;
drop table datawarehouse.police_leaver_absence_fact;

create table datawarehouse.police_leaver_dim (
	ID STRING,
	FORCE_NAME STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.police_absence_dim (
	ID STRING,
	FORCE_NAME STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.police_leaver_absence_fact (
	ABSENCE_ID STRING,
	LEAVER_ID STRING,
	REGION_ID STRING,
	TIME_ID STRING,
	TOTAL_FTE INT,
	TOTAL_HEADCOUNT INT,
	CONSTRAINT fk_police_leaver_absence_fact_absence_id FOREIGN KEY (ABSENCE_ID) REFERENCES datawarehouse.police_absence_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_police_leaver_absence_fact_leaver_id FOREIGN KEY (LEAVER_ID) REFERENCES datawarehouse.police_leaver_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_police_leaver_absence_region_id FOREIGN KEY (REGION_ID) REFERENCES datawarehouse.region_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_police_leaver_absence_time_id FOREIGN KEY (TIME_ID) REFERENCES datawarehouse.time_dim (ID) DISABLE NOVALIDATE
);

-- INSERTING VALID DATE

insert into datawarehouse.police_leaver_absence_fact (ABSENCE_ID, LEAVER_ID, REGION_ID, TIME_ID, TOTAL_FTE, TOTAL_HEADCOUNT)
select absence_id, leaver_id, region_id, time_id, total_fte, total_headcount
from (select pad.ID as absence_id, NULL as leaver_id, r.ID as region_id, t.ID as time_id, SUM(FTE) as total_fte, SUM(HEADCOUNT) as total_headcount
	from stage.police_absence pa
	inner join datawarehouse.police_absence_dim pad
	on pa.force_name = pad.force_name
	inner join datawarehouse.region_dim r
	on pa.region = r.region
	inner join datawarehouse.time_dim t
	on t.YEAR = pa.Year
	group by pad.ID, r.ID, t.ID
	union
	select NULL as absence_id, pld.ID as leaver_id, r.ID as region_id, t.ID as time_id, SUM(FTE) as total_fte, SUM(HEADCOUNT) as total_headcount
	from stage.police_leaver pl
	inner join datawarehouse.police_leaver_dim pld
	on pl.force_name = pld.force_name
	inner join datawarehouse.region_dim r
	on pl.region = r.region
	inner join datawarehouse.time_dim t
	on t.YEAR = pl.Year
    group by pld.ID, r.ID, t.ID) tmp;
	
-- RESULT
