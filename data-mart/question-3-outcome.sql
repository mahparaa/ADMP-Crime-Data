drop table datawarehouse.outcome_fact;
drop table datawarehouse.outcome_dim;


create table datawarehouse.outcome_dim (
	ID STRING,
	OUTCOME_TYPE STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.outcome_fact (
	FORCE_ID STRING,
	REGION_ID STRING,
	TIME_ID STRING,
	OUTCOME_ID STRING,
	TOTAL_NEGATIVE_OUTCOME INT,
	TOTAL_POSTIIVE_OUTCOME INT,
	
	CONSTRAINT fk_outcome_fact_force_id FOREIGN KEY (FORCE_ID) REFERENCES datawarehouse.police_force_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_outcome_fact_region_id FOREIGN KEY (REGION_ID) REFERENCES datawarehouse.region_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_outcome_fact_time_id FOREIGN KEY (TIME_ID) REFERENCES datawarehouse.time_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_outcome_fact_outcome_id FOREIGN KEY (OUTCOME_ID) REFERENCES datawarehouse.outcome_dim (ID) DISABLE NOVALIDATE
);


-- INSERT VALID DATA
insert into datawarehouse.outcome_dim (ID, OUTCOME_TYPE)
select UUID(), LAST_OUTCOME_CATEGORY
from stage.crime c
group by c.last_outcome_category;



insert into datawarehouse.outcome_fact (FORCE_ID, REGION_ID, TIME_ID, OUTCOME_ID, TOTAL_NEGATIVE_OUTCOME, TOTAL_POSTIIVE_OUTCOME)
select f.ID, rd.ID, t.ID, o.ID, COUNT(CASE WHEN c.is_postive = true THEN 1 END) AS positive_count,
    COUNT(CASE WHEN c.is_postive = false THEN 1 END) AS negative_count
from stage.crime c
inner join datawarehouse.police_force_dim f
on c.reported_by = f.force_name
inner join stage.region r
on c.lsoa_code = r.lsoa
INNER JOIN datawarehouse.region_dim rd
on r.region = rd.region
inner join datawarehouse.outcome_dim o
on o.OUTCOME_TYPE = c.last_outcome_category
inner join datawarehouse.time_dim t
on t.YEAR = YEAR(c.MONTH) and t.MONTH = MONTH(c.MONTH) and t.QUARTER = QUARTER(c.MONTH)
group by f.ID, rd.ID, t.ID, o.ID;



-- RESULT

select YEAR, REGION, FORCE_NAME, OUTCOME_TYPE, SUM(TOTAL_NEGATIVE_OUTCOME) no, SUM(TOTAL_POSTIIVE_OUTCOME) po
from datawarehouse.outcome_fact oc
inner join datawarehouse.outcome_dim od
on od.id = oc.outcome_id
inner join datawarehouse.police_force_dim pd
on pd.id = oc.force_id
inner join datawarehouse.region_dim rd
on rd.id = oc.region_id
inner join datawarehouse.time_dim td
on td.id = oc.time_id
group by td.year, rd.region, pd.force_name, od.outcome_type
order by no desc;
