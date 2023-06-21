

drop table datawarehouse.crime_status_dim;
drop table datawarehouse.crime_status_fact;


create table datawarehouse.crime_status_dim (
	ID STRING,
	DESCRIPTION STRING,
	PRIMARY KEY (ID) DISABLE NOVALIDATE
);

create table datawarehouse.crime_status_fact (
	STATUS_ID STRING,
	FORCE_ID STRING,
	TIME_ID STRING,
	TOTAL_OFFENECE INT,
	CONSTRAINT fk_crime_status_fact_status_id FOREIGN KEY (STATUS_ID) REFERENCES datawarehouse.crime_status_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_crime_status_fact_force_id FOREIGN KEY (FORCE_ID) REFERENCES datawarehouse.police_force_dim (ID) DISABLE NOVALIDATE,
	CONSTRAINT fk_crime_status_fact_time_id FOREIGN KEY (TIME_ID) REFERENCES datawarehouse.time_dim (ID) DISABLE NOVALIDATE
);

-- INSERT VALID DATA

insert into datawarehouse.crime_status_description_dim (ID, description, force_name)
select UUID(), ct.transfered_and_cancelled_description, ct.force_name
from stage.crime_can_trans ct
where ct.transfered_and_cancelled_description like 'Cancel%' or ct.transfered_and_cancelled_description like 'Transfer%'
group by ct.transfered_and_cancelled_description, ct.force_name;


insert into datawarehouse.crime_status_fact (STATUS_ID, FORCE_ID, TIME_ID, TOTAL_OFFENECE)
select d.ID, p.ID, t.ID, sum(offence_count)
from stage.crime_can_trans c
inner join datawarehouse.crime_status_description_dim d
on c.transfered_and_cancelled_description = d.description
inner join datawarehouse.time_dim t
on t.YEAR = c.YEAR and t.QUARTER = c.FINANCIAL_QUARTER
inner join datawarehouse.police_force_dim p
on c.FORCE_NAME = p.FORCE_NAME
group by d.ID, p.ID, t.ID;


-- RESULT
select t.Year, t.Quarter, p.force_name, d.description, SUM(TOTAL_OFFENECE) offence_count
from datawarehouse.crime_status_fact f
inner join datawarehouse.crime_status_description_dim d
on f.status_id = d.id
inner join datawarehouse.police_force_dim p
on f.force_id = p.id
inner join datawarehouse.time_dim t
on t.id = f.time_id
group by t.Year, t.Quarter, p.force_name, d.description
order by offence_count desc;


