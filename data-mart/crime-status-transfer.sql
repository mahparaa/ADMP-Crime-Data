drop table datawarehouse.crime_status_description_dim;
drop table datawarehouse.crime_status_fact;

create table datawarehouse.crime_status_description_dim (
	ID STRING,
	description STRING,
    force_name STRING,
	primary key (ID) disable novalidate
);

create table datawarehouse.crime_status_fact (
	DESCRIPTION_ID STRING,
	TIME_ID STRING,
	FORCE_ID STRING,
	SUM_OFFENCE_COUNT INT
);


insert into datawarehouse.crime_status_description_dim (ID, description, force_name)
select UUID(), ct.transfered_and_cancelled_description, ct.force_name
from stage.crime_can_trans ct
group by ct.transfered_and_cancelled_description, ct.force_name;

insert into datawarehouse.crime_status_fact (DESCRIPTION_ID, TIME_ID, SUM_OFFENCE_COUNT)
select d.ID, t.ID, sum(offence_count)
from stage.crime_can_trans c
inner join datawarehouse.crime_status_description_dim d
on c.transfered_and_cancelled_description = d.description
inner join datawarehouse.time_dim t
on t.YEAR = c.YEAR and t.QUARTER = c.FINANCIAL_QUARTER
group by d.ID, t.ID;
