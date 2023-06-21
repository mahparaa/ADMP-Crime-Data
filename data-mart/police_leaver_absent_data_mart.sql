
drop table datawarehouse.police_force_dim;
drop table datawarehouse.police_force_leaver_fact;
drop table datawarehouse.police_force_absent_fact;

create table datawarehouse.police_force_dim (
    ID STRING,
    FORCE_NAME STRING,
    REGION STRING,
    COUNTRY STRING,
    PRIMARY KEY (ID) DISABLE NOVALIDATE
);


create table datawarehouse.police_force_leaver_fact (
    FORCE_ID STRING,
    TIME_ID STRING,
    TOTAL_FTE INT,
    HEADCOUNT INT
);

create table datawarehouse.police_force_absent_fact (
    FORCE_ID STRING,
    TIME_ID STRING,
    TOTAL_FTE INT,
    HEADCOUNT INT
);

insert into datawarehouse.police_force_dim (ID, FORCE_NAME, REGION, COUNTRY)
select UUID(), tmp.FORCE_NAME, tmp.REGION, tmp.COUNTRY
FROM (
    select p.force_name, r.region, r.country
	from stage.police_leaver p
	inner join stage.region r
	on p.region = r.region
	group by p.force_name, r.region, r.country
    union
    select p.force_name, r.region, r.country
	from stage.police_leaver p
	inner join stage.region r
	on p.region = r.region
	group by p.force_name, r.region, r.country
) tmp
group by tmp.force_name, tmp.region, tmp.country;

insert into datawarehouse.police_force_leaver_fact (FORCE_ID, TIME_ID, TOTAL_FTE, HEADCOUNT)
select pd.ID, t.ID, SUM(p.fte), SUM(p.headcount)
from stage.police_leaver p
inner join datawarehouse.police_force_dim pd
on p.force_name = pd.force_name
inner join datawarehouse.time_dim t
on t.year = p.Year
group by pd.ID, t.ID;


insert into datawarehouse.police_force_absent_fact (FORCE_ID, TIME_ID, TOTAL_FTE, HEADCOUNT)
select pd.ID, t.ID, SUM(p.fte), SUM(p.headcount)
from stage.police_absence p
inner join datawarehouse.police_force_dim pd
on p.force_name = pd.force_name
inner join datawarehouse.time_dim t
on t.year = p.Year
group by pd.ID, t.ID;
