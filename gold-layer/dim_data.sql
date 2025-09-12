insert into cricket.consumption.team_dim (team_name)
select distinct team_name from (
    select first_team as team_name from cricket.clean.match_detail_clean
    union all
    select second_team as team_name from cricket.clean.match_detail_clean
) order by team_name;


insert into cricket.consumption.player_dim (team_id, player_name)
select b.team_id, a.player_name
from
    cricket.clean.player_clean_tbl a
    join cricket.consumption.team_dim b
        on a.country = b.team_name
group by
    b.team_id,
    a.player_name;


insert into cricket.consumption.venue_dim (venue_name, city)
select venue, city from (
    select
        venue,
        case when city is null then 'NA'
             else city
        end as city
    from cricket.clean.match_detail_clean
)
group by
    venue,
    city;

    

insert into cricket.consumption.match_type_dim (match_type)
select match_type from cricket.clean.match_detail_clean group by match_type;



INSERT INTO cricket.consumption.date_dim (Date_ID, Full_Dt, Day, Month, Year, Quarter, DayOfWeek, DayOfMonth, DayOfYear, DayOfWeekName, IsWeekend)
SELECT
    ROW_NUMBER() OVER (ORDER BY Date) AS DateID,
    Date AS FullDate,
    EXTRACT(DAY FROM Date) AS Day,
    EXTRACT(MONTH FROM Date) AS Month,
    EXTRACT(YEAR FROM Date) AS Year,
    CASE WHEN EXTRACT(QUARTER FROM Date) IN (1, 2, 3, 4) THEN EXTRACT(QUARTER FROM Date) END AS Quarter,
    DAYOFWEEKISO(Date) AS DayOfWeek,
    EXTRACT(DAY FROM Date) AS DayOfMonth,
    DAYOFYEAR(Date) AS DayOfYear,
    DAYNAME(Date) AS DayOfWeekName,
    CASE When DAYNAME(Date) IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS IsWeekend
FROM cricket.consumption.date_rnage01;

