-- creating stream on raw table for 3 different stream
create or replace stream cricket.raw.for_match_stream
  on table cricket.raw.match_raw_tbl
  append_only = true;

create or replace stream cricket.raw.for_player_stream
  on table cricket.raw.match_raw_tbl
  append_only = true;

create or replace stream cricket.raw.for_delivery_stream
  on table cricket.raw.match_raw_tbl
  append_only = true;



-- Creating a task that runs every 5min to load json data into raw layer.
create or replace task cricket.raw.load_json_to_raw
  warehouse = 'CRICKET_ANALYTICS'
  schedule = '5 minute'
as
  copy into cricket.raw.match_raw_tbl from
  (
    select
      t.$1:meta::object as meta,
      t.$1:info::variant as info,
      t.$1:innings::array as innings,
      --
      metadata$filename,
      metadata$file_row_number,
      metadata$file_content_key,
      metadata$file_last_modified
    from @cricket.land.my_stg/cricket/json (file_format => 'cricket.land.my_json_format') t
  )
  on_error = continue;



--Creating another child task to read stream & load data into clean layer.
create or replace task cricket.raw.load_to_clean_match
  warehouse = 'CRICKET_ANALYTICS'
  after cricket.raw.load_json_to_raw
  when system$stream_has_data('cricket.raw.for_match_stream')
as
insert into cricket.clean.match_detail_clean
select
  info:match_type_number::int as match_type_number,
  info:event.name::text as event_name,
  case
    when info:event.match_number::text is not null then info:event.match_number::text
    when info:event.stage::text is not null then info:event.stage::text
    else 'NA'
  end as match_stage,
  info:dates[0]::date as event_date,
  date_part('year', info:dates[0]::date) as event_year,
  date_part('month', info:dates[0]::date) as event_month,
  date_part('day', info:dates[0]::date) as event_day,
  info:match_type::text as match_type,
  info:season::text as season,
  info:team_type::text as team_type,
  info:overs::text as overs,
  info:city::text as city,
  info:venue::text as venue,
  info:gender::text as gender,
  info:teams[0]::text as first_team,
  info:teams[1]::text as second_team,
  case
    when info:outcome.winner is not null then 'Result Declared'
    when info:outcome.result = 'tie' then 'Tie'
    when info:outcome.result = 'no result' then 'No Result'
    else info:outcome.result
  end as match_result,
  case
    when info:outcome.winner is not null then info:outcome.winner
    else 'NA'
  end as winner,
  info:toss.winner::text as toss_winner,
  initcap(info:toss.decision::text) as toss_decision,
  stg_file_name,
  stg_file_row_number,
  stg_file_hashkey,
  stg_modified_ts
from
  cricket.raw.for_match_stream;



--creating a child task after match data is populated
create or replace task cricket.raw.load_to_clean_player
  warehouse = 'CRICKET_ANALYTICS'
  after cricket.raw.load_to_clean_match
  when system$stream_has_data('cricket.raw.for_player_stream')
as
insert into cricket.clean.player_clean_tbl
select
  rcm.info:match_type_number::int as match_type_number,
  p.path::text as country,
  team.value::text as player_name,
  stg_file_name,
  stg_file_row_number,
  stg_file_hashkey,
  stg_modified_ts
from
  cricket.raw.for_player_stream rcm,
  lateral flatten (input => rcm.info:players) p,
  lateral flatten (input => p.value) team;



--creating delivery clean table
create or replace task cricket.raw.load_to_clean_delivery
  warehouse = 'CRICKET_ANALYTICS'
  after cricket.raw.load_to_clean_player
  when system$stream_has_data('cricket.raw.for_delivery_stream')
as
insert into cricket.clean.delivery_clean_tbl
select
    m.info:match_type_number::int as match_type_number,
    i.value:team::text as team_name,
    o.value:over::int+1 as over,
    d.value:bowler::text as bowler,
    d.value:batter::text as batter,
    d.value:non_striker::text as non_striker,
    d.value:runs.batter::text as runs,
    d.value:runs.extras::text as extras,
    d.value:runs.total::text as total,
    e.key::text as extra_type,
    e.value::number as extra_runs,
    w.value:player_out::text as player_out,
    w.value:kind::text as player_out_kind,
    w.value:fielders::variant as player_out_fielders,
    m.stg_file_name,
    m.stg_file_row_number,
    m.stg_file_hashkey,
    m.stg_modified_ts
from cricket.raw.for_delivery_stream m,
lateral flatten (input => m.innings) i,
lateral flatten (input => i.value:overs) o,
lateral flatten (input => o.value:deliveries) d,
lateral flatten (input => d.value:extras, outer => True) e,
lateral flatten (input => d.value:wickets, outer => True) w;
 



--populate the fact table
create or replace task cricket.raw.load_match_fact
  warehouse = 'CRICKET_ANALYTICS'
  after cricket.raw.load_to_team_dim,cricket.raw.load_to_player_dim,cricket.raw.load_to_venue_dim
as
insert into cricket.consumption.match_fact
select a.* from (
  select
    m.match_type_number as match_id,
    dd.date_id as date_id,
    0 as referee_id,
    ftd.team_id as first_team_id,
    std.team_id as second_team_id,
    mtd.match_type_id as match_type_id,
    vd.venue_id as venue_id,
    50 as total_overs,
    6 as balls_per_overs,
    max(case when d.team_name = m.first_team then d.over else 0 end) as overs_played_by_team_a,
    sum(case when d.team_name = m.first_team then 1 else 0 end) as balls_played_by_team_a,
    sum(case when d.team_name = m.first_team then d.extras else 0 end) as extra_balls_played_by_team_a,
    sum(case when d.team_name = m.first_team then d.extra_runs else 0 end) as extra_runs_scored_by_team_a,
    0 fours_by_team_a,
    0 sixes_by_team_a,
    (sum(case when d.team_name = m.first_team then d.runs else 0 end) + sum(case when d.team_name = m.first_team then d.extra_runs else 0 end)) as total_runs_scored_by_team_a,
    sum(case when d.team_name = m.first_team and player_out is not null then 1 else 0 end) as wicket_lost_by_team_a,
    max(case when d.team_name = m.second_team then d.over else 0 end) as overs_played_by_team_b,
    sum(case when d.team_name = m.second_team then 1 else 0 end) as balls_played_by_team_b,
    sum(case when d.team_name = m.second_team then d.extras else 0 end) as extra_balls_played_by_team_b,
    sum(case when d.team_name = m.second_team then d.extra_runs else 0 end) as extra_runs_scored_by_team_b,
    0 fours_by_team_b,
    0 sixes_by_team_b,
    (sum(case when d.team_name = m.second_team then d.runs else 0 end) + sum(case when d.team_name = m.second_team then d.extra_runs else 0 end)) as total_runs_scored_by_team_b,
    sum(case when d.team_name = m.second_team and player_out is not null then 1 else 0 end) as wicket_lost_by_team_b,
    tw.team_id as toss_winner_team_id,
    m.toss_decision as toss_decision,
    m.match_result as match_result,
    mw.team_id as winner_team_id
  from
    cricket.clean.match_detail_clean m
    join cricket.consumption.date_dim dd on m.event_date = dd.full_dt
    join cricket.consumption.team_dim ftd on m.first_team = ftd.team_name
    join cricket.consumption.team_dim std on m.second_team = std.team_name
    join cricket.consumption.match_type_dim mtd on m.match_type = mtd.match_type
    join cricket.consumption.venue_dim vd on m.venue = vd.venue_name and m.city = vd.city
    join cricket.clean.delivery_clean_tbl d on d.match_type_number = m.match_type_number
    join cricket.consumption.team_dim tw on m.toss_winner = tw.team_name
    join cricket.consumption.team_dim mw on m.winner = mw.team_name
  group by
    m.match_type_number,
    dd.date_id,
    referee_id,
    first_team_id,
    second_team_id,
    mtd.match_type_id,
    vd.venue_id,
    total_overs,
    toss_winner_team_id,
    m.toss_decision,
    m.match_result,
    winner_team_id
) a
left join cricket.consumption.match_fact b on a.match_id = b.match_id
where b.match_id is null;






  