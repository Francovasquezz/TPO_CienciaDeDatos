-- Última temporada disponible por jugador (field players)
create or replace view players_current_season as
select pss.*
from player_season_stats pss
join (
  select player_uuid, max(season_id) as season_id
  from player_season_stats
  group by player_uuid
) last using (player_uuid, season_id);

-- Agregado 3 últimas temporadas (field players)
create or replace view players_last3_agg as
with last3 as (
  select pss.*,
         row_number() over (partition by player_uuid order by season_id::int desc) rn
  from player_season_stats pss
)
select player_uuid,
       sum(goals) as goals_3y,
       sum(assists) as assists_3y,
       sum(minutes) as minutes_3y,
       avg(xg_per90) as xg_per90_avg_3y,
       avg(xa_per90) as xa_per90_avg_3y
from last3
where rn <= 3
group by player_uuid;

-- Valor de mercado más reciente por jugador
create or replace view market_value_latest as
select mv.player_uuid, mv.value_eur, mv.as_of_date
from market_values mv
join (
  select player_uuid, max(as_of_date) as as_of_date
  from market_values
  group by player_uuid
) last using (player_uuid, as_of_date);
