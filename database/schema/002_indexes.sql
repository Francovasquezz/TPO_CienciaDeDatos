create index if not exists ix_player_xref_source_pair on player_xref(source, source_player_id);
create index if not exists ix_player_mv_player_date on market_values(player_uuid, as_of_date desc);
create index if not exists ix_pss_player on player_season_stats(player_uuid);
create index if not exists ix_pss_comp_season on player_season_stats(competition_id, season_id);
create index if not exists ix_pss_team on player_season_stats(team_id);

create index if not exists ix_gk_player on gk_season_stats(player_uuid);
create index if not exists ix_gk_comp_season on gk_season_stats(competition_id, season_id);
create index if not exists ix_gk_team on gk_season_stats(team_id);
