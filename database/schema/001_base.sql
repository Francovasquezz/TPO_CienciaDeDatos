-- Dimensiones
create table if not exists players (
  player_uuid uuid primary key,
  full_name text not null,
  known_as text,
  dob date,
  citizenship text[],
  height_cm int,
  foot text,
  primary_position text,
  created_at timestamptz default now()
);

create table if not exists teams (
  team_id text primary key,
  team_name text not null,
  country text,
  created_at timestamptz default now()
);

create table if not exists competitions (
  competition_id text primary key,  -- ej: ENG1, ARG1, BRA1
  competition_name text not null,
  country text
);

create table if not exists seasons (
  season_id text primary key,       -- ej: 2022, 2023, 2024
  start_date date,
  end_date date
);

create table if not exists player_xref (
  player_uuid uuid references players(player_uuid) on delete cascade,
  source text check (source in ('fbref','transfermarkt')),
  source_player_id text not null,
  unique (source, source_player_id)
);

-- Hechos
create table if not exists market_values (
  player_uuid uuid references players(player_uuid) on delete cascade,
  as_of_date date not null,
  value_eur numeric,
  source text default 'transfermarkt',
  primary key (player_uuid, as_of_date)
);

create table if not exists player_season_stats (
  player_uuid uuid references players(player_uuid) on delete cascade,
  competition_id text references competitions(competition_id),
  season_id text references seasons(season_id),
  team_id text references teams(team_id),
  minutes int,
  games int,
  starts int,
  goals int,
  assists int,
  xg numeric,
  xa numeric,
  xg_per90 numeric,
  xa_per90 numeric,
  shots int,
  key_passes int,
  yellow int,
  red int,
  position text,
  primary key (player_uuid, competition_id, season_id, team_id)
);

create table if not exists gk_season_stats (
  player_uuid uuid references players(player_uuid) on delete cascade,
  competition_id text references competitions(competition_id),
  season_id text references seasons(season_id),
  team_id text references teams(team_id),
  minutes int,
  games int,
  saves int,
  save_pct numeric,
  goals_against int,
  psxg numeric,          -- post-shot xG faced
  goals_prevented numeric, -- psxg - goals_against
  clean_sheets int,
  primary key (player_uuid, competition_id, season_id, team_id)
);
