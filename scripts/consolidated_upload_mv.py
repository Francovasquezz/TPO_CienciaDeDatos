# -*- coding: utf-8 -*-
import os
import sys
import glob
import argparse
import pandas as pd
from sqlalchemy import create_engine, text

# ================== CONFIG ==================
PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "LettitPrime")
DATABASE_URL = (
    "postgresql://postgres.eaipsfbrivaiqumhijdc:{}"
    "@aws-1-sa-east-1.pooler.supabase.com:6543/postgres"
).format(PASSWORD)

# default input dir for join_*_mv.csv
DATA_DIR_DEFAULT = "data/processed"

# Map league code -> human readable name
LEAGUE_NAMES = {
    # South America
    "arg": "Primera Division Argentina",
    "bra": "Brasileirao Serie A",
    "uru": "Primera Division Uruguaya",
    "chi": "Primera Division de Chile",
    "par": "Primera Division Paraguaya",
    "per": "Liga 1 Peru",
    "ecu": "Liga Pro Ecuador",
    "bol": "Primera Division Bolivia",
    "col": "Categoria Primera A",
    "mex": "Liga MX",
    # North America
    "mls": "Major League Soccer",
    # Europe top-5 + Portugal, Netherlands, Turkey
    "eng": "Premier League",
    "esp": "LaLiga",
    "fra": "Ligue 1",
    "ita": "Serie A",
    "ger": "Bundesliga",
    "por": "Primeira Liga",
    "ned": "Eredivisie",
    "tur": "Super Lig",
    # Other Europe
    "bel": "Belgian Pro League",
    "sco": "Scottish Premiership",
    "sui": "Swiss Super League",
    "aut": "Austrian Bundesliga",
    "den": "Danish Superliga",
    "nor": "Eliteserien",
    "swe": "Allsvenskan",
    "pol": "Ekstraklasa",
    "cze": "Czech First League",
    "ukr": "Ukrainian Premier League",
}

# ================== HELPERS ==================
def coalesce(*vals):
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s != "" and s.lower() not in ("nan", "none", "null"):
            return v
    return None

def parse_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    return str(v).strip().lower() in ("true", "1", "t", "yes", "y")

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None

def to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def normalize_dob_series(s):
    # Accepts YYYY-MM-DD and DD/MM/YYYY
    # dayfirst=True allows european formats like 17/12/2001
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return dt.dt.date  # datetime.date objects, psycopg2 handles well

def derive_is_gk_from_pos(pos):
    # Wider detection: GK, GOAL, PORT(ero)
    if pos is None:
        return None
    s = str(pos).upper()
    return ("GK" in s) or ("GOAL" in s) or ("PORT" in s)

def parse_league_season_from_filename(path):
    """
    Examples:
      join_esp_2024_2025_mv.csv -> league='esp', season='2024-2025'
      join_ger_2024_2025_mv.csv -> league='ger', season='2024-2025'
      join_bel_2025_2026_mv.csv -> league='bel', season='2025-2026'
    """
    base = os.path.basename(path)
    name = base[:-4] if base.lower().endswith(".csv") else base
    if not (name.lower().startswith("join_") and name.lower().endswith("_mv")):
        return None, None
    core = name[5:-3]  # strip "join_" and "_mv"
    parts = core.split("_")
    if not parts:
        return None, None
    league = parts[0].lower()
    season_tokens = parts[1:] if len(parts) > 1 else []
    season = "-".join(season_tokens) if len(season_tokens) > 1 else (season_tokens[0] if season_tokens else "")
    return league, season

def league_name_for(code):
    if not code:
        return ""
    return LEAGUE_NAMES.get(code.lower(), code.upper())

def clean_and_split(df):
    """
    Prepare DF: coalesce player_name/club, normalize types, derive IsGK,
    normalize 'dob', split GK/OF, keep only requested columns (ordered).
    """
    # Ensure base columns exist
    ensure_cols(df, [
        "Player","player_fl","player_norm",
        "club","club_norm","Squad",
        "Nation","Pos","dob","age","market_value_eur","player_id","IsGK"
    ])

    # Coalesce identity columns
    df["player_name"] = df.apply(
        lambda r: coalesce(r.get("player_name"), r.get("Player"), r.get("player_fl"), r.get("player_norm")),
        axis=1
    )
    df["club"] = df.apply(
        lambda r: coalesce(r.get("club"), r.get("club_norm"), r.get("Squad")),
        axis=1
    )

    # Normalize IsGK / derive from Pos
    df["IsGK"] = df["IsGK"].apply(parse_bool)
    df.loc[df["IsGK"].isna(), "IsGK"] = df.loc[df["IsGK"].isna(), "Pos"].map(derive_is_gk_from_pos)

    # Normalize dob to ISO date
    if "dob" in df.columns:
        df["dob"] = normalize_dob_series(df["dob"])

    # Numeric typing
    to_numeric(df, [
        "market_value_eur","age",
        "MatchesPlayed","Gls","Ast","xG","xAG","Shots","SoT",
        "PassCmp","PassAtt","PassCmpPct","Tkl","TklW","Blocks","Int",
        "GK_GA","GK_GA90","GK_SoTA","GK_Saves","GK_SavePct","GK_CS","GK_CSPct",
        "GK_PKAtt","GK_PKA","GK_PKsv","GK_PKm",
        "GK_PSxG","GK_PSxG_per_SoT","GK_PSxG_PlusMinus","GK_PSxG_PlusMinus_per90",
        "GK_PassCmp","GK_PassAtt","GK_PassCmpPct","GK_GKPassAtt","GK_Throws",
        "GK_LaunchPct","GK_AvgLen","GK_CrossesStp","GK_CrossesStpPct",
        "GK_OPA","GK_OPA90","GK_OPA_AvgDist"
    ])

    # Backfill player_name/club by player_id within same DF
    name_map = df.groupby("player_id")["player_name"].apply(
        lambda s: next((x for x in s if pd.notna(x) and str(x).strip() != ""), None)
    ).to_dict()
    club_map = df.groupby("player_id")["club"].apply(
        lambda s: next((x for x in s if pd.notna(x) and str(x).strip() != ""), None)
    ).to_dict()
    df["player_name"] = df.apply(
        lambda r: r["player_name"] if coalesce(r["player_name"]) else name_map.get(r["player_id"]), axis=1
    )
    df["club"] = df.apply(
        lambda r: r["club"] if coalesce(r["club"]) else club_map.get(r["player_id"]), axis=1
    )

    # Split GK vs OF
    df_gk = df[df["IsGK"] == True].copy()
    df_of = df[df["IsGK"] == False].copy()

    # Target columns (ordered)
    common = [c for c in ["player_id","player_name","club","Nation","Pos","dob","age","market_value_eur"] if c in df.columns]

    gk_order = [
        "GK_GA","GK_GA90","GK_SoTA","GK_Saves","GK_SavePct","GK_CS","GK_CSPct",
        "GK_PKAtt","GK_PKA","GK_PKsv","GK_PKm",
        "GK_PSxG","GK_PSxG_per_SoT","GK_PSxG_PlusMinus","GK_PSxG_PlusMinus_per90",
        "GK_PassCmp","GK_PassAtt","GK_PassCmpPct","GK_GKPassAtt","GK_Throws",
        "GK_LaunchPct","GK_AvgLen","GK_CrossesStp","GK_CrossesStpPct",
        "GK_OPA","GK_OPA90","GK_OPA_AvgDist"
    ]
    of_order = [
        "MatchesPlayed","Gls","Ast","xG","xAG","Shots","SoT",
        "PassCmp","PassAtt","PassCmpPct","Tkl","TklW","Blocks","Int"
    ]

    gk_metrics = [c for c in gk_order if c in df.columns]
    of_metrics = [c for c in of_order if c in df.columns]

    cols_gk = common + gk_metrics
    cols_of = common + of_metrics

    # Ensure presence then reorder
    ensure_cols(df_gk, cols_gk)
    ensure_cols(df_of, cols_of)
    df_gk = df_gk[cols_gk]
    df_of = df_of[cols_of]

    return df_gk, df_of

# ================== MAIN ==================
def main():
    ap = argparse.ArgumentParser(description="Consolidated uploader for join_*_mv.csv -> Supabase (two master tables).")
    ap.add_argument("--dir", default=DATA_DIR_DEFAULT, help="Directory with join_*_mv.csv files (default: data/processed)")
    ap.add_argument("--codes", help="Comma-separated whitelist of league codes to load (e.g. esp,ger,fra). If omitted, loads all found.", default=None)
    args = ap.parse_args()

    if not DATABASE_URL:
        print("Missing DATABASE_URL or SUPABASE_DB_PASSWORD.", file=sys.stderr)
        sys.exit(1)

    codes_whitelist = None
    if args.codes:
        codes_whitelist = set([x.strip().lower() for x in args.codes.split(",") if x.strip()])

    print("Connecting to Supabase/Neon ...")
    try:
        engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10})
        with engine.connect() as _:
            pass
        print("Connection OK\n")
    except Exception as e:
        print("Connection error:", e)
        sys.exit(1)

    # Create master tables (idempotent) + add league_name if missed + indexes
    with engine.begin() as conn:
        conn.execute(text("""
            create table if not exists field_players_all (
              player_id bigint,
              player_name text,
              club text,
              "Nation" text,
              "Pos" text,
              dob date,
              age int,
              market_value_eur numeric,
              "MatchesPlayed" numeric,
              "Gls" int,
              "Ast" int,
              "xG" numeric,
              "xAG" numeric,
              "Shots" int,
              "SoT" int,
              "PassCmp" int,
              "PassAtt" int,
              "PassCmpPct" numeric,
              "Tkl" int,
              "TklW" int,
              "Blocks" int,
              "Int" int,
              league_code text,
              season_code text,
              league_name text
            );
        """))
        conn.execute(text("""
            create table if not exists goalkeepers_all (
              player_id bigint,
              player_name text,
              club text,
              "Nation" text,
              "Pos" text,
              dob date,
              age int,
              market_value_eur numeric,
              "GK_GA" int,
              "GK_GA90" numeric,
              "GK_SoTA" int,
              "GK_Saves" int,
              "GK_SavePct" numeric,
              "GK_CS" int,
              "GK_CSPct" numeric,
              "GK_PKAtt" int,
              "GK_PKA" int,
              "GK_PKsv" int,
              "GK_PKm" int,
              "GK_PSxG" numeric,
              "GK_PSxG_per_SoT" numeric,
              "GK_PSxG_PlusMinus" numeric,
              "GK_PSxG_PlusMinus_per90" numeric,
              "GK_PassCmp" int,
              "GK_PassAtt" int,
              "GK_PassCmpPct" numeric,
              "GK_GKPassAtt" int,
              "GK_Throws" int,
              "GK_LaunchPct" numeric,
              "GK_AvgLen" numeric,
              "GK_CrossesStp" int,
              "GK_CrossesStpPct" numeric,
              "GK_OPA" int,
              "GK_OPA90" numeric,
              "GK_OPA_AvgDist" numeric,
              league_code text,
              season_code text,
              league_name text
            );
        """))
        conn.execute(text('alter table field_players_all add column if not exists league_name text;'))
        conn.execute(text('alter table goalkeepers_all add column if not exists league_name text;'))
        conn.execute(text("""
            create index if not exists idx_field_all_key
              on field_players_all (league_code, season_code, player_id, club);
        """))
        conn.execute(text("""
            create index if not exists idx_gk_all_key
              on goalkeepers_all (league_code, season_code, player_id, club);
        """))

    pattern = os.path.join(args.dir, "join_*_mv.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print("No join_*_mv.csv files found in:", args.dir)
        sys.exit(0)

    print("Found mv files:")
    for f in files:
        print(" -", os.path.basename(f))
    print()

    total_loaded = []

    for path in files:
        league, season = parse_league_season_from_filename(path)
        if not league:
            print(f"Skip {os.path.basename(path)}: could not parse league/season.")
            continue
        if codes_whitelist and league not in codes_whitelist:
            print(f"Skip {os.path.basename(path)}: league not in whitelist.")
            continue

        lname = league_name_for(league)

        try:
            df = pd.read_csv(path, encoding="utf-8")
        except Exception:
            # fallback if locale issues
            df = pd.read_csv(path, encoding="latin1")

        df_gk, df_of = clean_and_split(df)

        # attach league/season/name
        for d in (df_gk, df_of):
            d["league_code"] = league
            d["season_code"] = season
            d["league_name"] = lname

        try:
            with engine.begin() as conn:
                # delete existing block for this league+season
                conn.execute(text("delete from goalkeepers_all where league_code=:l and season_code=:s"),
                             {"l": league, "s": season})
                conn.execute(text("delete from field_players_all where league_code=:l and season_code=:s"),
                             {"l": league, "s": season})

                # append
                if len(df_gk) > 0:
                    df_gk.to_sql("goalkeepers_all", conn, if_exists="append", index=False, method="multi", chunksize=1000)
                if len(df_of) > 0:
                    df_of.to_sql("field_players_all", conn, if_exists="append", index=False, method="multi", chunksize=1000)

            print(f"[{league} {season}] OK -> GK={len(df_gk)}  OF={len(df_of)}  ({lname})")
            total_loaded.append((league, season, len(df_gk), len(df_of)))
        except Exception as e:
            print(f"[{league} {season}] ERROR loading {os.path.basename(path)}: {e}")

    print("\nSummary loaded:")
    for l, s, gk_n, of_n in total_loaded:
        print(f"  {l} {s} -> GK={gk_n} OF={of_n}")
    print("\nDone.")

if __name__ == "__main__":
    main()
