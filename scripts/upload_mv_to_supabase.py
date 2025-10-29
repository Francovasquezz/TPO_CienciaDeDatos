# -*- coding: utf-8 -*-
import os
import sys
import glob
import pandas as pd
from sqlalchemy import create_engine, text

# ================== CONFIG ==================
PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "LettitPrime")
DATABASE_URL = (
    "postgresql://postgres.eaipsfbrivaiqumhijdc:{}"
    "@aws-1-sa-east-1.pooler.supabase.com:6543/postgres"
).format(PASSWORD)

DATA_DIR = "data/processed"  # carpeta con join_*_mv.csv

# Mapeo código -> nombre visible de liga
LEAGUE_NAMES = {
    # Sudamérica
    "arg": "Primera División Argentina",
    "bra": "Brasileirão Serie A",
    # Norteamérica
    "mls": "Major League Soccer",
    # Europa top
    "pl": "Premier League",
    "esp": "LaLiga",
    "ita": "Serie A",
    "fra": "Ligue 1",
    "ger": "Bundesliga",
    "por": "Primeira Liga",
    "ned": "Eredivisie",
    # Europa otras
    "bel": "Belgian Pro League",
    #Arabia
    "sau": "Saudi Pro League",
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
    return str(v).strip().lower() in ("true","1","t","yes","y")

def to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None

def derive_is_gk_from_pos(pos):
    if pos is None:
        return None
    parts = [p.strip().upper() for p in str(pos).split(",") if p.strip()]
    return "GK" in parts

def parse_league_season_from_filename(path):
    """
    join_bel_2025_2026_mv.csv -> league='bel', season='2025-2026'
    join_bra_2025_mv.csv      -> league='bra', season='2025'
    """
    base = os.path.basename(path)
    name = base[:-4] if base.lower().endswith(".csv") else base
    if not name.lower().startswith("join_") or not name.lower().endswith("_mv"):
        return None, None
    core = name[5:-3]  # quita "join_" y "_mv"
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
    """Coalesce nombre/club, tipificar, derivar IsGK, separar GK/OF, dejar solo columnas pedidas."""
    # asegurar columnas base
    ensure_cols(df, ["Player","player_fl","player_norm","club","club_norm","Squad",
                     "Nation","Pos","dob","age","market_value_eur","player_id","IsGK"])

    # coalesce nombre y club
    df["player_name"] = df.apply(
        lambda r: coalesce(r.get("player_name"), r.get("Player"), r.get("player_fl"), r.get("player_norm")),
        axis=1
    )
    df["club"] = df.apply(
        lambda r: coalesce(r.get("club"), r.get("club_norm"), r.get("Squad")),
        axis=1
    )

    # normalizar IsGK / derivar desde Pos
    df["IsGK"] = df["IsGK"].apply(parse_bool)
    df.loc[df["IsGK"].isna(), "IsGK"] = df.loc[df["IsGK"].isna(), "Pos"].map(derive_is_gk_from_pos)

    # tipificar numéricos básicos
    to_numeric(df, ["market_value_eur","age","MatchesPlayed","Gls","Ast","xG","xAG","Shots","SoT",
                    "PassCmp","PassAtt","PassCmpPct","Tkl","TklW","Blocks","Int"])

    # backfill interno por player_id (si dentro del CSV hay filas sin nombre/club)
    name_map = df.groupby("player_id")["player_name"].apply(
        lambda s: next((x for x in s if pd.notna(x) and str(x).strip() != ""), None)
    ).to_dict()
    club_map = df.groupby("player_id")["club"].apply(
        lambda s: next((x for x in s if pd.notna(x) and str(x).strip() != ""), None)
    ).to_dict()
    df["player_name"] = df.apply(lambda r: r["player_name"] if coalesce(r["player_name"]) else name_map.get(r["player_id"]), axis=1)
    df["club"] = df.apply(lambda r: r["club"] if coalesce(r["club"]) else club_map.get(r["player_id"]), axis=1)

    # separar GK vs OF
    df_gk = df[df["IsGK"] == True].copy()
    df_of = df[df["IsGK"] == False].copy()

    # columnas comunes requeridas
    common = [c for c in ["player_id","player_name","club","Nation","Pos","dob","age","market_value_eur"] if c in df.columns]

    # orden de métricas
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

    ensure_cols(df_gk, cols_gk)
    ensure_cols(df_of, cols_of)

    return df_gk[cols_gk], df_of[cols_of]

# ================== MAIN ==================
def main():
    print("Conectando a Neon/Supabase ...")
    try:
        engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10})
        with engine.connect() as _:
            pass
        print("Conexión OK\n")
    except Exception as e:
        print("Error de conexión:", e)
        sys.exit(1)

    # DDL de tablas consolidadas (una sola vez) + columnas nuevas si faltan
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
        # Si ya existían y les falta 'league_name', la agregamos
        conn.execute(text('alter table field_players_all add column if not exists league_name text;'))
        conn.execute(text('alter table goalkeepers_all add column if not exists league_name text;'))
        # Índices
        conn.execute(text("""
            create index if not exists idx_field_all_key
              on field_players_all (league_code, season_code, player_id, club);
        """))
        conn.execute(text("""
            create index if not exists idx_gk_all_key
              on goalkeepers_all (league_code, season_code, player_id, club);
        """))

    # Buscar archivos *_mv.csv
    pattern = os.path.join(DATA_DIR, "join_*_mv.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print("No encontré archivos con patrón:", pattern)
        sys.exit(0)

    print("CSV a procesar:")
    for f in files: print(" -", os.path.basename(f))
    print()

    # Procesar cada archivo: borrar liga+temporada y subir
    for path in files:
        league, season = parse_league_season_from_filename(path)
        if not league:
            print(f"Saltando {path}: no pude parsear league/season.")
            continue
        lname = league_name_for(league)

        try:
            df = pd.read_csv(path)
        except Exception as e:
            print(f"[{league} {season}] Error leyendo {path}: {e}")
            continue

        df_gk, df_of = clean_and_split(df)

        # agregar league/season + league_name antes de subir
        for d in (df_gk, df_of):
            d["league_code"] = league
            d["season_code"] = season
            d["league_name"] = lname

        try:
            with engine.begin() as conn:
                # borrar lo existente de esa liga/temporada y luego append
                conn.execute(text("delete from goalkeepers_all where league_code=:l and season_code=:s"), {"l": league, "s": season})
                conn.execute(text("delete from field_players_all where league_code=:l and season_code=:s"), {"l": league, "s": season})

                if len(df_gk) > 0:
                    df_gk.to_sql("goalkeepers_all", conn, if_exists="append", index=False, method="multi", chunksize=1000)
                if len(df_of) > 0:
                    df_of.to_sql("field_players_all", conn, if_exists="append", index=False, method="multi", chunksize=1000)

            print(f"[{league} {season}] OK -> GK={len(df_gk)}  OF={len(df_of)}  ({lname})")

        except Exception as e:
            print(f"[{league} {season}] Error al subir: {e}")

    print("\nProceso terminado.")

if __name__ == "__main__":
    main()
