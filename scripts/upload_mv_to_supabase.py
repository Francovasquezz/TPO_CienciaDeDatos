# -*- coding: utf-8 -*-
import os
import sys
import glob
import pandas as pd
from sqlalchemy import create_engine, text

# ========= Config =========
PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "LettitPrime")
DATABASE_URL = (
    "postgresql://postgres.eaipsfbrivaiqumhijdc:{}"
    "@aws-1-sa-east-1.pooler.supabase.com:6543/postgres"
).format(PASSWORD)

DATA_DIR = "data/processed"  # carpeta donde están los CSV *_mv

# ========= Helpers =========
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

def league_from_filename(path):
    # join_bel_2025_2026_mv.csv -> bel
    base = os.path.basename(path)
    name = base[:-4] if base.lower().endswith(".csv") else base
    if not name.lower().startswith("join_"):
        return None
    # join_<liga>_<...>_mv
    rest = name[5:]  # quita "join_"
    parts = rest.split("_")
    return parts[0].lower() if parts else None

def clean_split(df):
    """Prepara DF: coalesce de nombre/club, IsGK, tipos; separa GK/OF y deja solo columnas pedidas."""
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

    # tipificar numéricos clave (ya vienen con MV>0, pero reforzamos tipos)
    to_numeric(df, ["market_value_eur","age","MatchesPlayed","Gls","Ast","xG","xAG","Shots","SoT",
                    "PassCmp","PassAtt","PassCmpPct","Tkl","TklW","Blocks","Int"])

    # backfill por player_id (si dentro del mismo csv hay filas sin nombre/club)
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

    # orden común + métricas pedidas
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

    ensure_cols(df_gk, cols_gk)
    ensure_cols(df_of, cols_of)

    return df_gk[cols_gk], df_of[cols_of]

def upload_league(engine, csv_path):
    league = league_from_filename(csv_path)
    if not league:
        print(f"Saltando {csv_path}: no pude extraer el código de liga.")
        return

    print(f"[{league}] Cargando {csv_path} ...")
    df = pd.read_csv(csv_path)
    df_gk, df_of = clean_split(df)

    tbl_gk = f"goalkeepers_{league}"
    tbl_of = f"field_players_{league}"

    with engine.begin() as conn:
        if len(df_gk) > 0:
            df_gk.to_sql(tbl_gk, conn, if_exists="replace", index=False, method="multi", chunksize=1000)
        else:
            # crear vacía con columnas correctas
            pd.DataFrame(columns=df_gk.columns if len(df_gk.columns) else ["player_id"]).to_sql(
                tbl_gk, conn, if_exists="replace", index=False
            )

        if len(df_of) > 0:
            df_of.to_sql(tbl_of, conn, if_exists="replace", index=False, method="multi", chunksize=1000)
        else:
            pd.DataFrame(columns=df_of.columns if len(df_of.columns) else ["player_id"]).to_sql(
                tbl_of, conn, if_exists="replace", index=False
            )

    print(f"[{league}] OK -> {tbl_gk} ({len(df_gk)}) | {tbl_of} ({len(df_of)})")

def main():
    print("Conectando a Neon/Supabase ...")
    try:
        engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10})
        with engine.connect() as _:
            pass
        print("Conexión OK\n")
    except Exception as e:
        print(f"Error de conexión: {e}")
        sys.exit(1)

    pattern = os.path.join(DATA_DIR, "join_*_mv.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"No encontré archivos con patrón: {pattern}")
        sys.exit(0)

    print("Encontrados CSV a cargar:")
    for f in files: print(" -", os.path.basename(f))
    print()

    for f in files:
        try:
            upload_league(engine, f)
        except Exception as e:
            print(f"Error subiendo {f}: {e}")

    print("\nProceso terminado.")

if __name__ == "__main__":
    main()
