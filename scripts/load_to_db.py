# -*- coding: utf-8 -*-
import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# ========= Config DB =========
PASSWORD = os.environ.get("SUPABASE_DB_PASSWORD", "LettitPrime")
DATABASE_URL = (
    f"postgresql://postgres.eaipsfbrivaiqumhijdc:{PASSWORD}"
    f"@aws-1-sa-east-1.pooler.supabase.com:6543/postgres"
)

CSV_PATH = "data/processed/join_arg_2025_mv.csv"
TABLE_GK = "goalkeepers_arg"
TABLE_OF = "field_players_arg"

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
    return str(v).strip().lower() in ("true", "1", "t", "yes", "y")

def to_numeric(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = None

# ========= Conexión =========
print("Conectando a Neon/Supabase ...")
try:
    engine = create_engine(DATABASE_URL, connect_args={"connect_timeout": 10})
    with engine.connect() as _:
        print("✓ Conexión OK\n")
except Exception as e:
    print(f"❌ Error de conexión: {e}")
    sys.exit(1)

# ========= Drop vistas (si existen) =========
try:
    with engine.begin() as conn:
        conn.execute(text('drop view if exists vw_field_players_arg;'))
        conn.execute(text('drop view if exists vw_goalkeepers_arg;'))
        print("✓ Vistas vw_* eliminadas (si existían)\n")
except Exception as e:
    print(f"⚠ No se pudieron eliminar vistas (continuo): {e}\n")

# ========= Carga CSV =========
print(f"Leyendo CSV: {CSV_PATH}")
df = pd.read_csv(CSV_PATH)
print(f"Filas leídas: {len(df)}")

# ========= Normalizaciones =========
ensure_cols(df, ["Player","player_fl","player_norm","club","club_norm","Squad",
                 "Nation","Pos","dob","age","market_value_eur","player_id","IsGK"])

# Nombre y club (coalesce)
df["player_name"] = df.apply(
    lambda r: coalesce(r.get("player_name"), r.get("Player"), r.get("player_fl"), r.get("player_norm")),
    axis=1
)
df["club"] = df.apply(
    lambda r: coalesce(r.get("club"), r.get("club_norm"), r.get("Squad")),
    axis=1
)

# IsGK: normalizar/derivar
df["IsGK"] = df["IsGK"].apply(parse_bool)
def derive_is_gk_from_pos(pos):
    if pos is None:
        return None
    parts = [p.strip().upper() for p in str(pos).split(",") if p.strip()]
    return "GK" in parts
df.loc[df["IsGK"].isna(), "IsGK"] = df.loc[df["IsGK"].isna(), "Pos"].map(derive_is_gk_from_pos)

# Numéricos
to_numeric(df, ["market_value_eur","age","MatchesPlayed","Gls","Ast","xG","xAG","Shots","SoT",
                "PassCmp","PassAtt","PassCmpPct","Tkl","TklW","Blocks","Int"])

# Backfill por player_id (si alguna fila de ese jugador trae nombre/club)
name_map = df.groupby("player_id")["player_name"].apply(
    lambda s: next((x for x in s if pd.notna(x) and str(x).strip() != ""), None)
).to_dict()
club_map = df.groupby("player_id")["club"].apply(
    lambda s: next((x for x in s if pd.notna(x) and str(x).strip() != ""), None)
).to_dict()
df["player_name"] = df.apply(lambda r: r["player_name"] if coalesce(r["player_name"]) else name_map.get(r["player_id"]), axis=1)
df["club"] = df.apply(lambda r: r["club"] if coalesce(r["club"]) else club_map.get(r["player_id"]), axis=1)

# ========= Separar GK / Campo =========
df_gk = df[df["IsGK"] == True].copy()
df_of = df[df["IsGK"] == False].copy()
print(f"Arqueros: {len(df_gk)} | Jugadores de campo: {len(df_of)}")

# ========= Orden de columnas deseado =========
common_cols = [c for c in ["player_id","player_name","club","Nation","Pos","dob","age","market_value_eur"] if c in df.columns]

# Orden fijo para GK_* (incluye los más comunes; si falta alguno, se ignora)
gk_order = [
    "GK_GA","GK_GA90","GK_SoTA","GK_Saves","GK_SavePct","GK_CS","GK_CSPct",
    "GK_PKAtt","GK_PKA","GK_PKsv","GK_PKm",
    "GK_PSxG","GK_PSxG_per_SoT","GK_PSxG_PlusMinus","GK_PSxG_PlusMinus_per90",
    "GK_PassCmp","GK_PassAtt","GK_PassCmpPct","GK_GKPassAtt","GK_Throws",
    "GK_LaunchPct","GK_AvgLen","GK_CrossesStp","GK_CrossesStpPct",
    "GK_OPA","GK_OPA90","GK_OPA_AvgDist"
]
gk_metrics = [c for c in gk_order if c in df.columns]

# Orden fijo para campo
of_order = [
    "MatchesPlayed","Gls","Ast","xG","xAG","Shots","SoT",
    "PassCmp","PassAtt","PassCmpPct","Tkl","TklW","Blocks","Int"
]
of_metrics = [c for c in of_order if c in df.columns]

# Construir DataFrames finales en el orden exacto (sin IsGK)
cols_gk = common_cols + gk_metrics
cols_of = common_cols + of_metrics

# Asegurar columnas para to_sql
for need in cols_gk:
    if need not in df_gk.columns: df_gk[need] = None
for need in cols_of:
    if need not in df_of.columns: df_of[need] = None

df_gk = df_gk[cols_gk]
df_of = df_of[cols_of]

# ========= Subir (replace) =========
print("\nSubiendo tablas finales (replace) ...")
try:
    with engine.begin() as conn:
        # Reemplazar tablas con columnas en este orden
        if len(df_gk) > 0:
            df_gk.to_sql(TABLE_GK, conn, if_exists="replace", index=False, method="multi", chunksize=1000)
            print(f"✓ {len(df_gk)} filas -> {TABLE_GK}")
        else:
            pd.DataFrame(columns=cols_gk).to_sql(TABLE_GK, conn, if_exists="replace", index=False)
            print("✓ 0 filas -> creada tabla GK vacía")

        if len(df_of) > 0:
            df_of.to_sql(TABLE_OF, conn, if_exists="replace", index=False, method="multi", chunksize=1000)
            print(f"✓ {len(df_of)} filas -> {TABLE_OF}")
        else:
            pd.DataFrame(columns=cols_of).to_sql(TABLE_OF, conn, if_exists="replace", index=False)
            print("✓ 0 filas -> creada tabla OF vacía")

    print("\n✅ Carga finalizada. Tablas sin IsGK y con columnas ordenadas.")
except Exception as e:
    print(f"\n❌ Error al subir datos: {e}")
    sys.exit(1)

