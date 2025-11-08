# scripts/build_similarity_model.py
import os
import json
import joblib
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
from dotenv import load_dotenv

# ======================
# 0) Config & helpers
# ======================
load_dotenv(override=True)

MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

TARGET_SEASON = os.getenv("TARGET_SEASON", "2025")
MIN_MINUTES_PLAYED = int(os.getenv("MIN_MINUTES_PLAYED", "500"))
N_NEIGHBORS = int(os.getenv("N_NEIGHBORS", "10"))

# Cols candidatas para CAMPO (según tu tabla public.field_players_all)
FP_FEATURE_CANDIDATES = [
    "Min", "MatchesPlayed", "Gls", "Ast", "xG", "xAG",
    "Shots", "KeyPasses", "CrdY", "CrdR", "Tkl", "Int"
]
FP_ID_CANDIDATES = ["player_id", "player_uuid"]

# Cols candidatas para ARQUEROS (según goalkeepers_all)
GK_FEATURE_CANDIDATES = [
    "GK_Min", "GK_MatchesPlayed", "GK_SoTA", "GK_Saves", "GK_SavePct",
    "GK_GA", "GK_CS", "GK_PSxG", "GK_PSxG_PlusMinus", "GK_PSxG_PlusMinus_per90",
    "GK_OPA90", "GK_PassCmp", "GK_PassAtt", "GK_PassCmpPct"
]
GK_ID_CANDIDATES = ["player_id", "player_uuid"]

SEASON_COL_CANDIDATES = ["season_code", "season_id", "season"]

def select_existing(cols, candidates):
    return [c for c in candidates if c in cols]

def q(ident: str) -> str:
    """Cita SIEMPRE el identificador como "Ident". Evita problemas de mayúsculas."""
    ident = ident.replace('"', '""')
    return f'"{ident}"'

def find_table(conn, like_patterns):
    cond = " OR ".join([f"table_name ILIKE :p{i}" for i in range(len(like_patterns))])
    sql = f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema')
          AND ({cond})
        ORDER BY CASE WHEN table_schema='public' THEN 0 ELSE 1 END, table_schema, table_name
        LIMIT 1
    """
    params = {f"p{i}": pat for i, pat in enumerate(like_patterns)}
    row = conn.execute(text(sql), params).fetchone()
    return (row[0], row[1]) if row else None

def read_table_columns(conn, schema, table):
    rows = conn.execute(text("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema=:s AND table_name=:t
    """), {"s": schema, "t": table}).fetchall()
    return [r[0] for r in rows]

def build_and_save_knn(df, feature_cols, player_id_col, model_prefix):
    # Relleno de nulos
    df = df.copy()
    df[feature_cols] = df[feature_cols].fillna(0)

    # Filtrar por minutos si existe 'Min'
    if "Min" in df.columns:
        before = len(df)
        df = df[df["Min"] >= MIN_MINUTES_PLAYED]
        print(f"→ {model_prefix}: filtro Min≥{MIN_MINUTES_PLAYED}: {before} → {len(df)} filas")

    if len(df) == 0:
        raise RuntimeError(f"{model_prefix}: sin filas para entrenar (revisar filtros/temporada).")

    X = df[feature_cols]
    player_index = df[player_id_col].astype(str).tolist()

    print(f"→ {model_prefix}: escalando {X.shape[1]} features para {X.shape[0]} jugadores...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    print(f"→ {model_prefix}: entrenando KNN (n_neighbors={N_NEIGHBORS})...")
    model = NearestNeighbors(n_neighbors=N_NEIGHBORS, metric="euclidean")
    model.fit(X_scaled)

    joblib.dump(scaler, MODEL_DIR / f"{model_prefix}_scaler.joblib")
    joblib.dump(model, MODEL_DIR / f"{model_prefix}_knn_model.joblib")
    with open(MODEL_DIR / f"{model_prefix}_player_index.json", "w") as f:
        json.dump(player_index, f)
    joblib.dump(X_scaled, MODEL_DIR / f"{model_prefix}_features_matrix.joblib")

    print(f"✅ {model_prefix}: artefactos guardados en {MODEL_DIR}\n")

# ======================
# 1) Conexión a la DB
# ======================
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")
DB_CONNECT_TIMEOUT = os.getenv("DB_CONNECT_TIMEOUT", "10")

for k, v in {
    "DB_HOST": DB_HOST, "DB_PORT": DB_PORT, "DB_NAME": DB_NAME,
    "DB_USER": DB_USER, "DB_PASSWORD": DB_PASSWORD
}.items():
    if not v:
        raise RuntimeError(f"Falta variable de entorno: {k}")

db_url = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    query={"sslmode": DB_SSLMODE, "connect_timeout": DB_CONNECT_TIMEOUT},
)

print("Conectando a la base de datos...")
print("→ Conexion:", db_url.render_as_string(hide_password=True))

engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=5,
    max_overflow=5,
)

# ======================
# 2) Inventario y autodetección
# ======================
with engine.begin() as conn:
    who = conn.execute(text("SELECT current_user, current_database()")).fetchone()
    print("→ current_user:", who[0], "db:", who[1])

    print("\n-- Esquemas visibles --")
    schemas = conn.execute(text("""
        SELECT nspname AS schema
        FROM pg_namespace
        WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema'
        ORDER BY 1
    """)).fetchall()
    print([r[0] for r in schemas])

    print("\n-- Tablas candidatas campo/arquero --")
    all_candidates = conn.execute(text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type='BASE TABLE'
          AND table_schema NOT IN ('pg_catalog','information_schema')
          AND (table_name ILIKE '%player%' OR table_name ILIKE '%season%' OR
               table_name ILIKE '%stats%' OR table_name ILIKE '%goalkeeper%' OR
               table_name ILIKE '%keeper%' OR table_name ILIKE '%gk%')
        ORDER BY 1,2
    """)).fetchall()
    for r in all_candidates:
        print(f"{r[0]}.{r[1]}")

    # Campo
    fp_loc = find_table(conn, [
        "%field_players_all%", "%player_season_stats%", "%players_season%",
        "%player_season%", "%field_players%", "%players_all%", "%players%"
    ])
    if not fp_loc:
        raise RuntimeError("No se encontró una tabla de JUGADORES DE CAMPO.")
    fp_schema, fp_table = fp_loc
    fp_cols = read_table_columns(conn, fp_schema, fp_table)
    print(f"\n→ Tabla CAMPO detectada: {fp_schema}.{fp_table}")
    print("→ Columnas CAMPO:", fp_cols)

    fp_id_cols = select_existing(fp_cols, FP_ID_CANDIDATES)
    if not fp_id_cols:
        raise RuntimeError("Tabla CAMPO sin columna id (player_id/player_uuid).")
    FP_ID = fp_id_cols[0]

    fp_features = select_existing(fp_cols, FP_FEATURE_CANDIDATES)
    if not fp_features:
        raise RuntimeError(f"Tabla CAMPO sin features esperadas. Esperaba alguna de: {FP_FEATURE_CANDIDATES}")
    print("→ Features CAMPO usadas:", fp_features)

    season_cols_fp = select_existing(fp_cols, SEASON_COL_CANDIDATES)
    season_filter_fp = ""
    if season_cols_fp:
        sc = season_cols_fp[0]
        #season_filter_fp = f"WHERE {q(sc)} = :season_val"
        print(f"→ Filtro temporada CAMPO: {sc} = {TARGET_SEASON}")
    else:
        print("⚠️  CAMPO sin columna de temporada; no se filtra.")

    select_list_fp = ", ".join(
        [q(FP_ID), *[q(c) for c in fp_features]] + [q(c) for c in select_existing(fp_cols, SEASON_COL_CANDIDATES)]
    )
    sql_fp = f'SELECT {select_list_fp} FROM {q(fp_schema)}.{q(fp_table)} {season_filter_fp}'
    df_fp = pd.read_sql(text(sql_fp), conn, params={"season_val": TARGET_SEASON} if season_filter_fp else {})
    print(f"\nCAMPO: filas leídas = {len(df_fp)}")

    # GK
    gk_loc = find_table(conn, [
        "%goalkeepers_all%", "%goalkeepers%", "%gk%", "%keeper%"
    ])
    df_gk = None
    GK_ID = None
    gk_features = None

    if gk_loc:
        gk_schema, gk_table = gk_loc
        gk_cols = read_table_columns(conn, gk_schema, gk_table)
        print(f"\n→ Tabla GK detectada: {gk_schema}.{gk_table}")
        print("→ Columnas GK:", gk_cols)

        gk_id_cols = select_existing(gk_cols, GK_ID_CANDIDATES)
        if gk_id_cols:
            GK_ID = gk_id_cols[0]
            gk_features = select_existing(gk_cols, GK_FEATURE_CANDIDATES)
            if gk_features:
                season_cols_gk = select_existing(gk_cols, SEASON_COL_CANDIDATES)
                season_filter_gk = ""
                if season_cols_gk:
                    scg = season_cols_gk[0]
                    #season_filter_gk = f"WHERE {q(scg)} = :season_val"
                    print(f"→ Filtro temporada GK: {scg} = {TARGET_SEASON}")
                else:
                    print("⚠️  GK sin columna de temporada; no se filtra.")

                select_list_gk = ", ".join(
                    [q(GK_ID), *[q(c) for c in gk_features]] + [q(c) for c in select_existing(gk_cols, SEASON_COL_CANDIDATES)]
                )
                sql_gk = f'SELECT {select_list_gk} FROM {q(gk_schema)}.{q(gk_table)} {season_filter_gk}'
                df_gk = pd.read_sql(text(sql_gk), conn, params={"season_val": TARGET_SEASON} if season_filter_gk else {})
                print(f"GK: filas leídas = {len(df_gk)}")
            else:
                print(f"⚠️  GK: no encontré features esperadas. Esperaba alguna de: {GK_FEATURE_CANDIDATES}")
        else:
            print("⚠️  GK: no encontré columna id (player_id/player_uuid).")
    else:
        print("⚠️  No se encontró tabla de ARQUEROS; se entrenará solo CAMPO.")

# ======================
# 3) Entrenamiento CAMPO
# ======================
print("\n=== Entrenando modelo CAMPO ===")
build_and_save_knn(df_fp, fp_features, FP_ID, model_prefix="field")

# ======================
# 4) Entrenamiento GK (si aplica)
# ======================
if isinstance(df_gk, pd.DataFrame) and GK_ID and gk_features:
    print("=== Entrenando modelo GK ===")
    build_and_save_knn(df_gk, gk_features, GK_ID, model_prefix="gk")
else:
    print("ℹ️  No se entrenó modelo GK (tabla/cols no detectadas).")

print("🎉 Listo. Artefactos en ./models")
