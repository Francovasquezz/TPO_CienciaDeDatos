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

# 1) Cargar .env pisando variables previas
load_dotenv(override=True)

MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)

FEATURE_COLUMNS = [
    'minutes', 'games', 'starts', 'goals', 'assists', 'xg',
    'xa', 'shots', 'key_passes', 'yellow', 'red', 'tkl', 'int'
]
MIN_MINUTES_PLAYED = 500

print("Conectando a la base de datos...")

# 2) Tomar campos sueltos (evita URL-encoding)
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

# 3) Construir URL con SQLAlchemy (sin strings manuales)
db_url = URL.create(
    "postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME,
    query={"sslmode": DB_SSLMODE, "connect_timeout": DB_CONNECT_TIMEOUT},
)
# DEBUG opcional: NO imprime la password
print("→ Conexion:", db_url.render_as_string(hide_password=True))

engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=5,
    max_overflow=5,
)

query = text(f"""
    SELECT player_uuid, competition_id, season_id, team_id, position,
           {", ".join(FEATURE_COLUMNS)}
    FROM player_season_stats
    WHERE season_id = '2024'
""")

with engine.begin() as conn:
    # Smoke test
    conn.execute(text("SELECT 1"))
    # Ver quién sos y contra qué DB (para validar user/host reales)
    who = conn.execute(text("SELECT current_user, current_database()")).fetchone()
    print("→ current_user:", who[0], "db:", who[1])

    df = pd.read_sql(query, conn)

print(f"Se obtuvieron {len(df)} registros de jugadores.")

df_filtered = df[df['minutes'] >= MIN_MINUTES_PLAYED].copy()
df_filtered[FEATURE_COLUMNS] = df_filtered[FEATURE_COLUMNS].fillna(0)

features = df_filtered[FEATURE_COLUMNS]
player_index = df_filtered['player_uuid'].tolist()

print(f"Se procesarán {len(features)} jugadores (después de filtrar).")

print("Escalando features...")
scaler = StandardScaler()
features_scaled = scaler.fit_transform(features)
joblib.dump(scaler, MODEL_DIR / "scaler.joblib")

print("Entrenando modelo K-NN...")
model = NearestNeighbors(n_neighbors=10, metric='euclidean')
model.fit(features_scaled)
joblib.dump(model, MODEL_DIR / "knn_model.joblib")

with open(MODEL_DIR / "player_index.json", 'w') as f:
    json.dump(player_index, f)

joblib.dump(features_scaled, MODEL_DIR / "features_matrix.joblib")
print("\n✅ ¡Artefactos del modelo creados exitosamente!")
