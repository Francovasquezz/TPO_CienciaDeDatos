# scripts/build_similarity_model.py
import pandas as pd
import joblib
import json
from pathlib import Path
from sqlalchemy import create_engine, text
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import os
from dotenv import load_dotenv

# Carga las variables de entorno (DB_URL)
load_dotenv()

# --- 1. CONFIGURACIÓN ---
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(exist_ok=True)  # Crea la carpeta /models si no existe

# Define las columnas que usarás para medir la "similitud"
# Asegúrate de que existan en tu tabla 'player_season_stats'
FEATURE_COLUMNS = [
    'minutes', 'games', 'starts', 'goals', 'assists', 'xg', 
    'xa', 'shots', 'key_passes', 'yellow', 'red', 'tkl', 'int' # Agrega más si quieres
]

# Filtro para excluir jugadores con pocos datos
MIN_MINUTES_PLAYED = 500

# --- 2. CONEXIÓN Y EXTRACCIÓN DE DATOS ---
print("Conectando a la base de datos...")

# Carga las variables individuales desde .env
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_name = os.getenv("DB_NAME")

# Verifica que todas las variables existan
if not all([db_user, db_password, db_host, db_port, db_name]):
    raise ValueError("Faltan una o más variables de entorno de la BD (DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME)")

# Construye la URL de conexión para PostgreSQL
db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

engine = create_engine(db_url)

# Lee todas las estadísticas de la temporada que te interesa
# (Ajusta la temporada si es necesario)
query = text("""
    SELECT player_uuid, competition_id, season_id, team_id, position, 
           {} 
    FROM player_season_stats
    WHERE season_id = '2024'
""".format(", ".join(FEATURE_COLUMNS)))

with engine.connect() as conn:
    df = pd.read_sql(query, conn)

print(f"Se obtuvieron {len(df)} registros de jugadores.")

# --- 3. LIMPIEZA Y PREPARACIÓN ---

# Filtra jugadores con pocos minutos
df_filtered = df[df['minutes'] >= MIN_MINUTES_PLAYED].copy()

# Maneja valores nulos (ej: rellenar con 0)
df_filtered[FEATURE_COLUMNS] = df_filtered[FEATURE_COLUMNS].fillna(0)

# Separa las features (X) y el índice (UUIDs)
features = df_filtered[FEATURE_COLUMNS]
player_index = df_filtered['player_uuid'].tolist() # El índice de jugadores

print(f"Se procesarán {len(features)} jugadores (después de filtrar).")

# --- 4. ESCALADO (StandardScaler) ---
print("Escalando features...")
scaler = StandardScaler()
features_scaled = scaler.fit_transform(features)

# Guarda el scaler
joblib.dump(scaler, MODEL_DIR / "scaler.joblib")
print(f"Scaler guardado en {MODEL_DIR / 'scaler.joblib'}")

# --- 5. ENTRENAMIENTO (NearestNeighbors) ---
print("Entrenando modelo K-NN...")
# Usamos 'euclidean' para distancia matemática
# 'cosine' es bueno si quieres similitud "proporcional" sin importar el volumen
model = NearestNeighbors(n_neighbors=10, metric='euclidean') 
model.fit(features_scaled)

# Guarda el modelo K-NN
joblib.dump(model, MODEL_DIR / "knn_model.joblib")
print(f"Modelo K-NN guardado en {MODEL_DIR / 'knn_model.joblib'}")

# --- 6. GUARDAR ÍNDICE Y MATRIZ ---

# Guarda el índice de jugadores (la lista de UUIDs)
with open(MODEL_DIR / "player_index.json", 'w') as f:
    json.dump(player_index, f)
print(f"Índice de jugadores guardado en {MODEL_DIR / 'player_index.json'}")

# Guarda la matriz de features escaladas (para consultas rápidas)
joblib.dump(features_scaled, MODEL_DIR / "features_matrix.joblib")
print(f"Matriz de features guardada en {MODEL_DIR / 'features_matrix.joblib'}")

print("\n✅ ¡Artefactos del modelo creados exitosamente!")