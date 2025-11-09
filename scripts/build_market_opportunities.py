# scripts/build_market_opportunities.py (CORREGIDO v3)

import pandas as pd
import joblib
import json
import logging
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text # <-- Import text
from sqlalchemy.engine import URL
from dotenv import load_dotenv
import os
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# --- Configuración ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger()

load_dotenv(override=True)

MODEL_DIR = Path("models")

# --- Lógica de conexión ---
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

log.info("Conectando a la base de datos...")
log.info(f"→ Conexion: {db_url.render_as_string(hide_password=True)}")

def get_db_connection():
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
    )

def fetch_player_data(engine):
    """
    Obtiene todos los datos de la vista para compararlos
    """
    log.info("Obteniendo todos los datos de v_players_union_with_sort...")
    
    # --- INICIO DE LA CORRECCIÓN ---
    # 1. Cambiamos "Min" por "MatchesPlayed"
    #    (Asegúrate de que 'v_players_union_with_sort' tiene la columna "MatchesPlayed")
    sql = text("""
    SELECT 
        player_id, 
        player_name, 
        pos,
        club,
        league_name,
        season_code,
        latest_mv_eur,
        age, 
        "MatchesPlayed" 
    FROM 
        v_players_union_with_sort
    WHERE
        pos != 'GK'
    """)
    # --- FIN DE LA CORRECCIÓN ---
    
    df = pd.read_sql(sql, engine)
    
    # --- INICIO DE LA CORRECCIÓN ---
    # 2. Cambiamos el filtro de "Min" a "MatchesPlayed"
    #    Usamos 5 partidos como filtro mínimo, en lugar de 500 minutos
    log.info(f"Datos crudos leídos: {len(df)} filas.")
    df = df[df['MatchesPlayed'] >= 5].copy()
    # --- FIN DE LA CORRECCIÓN ---
    
    df = df.dropna(subset=['latest_mv_eur'])
    df = df[df['latest_mv_eur'] > 100000] # Solo jugadores con valor > 100k
    
    df['player_id'] = df['player_id'].astype(str)
    
    log.info(f"Datos de BD filtrados (>=5 partidos, >100k valor): {len(df)} jugadores.")
    return df

def main():
    log.info("Iniciando construcción de modelo de valor...")
    
    try:
        log.info("Cargando artefactos base (scaler, matrix, index)...")
        scaler = joblib.load(MODEL_DIR / "field_scaler.joblib")
        features_matrix = joblib.load(MODEL_DIR / "field_features_matrix.joblib")
        
        with open(MODEL_DIR / "field_player_index.json", "r") as f:
            player_index = json.load(f)
        
        log.info(f"Artefactos cargados: {len(player_index)} jugadores en el índice.")

        engine = get_db_connection()
        players_df = fetch_player_data(engine)
        
        index_df = pd.DataFrame({
            'player_id': player_index,
            'matrix_index': range(len(player_index))
        })
        
        merged_data = index_df.merge(players_df, on='player_id', how='inner')
        
        if merged_data.empty:
            log.error("Error: El 'merge' entre el índice del modelo y los datos de la BD no produjo resultados.")
            log.error("Posibles causas: ¿Los 'player_id' en 'v_players_union_with_sort' coinciden con los del 'field_player_index.json'?")
            log.error(f"IDs en el índice (muestra): {player_index[:5]}")
            log.error(f"IDs en la BD (muestra): {players_df['player_id'].head(5).tolist()}")
            raise RuntimeError("No se pudieron alinear los datos del modelo con la BD.")
        
        X_indices = merged_data['matrix_index'].values
        X = features_matrix[X_indices]
        y = np.log1p(merged_data['latest_mv_eur'])
        
        log.info(f"Datos listos para entrenar. X shape: {X.shape}, y shape: {y.shape}")

        log.info("Entrenando RandomForestRegressor para predecir valor...")
        value_model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        value_model.fit(X, y)
        
        y_pred_log = value_model.predict(X)
        rmse = np.sqrt(mean_squared_error(y, y_pred_log))
        log.info(f"RMSE (log-scale) del modelo de valor: {rmse:.4f}")
        
        joblib.dump(value_model, MODEL_DIR / "field_value_model.joblib")
        log.info("Modelo de valor guardado en 'models/field_value_model.joblib'")

        log.info("Calculando valor predicho vs valor real...")
        predicted_value_eur = np.expm1(y_pred_log)
        
        results_df = merged_data.copy()
        results_df['predicted_value_eur'] = predicted_value_eur
        results_df['actual_value_eur'] = merged_data['latest_mv_eur']
        results_df['value_diff_eur'] = results_df['predicted_value_eur'] - results_df['actual_value_eur']
        results_df['value_ratio'] = results_df['predicted_value_eur'] / (results_df['actual_value_eur'] + 1)

        log.info("Filtrando y guardando la lista final...")
        opportunities = results_df[results_df['value_diff_eur'] > 0].copy()
        opportunities = opportunities.sort_values(by='value_diff_eur', ascending=False)
        
        final_cols = [
            'player_id', 'player_name', 'pos', 'club', 'league_name', 'age', 'season_code',
            'actual_value_eur', 'predicted_value_eur', 'value_diff_eur', 'value_ratio',
            'MatchesPlayed' # <-- Agregamos esto al JSON final
        ]
        
        opportunities = opportunities[final_cols].rename(columns={
            'player_id': 'player_uuid',
            'player_name': 'full_name',
            'pos': 'primary_position',
            'club': 'team_name',
            'latest_mv_eur': 'actual_value_eur'
        })
        
        top_200_opportunities = opportunities.head(200)
        
        output_path = MODEL_DIR / "market_opportunities.json"
        top_200_opportunities.to_json(output_path, orient='records', indent=2)
        
        log.info(f"✅ ¡Éxito! Lista de oportunidades de mercado guardada en '{output_path}'")

    except FileNotFoundError as e:
        log.error(f"Error: No se encontró un archivo. Asegúrate de tener los modelos base.")
        log.error(f"Ejecuta 'python scripts/build_similarity_model.py' primero.")
        log.error(e)
    except Exception as e:
        log.error(f"Error inesperado: {e}")
        raise

if __name__ == "__main__":
    main()