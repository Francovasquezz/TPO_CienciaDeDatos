# scripts/build_market_opportunities.py (SOPORTE GK + FIELD)

import pandas as pd
import joblib
import json
import logging
import numpy as np
from pathlib import Path
from sqlalchemy import create_engine, text
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

def get_db_connection():
    return create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=5,
    )

def fetch_all_data(engine):
    """
    Obtiene TODOS los jugadores (Field + GK) que cumplan el criterio mínimo.
    """
    log.info("Obteniendo datos de v_players_union_with_sort (Field + GK)...")
    
    # NOTA: Quitamos "pos != 'GK'" para traer a todos.
    sql = text("""
    SELECT 
        player_id, 
        player_name, 
        pos,
        club,
        league_name,
        season_code,
        latest_mv_eur,
        "Age", 
        "MatchesPlayed" 
    FROM 
        v_players_union_with_sort
    WHERE
        "MatchesPlayed" >= :min_matches
        AND latest_mv_eur > 100000
    """)
    
    # Filtro de 5 partidos mínimo
    df = pd.read_sql(sql, engine, params={"min_matches": 5})
    df = df.dropna(subset=['latest_mv_eur'])
    df['player_id'] = df['player_id'].astype(str)
    
    log.info(f"Total jugadores recuperados (>=5 partidos): {len(df)}")
    return df

def train_and_predict(player_type, df_subset, features_matrix, player_index):
    """
    Función genérica para entrenar modelo y predecir oportunidades.
    player_type: 'field' o 'gk'
    """
    if df_subset.empty:
        log.warning(f"No hay jugadores para el tipo {player_type}.")
        return pd.DataFrame()

    log.info(f"--- Procesando {player_type.upper()} ({len(df_subset)} jugadores) ---")

    # 1. Alinear datos con el índice del modelo (Matrix)
    index_df = pd.DataFrame({
        'player_id': player_index,
        'matrix_index': range(len(player_index))
    })
    
    # Merge inner: solo jugadores que tengan datos en la DB Y en la matriz de features
    merged_data = index_df.merge(df_subset, on='player_id', how='inner')
    
    if merged_data.empty:
        log.warning(f"No se pudieron cruzar datos de DB con el índice de {player_type}.")
        return pd.DataFrame()

    # 2. Preparar X e y
    X_indices = merged_data['matrix_index'].values
    X = features_matrix[X_indices]
    y = np.log1p(merged_data['latest_mv_eur']) # Log del valor real
    
    # 3. Entrenar Modelo de Valor (Random Forest)
    #    Entrenamos un modelo específico para este tipo de jugador (GK o Field)
    model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    model.fit(X, y)
    
    # Guardamos el modelo entrenado
    model_path = MODEL_DIR / f"{player_type}_value_model.joblib"
    joblib.dump(model, model_path)
    log.info(f"Modelo {player_type} guardado en {model_path}")
    
    # 4. Predecir
    y_pred_log = model.predict(X)
    rmse = np.sqrt(mean_squared_error(y, y_pred_log))
    log.info(f"RMSE (log) para {player_type}: {rmse:.4f}")

    predicted_value = np.expm1(y_pred_log)
    
    # 5. Construir DataFrame de resultados
    results = merged_data.copy()
    results['predicted_value_eur'] = predicted_value
    results['actual_value_eur'] = merged_data['latest_mv_eur']
    results['value_diff_eur'] = results['predicted_value_eur'] - results['actual_value_eur']
    results['value_ratio'] = results['predicted_value_eur'] / (results['actual_value_eur'] + 1)
    
    # Filtramos solo oportunidades positivas
    opportunities = results[results['value_diff_eur'] > 0].copy()
    return opportunities

def main():
    log.info("Iniciando construcción de oportunidades de mercado (Híbrido)...")
    
    try:
        engine = get_db_connection()
        all_players = fetch_all_data(engine)
        
        # --- CARGAR ARTEFACTOS ---
        # Field
        log.info("Cargando artefactos FIELD...")
        field_matrix = joblib.load(MODEL_DIR / "field_features_matrix.joblib")
        with open(MODEL_DIR / "field_player_index.json", "r") as f:
            field_index = json.load(f)
            
        # GK (Arqueros)
        log.info("Cargando artefactos GK...")
        gk_matrix = joblib.load(MODEL_DIR / "gk_features_matrix.joblib")
        with open(MODEL_DIR / "gk_player_index.json", "r") as f:
            gk_index = json.load(f)

        # --- SEPARAR DATASET ---
        # Un jugador es GK si su posición contiene 'GK'
        # Ajusta esto según cómo guardes la posición en tu DB ('GK', 'Goalkeeper', etc.)
        mask_gk = all_players['pos'].str.contains('GK', case=False, na=False)
        
        df_gk = all_players[mask_gk].copy()
        df_field = all_players[~mask_gk].copy()
        
        # --- EJECUTAR PIPELINES ---
        opp_field = train_and_predict('field', df_field, field_matrix, field_index)
        opp_gk = train_and_predict('gk', df_gk, gk_matrix, gk_index)
        
        # --- UNIFICAR Y GUARDAR ---
        log.info(f"Oportunidades encontradas: Field={len(opp_field)}, GK={len(opp_gk)}")
        
        final_df = pd.concat([opp_field, opp_gk], ignore_index=True)
        final_df = final_df.sort_values(by='value_diff_eur', ascending=False)
        
        final_cols = [
            'player_id', 'player_name', 'pos', 'club', 'league_name', 'Age', 'season_code',
            'actual_value_eur', 'predicted_value_eur', 'value_diff_eur', 'value_ratio',
            'MatchesPlayed'
        ]
        
        # Seleccionar y renombrar para el JSON final
        export_df = final_df[final_cols].rename(columns={
            'player_id': 'player_uuid',
            'player_name': 'full_name',
            'pos': 'primary_position',
            'club': 'team_name',
            'latest_mv_eur': 'actual_value_eur',
            'Age': 'age'
        })
        
        # Guardamos top 200 (mezclados)
        top_200 = export_df.head(200)
        
        output_path = MODEL_DIR / "market_opportunities.json"
        top_200.to_json(output_path, orient='records', indent=2)
        
        log.info(f"✅ ¡Éxito! Lista combinada guardada en '{output_path}'")

    except Exception as e:
        log.error(f"Error inesperado: {e}")
        raise

if __name__ == "__main__":
    main()