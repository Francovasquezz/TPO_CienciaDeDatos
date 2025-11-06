# backend/similarity.py (Nuevo)
import joblib
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import text
import numpy as np

MODEL_DIR = Path("models")

class SimilarityService:
    def __init__(self, db_session_factory):
        # 1. Cargar artefactos
        self.scaler = joblib.load(MODEL_DIR / "scaler.joblib")
        self.model = joblib.load(MODEL_DIR / "knn_model.joblib")
        
        # Cargar el índice de jugadores (debería ser una lista de UUIDs)
        self.player_index = pd.read_json(MODEL_DIR / "player_index.json")[0].tolist()

        # 2. Cargar la matriz de features (datos escalados)
        # Idealmente, el script de "entrenamiento" también guarda esta matriz
        self.features_matrix = joblib.load(MODEL_DIR / "features_matrix.joblib")

        self.db_session_factory = db_session_factory


    def find_similar_players(self, target_player_uuid: str, n_similar: int = 5):
        try:
            # 1. Encontrar el índice del jugador target
            target_idx = self.player_index.index(target_player_uuid)
        except ValueError:
            raise Exception(f"Jugador {target_player_uuid} no encontrado en el índice del modelo")

        # 2. Obtener el vector de features del jugador
        target_features = self.features_matrix[target_idx].reshape(1, -1)

        # 3. Usar el modelo K-NN para encontrar los N+1 más cercanos
        # (El +1 es porque el más cercano será él mismo)
        distances, indices = self.model.kneighbors(target_features, n_neighbors=n_similar + 1)

        # 4. Obtener los UUIDs de los jugadores similares (saltando el primero)
        similar_indices = indices[0][1:]
        similar_uuids = [self.player_index[i] for i in similar_indices]

        # 5. Buscar los datos completos de estos jugadores en la BD
        with self.db_session_factory() as db:
            return self._get_details_for_uuids(db, similar_uuids)

    def _get_details_for_uuids(self, db: Session, uuids: list):
        # Esta función debe consultar la BD (tablas 'players', 'player_season_stats', 'market_values')
        # para devolver los detalles completos de los jugadores en la lista 'uuids'.
        # (Similar a get_player_details, pero para una lista)
        
        # OJO: SQLAlchemy no acepta listas en text() directamente para 'IN'
        # Esta es una forma segura de hacerlo
        sql = text("""
            SELECT p.full_name, p.player_uuid, mv.value_eur, pss.goals, pss.assists, pss.xg, pss.minutes
            FROM players p
            LEFT JOIN player_season_stats pss ON p.player_uuid = pss.player_uuid AND pss.season_id = '2024'
            LEFT JOIN (
                SELECT player_uuid, value_eur FROM market_values
                WHERE (player_uuid, as_of_date) IN (
                    SELECT player_uuid, MAX(as_of_date)
                    FROM market_values
                    GROUP BY player_uuid
                )
            ) mv ON p.player_uuid = mv.player_uuid
            WHERE p.player_uuid = ANY(:uuids)
        """)
        
        result = db.execute(sql, {"uuids": uuids})
        return result.mappings().all()

# --- En backend/app.py, inicializa este servicio ---
# from .db import SessionLocal
# from .similarity import SimilarityService
# 
# similarity_service = SimilarityService(db_session_factory=SessionLocal)