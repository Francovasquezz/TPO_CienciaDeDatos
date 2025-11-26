# backend/similarity.py
import joblib
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy import text
import numpy as np
import logging
import os
import json

MODEL_DIR = Path("models")

class SimilarityService:
    def __init__(self, db_session_factory: sessionmaker):
        logging.info("Cargando artefactos del modelo de similitud...")
        
        self.db_session_factory = db_session_factory
        
        try:
            # --- 1. CARGA DE MODELOS DE JUGADORES DE CAMPO (FIELD) ---
            self.field_scaler = joblib.load(MODEL_DIR / "field_scaler.joblib")
            self.field_model = joblib.load(MODEL_DIR / "field_knn_model.joblib")
            self.field_features_matrix = joblib.load(MODEL_DIR / "field_features_matrix.joblib")
            
            with open(MODEL_DIR / "field_player_index.json", "r") as f:
                self.field_player_index = json.load(f) # Lista de IDs (strings o ints)

            # --- 2. CARGA DE MODELOS DE ARQUEROS (GK) ---
            self.gk_scaler = joblib.load(MODEL_DIR / "gk_scaler.joblib")
            self.gk_model = joblib.load(MODEL_DIR / "gk_knn_model.joblib")
            self.gk_features_matrix = joblib.load(MODEL_DIR / "gk_features_matrix.joblib")
            
            with open(MODEL_DIR / "gk_player_index.json", "r") as f:
                self.gk_player_index = json.load(f)

            logging.info(f"✅ Artefactos cargados. Field: {len(self.field_player_index)}, GK: {len(self.gk_player_index)}")

        except FileNotFoundError as e:
            logging.error(f"Error: No se encontró un archivo del modelo: {e}")
            logging.error("Asegúrate de haber ejecutado los scripts de entrenamiento (build_similarity_model) correctamente.")
            raise
        except Exception as e:
            logging.error(f"Error al cargar los artefactos: {e}")
            raise
        
    def find_similar_players(self, target_player_uuid: str, n_similar: int = 5):
        """
        Busca jugadores similares. Detecta automáticamente si es jugador de campo o arquero
        basándose en qué índice se encuentra el ID.
        """
        target_uuid_str = str(target_player_uuid)
        
        # Variables para decidir qué modelo usar
        model = None
        features_matrix = None
        player_index = None
        idx = -1

        # 1. Buscar en índice de CAMPO
        if target_uuid_str in self.field_player_index:
            idx = self.field_player_index.index(target_uuid_str)
            model = self.field_model
            features_matrix = self.field_features_matrix
            player_index = self.field_player_index
            logging.info(f"Jugador {target_uuid_str} identificado como JUGADOR DE CAMPO.")

        # 2. Si no, buscar en índice de ARQUEROS
        elif target_uuid_str in self.gk_player_index:
            idx = self.gk_player_index.index(target_uuid_str)
            model = self.gk_model
            features_matrix = self.gk_features_matrix
            player_index = self.gk_player_index
            logging.info(f"Jugador {target_uuid_str} identificado como ARQUERO.")
            
        else:
            raise Exception(f"Jugador {target_player_uuid} no encontrado en ninguno de los índices (Field o GK)")

        # Calcular vecinos más cercanos
        try:
            target_features = features_matrix[idx].reshape(1, -1)
            # n_neighbors = n_similar + 1 porque el primero siempre es el mismo jugador
            distances, indices = model.kneighbors(target_features, n_neighbors=n_similar + 1)
            
            similar_indices = indices[0][1:] # Excluir el propio jugador (índice 0)
            similar_uuids = [player_index[i] for i in similar_indices]
            
            logging.info(f"Encontrados {len(similar_uuids)} similares: {similar_uuids}")

            with self.db_session_factory() as db:
                return self._get_details_for_uuids(db, similar_uuids)
                
        except Exception as e:
            logging.error(f"Error calculando vecinos: {e}")
            raise

    def _get_details_for_uuids(self, db: Session, uuids: list):
        """
        Obtiene los detalles de una lista de UUIDs.
        """
        sql = text("""
            SELECT 
                player_id AS player_uuid,
                player_name AS full_name,
                pos AS primary_position, 
                season_code AS season_id,
                league_name,
                club AS team_name,
                latest_mv_eur AS value_eur,
                "Gls",
                "Ast",
                "xG",
                "Tkl"
            FROM 
                v_players_union_with_sort
            WHERE 
                player_id = ANY(:uuids)
        """)
        
        try:
            # Asegurar que sean enteros para la query SQL si tu DB usa integer IDs
            uuids_as_int = [int(uid) for uid in uuids]
            
            result = db.execute(sql, {"uuids": uuids_as_int})
            return result.mappings().all()
        except Exception as e:
            logging.error(f"Error al consultar detalles de UUIDs en la BD: {e}")
            return []