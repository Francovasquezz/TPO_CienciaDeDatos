# backend/similarity.py (COMPLETO Y CORREGIDO FINAL - SIN FILTRO DE SEASON)

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
        
        # --- ESTE ES EL BLOQUE CORREGIDO ---
        try:
            # Carga los artefactos de JUGADORES DE CAMPO
            self.scaler = joblib.load(MODEL_DIR / "field_scaler.joblib")
            self.model = joblib.load(MODEL_DIR / "field_knn_model.joblib")
            self.features_matrix = joblib.load(MODEL_DIR / "field_features_matrix.joblib")
            
            # Carga el índice de jugadores (que es un JSON)
            with open(MODEL_DIR / "field_player_index.json", "r") as f:
                self.player_index = json.load(f)
            
            self.db_session_factory = db_session_factory
            logging.info(f"✅ Artefactos cargados. {len(self.player_index)} jugadores indexados.")

        except FileNotFoundError as e:
            logging.error(f"Error: No se encontró el archivo del modelo: {e}")
            logging.error("Asegúrate de haber ejecutado 'python scripts/build_similarity_model.py' primero.")
            raise
        except Exception as e:
            logging.error(f"Error al cargar los artefactos: {e}")
            raise
        # --- FIN DEL BLOQUE CORREGIDO ---
        
    def find_similar_players(self, target_player_uuid: str, n_similar: int = 5):
        try:
            target_idx = self.player_index.index(str(target_player_uuid))
        except ValueError:
            raise Exception(f"Jugador {target_player_uuid} no encontrado en el índice del modelo")
        except Exception as e:
            raise

        target_features = self.features_matrix[target_idx].reshape(1, -1)
        distances, indices = self.model.kneighbors(target_features, n_neighbors=n_similar + 1)
        similar_indices = indices[0][1:]
        similar_uuids = [self.player_index[i] for i in similar_indices]
        
        logging.info(f"Jugadores similares a {target_player_uuid}: {similar_uuids}")

        with self.db_session_factory() as db:
            return self._get_details_for_uuids(db, similar_uuids) 

    def _get_details_for_uuids(self, db: Session, uuids: list):
        """
        Obtiene los detalles de una lista de UUIDs.
        CORREGIDO: Se quitó el filtro 'AND season_code = :season'
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
        """) # <-- SE QUITÓ EL FILTRO DE SEASON_CODE
        
        try:
            uuids_as_int = [int(uid) for uid in uuids]
            
            # Pasamos la lista de integers a la consulta (sin season)
            result = db.execute(sql, {"uuids": uuids_as_int})
            return result.mappings().all()
        except Exception as e:
            logging.error(f"Error al consultar detalles de UUIDs en la BD: {e}")
            logging.warning("Asegúrate que la vista 'v_players_union_with_sort' exista.")
            return []