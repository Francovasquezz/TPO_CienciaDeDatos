# backend/app.py (CORREGIDO FINAL)

import logging
import os
from fastapi import FastAPI, HTTPException, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

# Importa la configuración de BD (get_db, SessionLocal)
from .db import get_db, SessionLocal
# Importa el servicio de similitud
from .similarity import SimilarityService

# Configura logging básico
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- Carga de Modelos ---
log.info("Iniciando API y cargando modelos...")
try:
    similarity_service = SimilarityService(db_session_factory=SessionLocal)
    log.info("Servicio de similitud cargado exitosamente.")
except Exception as e:
    log.error(f"Error fatal: No se pudo cargar el SimilarityService: {e}")
    raise

app = FastAPI(
    title="TPO Futbol API",
    description="API para búsqueda de jugadores y similitud."
)

# --- Endpoints ---

@app.get("/")
def read_root():
    return {"status": "API de Similitud de Jugadores está en línea"}

@app.get("/players/search")
def search_players(
    query: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """
    Busca jugadores por nombre (usa 'v_players_union_with_sort')
    """
    try:
        # CORREGIDO: Usa 'v_players_union_with_sort' y las columnas 'player_id', 'player_name', 'Pos'
        # Renombramos las columnas en la consulta para que la API sea consistente
        sql = text("""
            SELECT DISTINCT 
                player_id AS player_uuid, 
                player_name AS full_name, 
                Pos AS primary_position 
            FROM v_players_union_with_sort 
            WHERE player_name ILIKE :query 
            LIMIT :limit
        """)
        result = db.execute(sql, {"query": f"%{query}%", "limit": limit})
        return result.mappings().all()
    except Exception as e:
        log.error(f"Error en search_players: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/player/{player_uuid}/details")
def get_player_details(
    player_uuid: str, # <-- 1. Se quitó el parámetro 'season'
    db: Session = Depends(get_db)
):
    """
    Obtiene los detalles de UN jugador (usa 'v_players_union_with_sort')
    Devuelve TODAS las temporadas disponibles para ese jugador.
    """
    try:
        # 2. CORREGIDO: Se quitó el filtro 'season_code'
        sql = text("""
            SELECT * FROM v_players_union_with_sort 
            WHERE player_id = :uuid
            ORDER BY season_code DESC
        """) # <-- Se agregó un ORDER BY para que la más reciente venga primero
        
        # 3. Se quita 'season' de los parámetros
        # 4. Usamos .all() para devolver una lista con todas sus temporadas
        stats = db.execute(sql, {"uuid": player_uuid}).mappings().all()
        
        if not stats:
            raise HTTPException(status_code=404, detail="Estadísticas no encontradas")
        
        return stats
    except Exception as e:
        log.error(f"Error en get_player_details: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/player/{player_uuid}/similar")
def get_similar_players(
    player_uuid: str, # Este es el 'player_id'
    n: int = 5,
):
    """
    Encuentra jugadores similares.
    """
    if n > 20:
        raise HTTPException(status_code=400, detail="No se pueden pedir más de 20 similares")
        
    try:
        similar_players = similarity_service.find_similar_players(
            target_player_uuid=player_uuid, # Pasamos el ID al servicio
            n_similar=n
        )
        return similar_players
    except Exception as e:
        log.error(f"Error en get_similar_players: {e}")
        if "no encontrado en el índice" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")