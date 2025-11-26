# backend/app.py
import logging
import os
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware # ⬅️ 1. NUEVO IMPORT
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
from .value import MarketValueService 

from .db import get_db, SessionLocal
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

try:
    value_service = MarketValueService()
    log.info("Servicio de oportunidades de mercado cargado exitosamente.")
except Exception as e:
    log.error(f"Error: No se pudo cargar el MarketValueService: {e}")
    value_service = None 

app = FastAPI(
    title="TPO Futbol API",
    description="API para búsqueda de jugadores y similitud."
)

# ⬅️ 2. CONFIGURACIÓN DE CORS (Permite que el frontend hable con el backend)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"], # Permite solo a tu frontend local
    allow_credentials=True,
    allow_methods=["*"], # Permite todos los métodos (GET, POST, etc.)
    allow_headers=["*"], # Permite todos los headers
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
    player_uuid: str, 
    db: Session = Depends(get_db)
):
    """
    Obtiene los detalles de UN jugador (usa 'v_players_union_with_sort')
    Devuelve TODAS las temporadas disponibles para ese jugador.
    """
    try:
        sql = text("""
            SELECT * FROM v_players_union_with_sort 
            WHERE player_id = :uuid
            ORDER BY season_code DESC
        """) 
        
        stats = db.execute(sql, {"uuid": player_uuid}).mappings().all()
        
        if not stats:
            raise HTTPException(status_code=404, detail="Estadísticas no encontradas")
        
        return stats
    except Exception as e:
        log.error(f"Error en get_player_details: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/player/{player_uuid}/similar")
def get_similar_players(
    player_uuid: str, 
    n: int = 5,
):
    """
    Encuentra jugadores similares.
    """
    if n > 20:
        raise HTTPException(status_code=400, detail="No se pueden pedir más de 20 similares")
        
    try:
        similar_players = similarity_service.find_similar_players(
            target_player_uuid=player_uuid,
            n_similar=n
        )
        return similar_players
    except Exception as e:
        log.error(f"Error en get_similar_players: {e}")
        if "no encontrado en el índice" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=500, detail=f"Error interno: {e}")
    

@app.get("/leagues")
def get_leagues(db: Session = Depends(get_db)):
    try:
        sql = text("SELECT league_name FROM v_leagues ORDER BY league_name")
        result = db.execute(sql)
        return [row['league_name'] for row in result.mappings().all()]
    except Exception as e:
        log.error(f"Error en get_leagues: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/leagues/{league_name}/clubs")
def get_clubs_by_league(
    league_name: str,
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            SELECT team_name 
            FROM v_clubs_by_league 
            WHERE league_name ILIKE :league
            ORDER BY team_name
        """)
        result = db.execute(sql, {"league": league_name})
        clubs = [row['team_name'] for row in result.mappings().all()]
        if not clubs:
            log.warning(f"No se encontraron clubes para la liga: {league_name}")
        return clubs
    except Exception as e:
        log.error(f"Error en get_clubs_by_league: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/clubs/{club_name}/players")
def get_players_by_club(
    club_name: str,
    db: Session = Depends(get_db)
):
    try:
        sql = text("""
            SELECT DISTINCT 
                player_id AS player_uuid, 
                player_name AS full_name, 
                Pos AS primary_position 
            FROM v_players_union_with_sort 
            WHERE club ILIKE :club_name 
            ORDER BY player_name
        """)
        result = db.execute(sql, {"club_name": club_name})
        players = result.mappings().all()
        if not players:
            log.warning(f"No se encontraron jugadores para el club: {club_name}")
        return players
    except Exception as e:
        log.error(f"Error en get_players_by_club: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/market-opportunities")
def get_market_opportunities(
    limit: int = 50
):
    if not value_service:
        raise HTTPException(
            status_code=503, 
            detail="El servicio de oportunidades de mercado no está disponible."
        )
    if limit > 200: 
        limit = 200
    try:
        players = value_service.get_opportunities(limit=limit)
        return players
    except Exception as e:
        log.error(f"Error en get_market_opportunities: {e}")
        raise HTTPException(status_code=500, detail=str(e))