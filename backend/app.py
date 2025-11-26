# backend/app.py
import logging
import os
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List

# Importamos la configuración optimizada de DB
from db import get_db, SessionLocal
from similarity import SimilarityService
from value import MarketValueService 

# Configura logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- Carga de Modelos ---
log.info("🚀 Iniciando API y cargando modelos de ML...")

try:
    similarity_service = SimilarityService(db_session_factory=SessionLocal)
    log.info("✅ Servicio de similitud cargado exitosamente.")
except Exception as e:
    log.error(f"❌ Error fatal cargando SimilarityService: {e}")
    # En producción podrías querer no detener la API, pero para este TP es mejor saber si falla.
    similarity_service = None

try:
    value_service = MarketValueService()
    log.info("✅ Servicio de oportunidades de mercado cargado exitosamente.")
except Exception as e:
    log.error(f"⚠️ Error cargando MarketValueService (Oportunidades deshabilitadas): {e}")
    value_service = None 

app = FastAPI(
    title="TPO Futbol API",
    description="API para búsqueda de jugadores, similitud y oportunidades de mercado."
)

# --- Configuración de CORS ---
# Permite que tu frontend (Vercel/Localhost) consuma esta API sin bloqueos.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Para desarrollo está bien "*". En prod idealmente pon tu dominio de Vercel.
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Endpoints ---

@app.get("/")
def read_root():
    return {"status": "online", "message": "API de TPO Ciencia de Datos funcionando 🚀"}

@app.get("/players/search")
def search_players(
    query: str,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """
    Busca jugadores por nombre parcial.
    Optimización: La BD responderá rápido gracias al Connection Pooling.
    """
    try:
        # Nota: ILIKE con % al inicio (%query%) no usa índices estándar eficientemente,
        # pero con el pooling la latencia de conexión desaparece, mejorando la UX.
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
    Obtiene el historial completo de un jugador.
    """
    try:
        sql = text("""
            SELECT * FROM v_players_union_with_sort 
            WHERE player_id = :uuid
            ORDER BY season_code DESC
        """) 
        
        stats = db.execute(sql, {"uuid": player_uuid}).mappings().all()
        
        if not stats:
            raise HTTPException(status_code=404, detail="Jugador no encontrado o sin estadísticas.")
        
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
    Encuentra jugadores similares usando el modelo KNN cargado en memoria.
    """
    if similarity_service is None:
        raise HTTPException(status_code=503, detail="Servicio de similitud no disponible.")

    if n > 20:
        raise HTTPException(status_code=400, detail="Máximo 20 jugadores similares.")
        
    try:
        similar_players = similarity_service.find_similar_players(
            target_player_uuid=player_uuid,
            n_similar=n
        )
        return similar_players
    except Exception as e:
        log.error(f"Error en get_similar_players: {e}")
        if "no encontrado" in str(e).lower():
            raise HTTPException(status_code=404, detail="Jugador no encontrado en los modelos de IA.")
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
        return result.mappings().all()
    except Exception as e:
        log.error(f"Error en get_players_by_club: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/market-opportunities")
def get_market_opportunities(
    limit: int = 50
):
    """
    Devuelve jugadores subvaluados.
    """
    if not value_service:
        raise HTTPException(
            status_code=503, 
            detail="El servicio de oportunidades de mercado no está disponible."
        )
    
    # Límite de seguridad
    if limit > 200: 
        limit = 200

    try:
        players = value_service.get_opportunities(limit=limit)
        return players
    except Exception as e:
        log.error(f"Error en get_market_opportunities: {e}")
        raise HTTPException(status_code=500, detail=str(e))