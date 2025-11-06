# backend/app.py (Modificado)

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
import pandas as pd
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import text

# Importa el engine de tu archivo db.py
from .db import engine 

app = FastAPI(title="TPO Futbol API")

# --- Configuración de la Sesión de Base de Datos ---
# Crea una sesión para interactuar con la DB
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependencia de FastAPI: Inyecta una sesión de BD en tus endpoints
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Modelos Pydantic (ya los tienes) ---
class Team(BaseModel):
    team_id: str
    name: str
    league: str
    season: int

class Player(BaseModel):
    player_uuid: str
    full_name: str
    team_id: str | None = None
    season: int | None = None

# --- Endpoints Refactorizados ---

@app.get("/teams")
def get_teams(
    league: str | None = None, 
    season: int | None = None,
    db: Session = Depends(get_db) # Inyecta la sesión de BD
):
    # Construye la consulta SQL de forma segura
    query = "SELECT team_id, team_name AS name, country AS league, :season AS season FROM teams" # Asumiendo una estructura simple
    params = {"season": season or 2024} # Ajusta el default
    
    # Aquí deberías ajustar la query para que filtre por liga y temporada
    # Esta es una implementación simple leyendo de la tabla 'teams'
    
    try:
        # Ejecuta la consulta usando SQLAlchemy
        result = db.execute(text(query), params)
        teams_data = result.mappings().all() # Convierte a lista de dicts
        
        # Valida y devuelve los datos usando tu modelo Pydantic
        return [Team(**row) for row in teams_data]
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/players")
def get_players(
    team_id: str | None = None, 
    season: int | None = None,
    db: Session = Depends(get_db) # Inyecta la sesión de BD
):
    # Ejemplo: Consulta para obtener jugadores de 'player_season_stats'
    # Esta query ASUME que tienes las tablas del schema.sql
    
    query = """
        SELECT 
            p.player_uuid, 
            p.full_name, 
            pss.team_id, 
            pss.season_id AS season
        FROM players p
        JOIN player_season_stats pss ON p.player_uuid = pss.player_uuid
        WHERE 1=1
    """
    params = {}

    if team_id:
        query += " AND pss.team_id = :team_id"
        params["team_id"] = team_id
    if season:
        query += " AND pss.season_id = :season_id"
        params["season_id"] = str(season) # El schema lo define como text

    try:
        result = db.execute(text(query), params)
        player_data = result.mappings().all()
        return [Player(**row) for row in player_data]

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    # backend/app.py (parcial)

# ... (importaciones y setup de 'get_db' como en la respuesta anterior) ...

@app.get("/players/search")
def search_players(
    query: str,
    db: Session = Depends(get_db)
):
    """
    Busca jugadores por nombre para el autocompletado del frontend.
    """
    # Consulta la tabla 'players'
    sql = text("SELECT player_uuid, full_name, primary_position FROM players WHERE full_name ILIKE :query LIMIT 10")
    result = db.execute(sql, {"query": f"%{query}%"})
    return result.mappings().all()

@app.get("/player/{player_uuid}/details")
def get_player_details(
    player_uuid: str,
    season_id: str = "2024", # O la temporada que definas por defecto
    db: Session = Depends(get_db)
):
    """
    Obtiene las estadísticas y el valor de mercado de UN jugador.
    """
    # 1. Obtener stats (de 'player_season_stats')
    sql_stats = text("""
        SELECT * FROM player_season_stats 
        WHERE player_uuid = :uuid AND season_id = :season
    """)
    stats = db.execute(sql_stats, {"uuid": player_uuid, "season": season_id}).mappings().first()

    # 2. Obtener valor de mercado (de 'market_values')
    sql_mv = text("""
        SELECT value_eur FROM market_values
        WHERE player_uuid = :uuid
        ORDER BY as_of_date DESC
        LIMIT 1
    """)
    mv = db.execute(sql_mv, {"uuid": player_uuid}).mappings().first()

    if not stats:
        raise HTTPException(status_code=404, detail="Estadísticas no encontradas para este jugador/temporada")

    return {
        "stats": stats,
        "market_value": mv["value_eur"] if mv else None
    }

# backend/app.py (Añadir esto)

# ... (arriba, inicializa el 'similarity_service' como se muestra en el Paso B) ...

@app.get("/player/{player_uuid}/similar")
def get_similar_players(
    player_uuid: str,
    n: int = 5,
    db: Session = Depends(get_db)
):
    """
    Recibe un UUID de jugador y devuelve los N jugadores más similares
    estadísticamente, junto con sus stats y valor de mercado.
    """
    try:
        # Re-inyectamos la fábrica de sesiones, no la sesión misma
        # O mejor, modificamos SimilarityService para que acepte la sesión 'db'
        
        # --- Solución más simple (modifica SimilarityService) ---
        # Pasa la sesión 'db' directamente al método
        # (Tendrías que refactorizar SimilarityService para que no dependa de la fábrica)
        
        # --- Solución robusta (usando la instancia ya creada) ---
        # (Asumiendo que similarity_service fue creado con SessionLocal)
        
        similar_players = similarity_service.find_similar_players(
            target_player_uuid=player_uuid,
            n_similar=n
        )
        return similar_players
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    