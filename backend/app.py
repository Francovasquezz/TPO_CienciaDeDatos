from fastapi import FastAPI
import pandas as pd
from pathlib import Path

app = FastAPI(
    title="Fútbol Analytics API",
    description="API para servir datos y predicciones de fútbol."
)

PROCESSED_DATA_PATH = Path("data/processed")

@app.get("/health")
def health_check():
    """Chequeo de salud de la API."""
    return {"status": "ok", "message": "API funcionando!"}


@app.get("/league-table/{season}")
def get_league_table(season: str):
    """
    Devuelve la tabla de posiciones para una temporada específica.
    """
    file_path = PROCESSED_DATA_PATH / f"processed_league_table_{season}.parquet"
    
    if not file_path.exists():
        return {"error": f"No se encontraron datos para la temporada {season}"}
    
    df = pd.read_parquet(file_path)
    
    # Convertimos el DataFrame a una lista de diccionarios para devolverlo como JSON
    return df.to_dict(orient="records")