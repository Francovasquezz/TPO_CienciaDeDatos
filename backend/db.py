# backend/db.py (CORREGIDO)

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Carga las variables de entorno (ej: desde el archivo .env)
load_dotenv()

# Intenta obtener la URL completa primero (para flexibilidad)
DB_URL = os.getenv("DB_URL")

if not DB_URL:
    # Si DB_URL no existe, construye la URL desde las partes
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")

    if all([db_user, db_password, db_host, db_port, db_name]):
        DB_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    else:
        # Si tampoco están las partes, usa DuckDB como fallback
        print("Variables de BD no encontradas. Usando DuckDB como fallback.")
        DB_URL = None

# Crea el 'engine'
if DB_URL:
    engine = create_engine(DB_URL, pool_pre_ping=True)
else:
    # Fallback a DuckDB si todo lo demás falla
    engine = create_engine("duckdb:///data/tpo.duckdb")