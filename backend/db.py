# backend/db.py (CORREGIDO Y ADAPTADO)

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import quote_plus
import logging

# Carga las variables de entorno (ej: desde el archivo .env)
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require") # Fundamental para Supabase Pooler

if not all([DB_HOST, DB_USER, DB_PASSWORD]):
    logging.warning("ADVERTENCIA: Faltan variables de BD. Usando DuckDB como fallback.")
    DB_URL = "duckdb:///data/tpo.duckdb" # Fallback local
else:
    # Construye la URL de conexión robusta para PostgreSQL
    pwd = quote_plus(DB_PASSWORD)
    DB_URL = f"postgresql+psycopg2://{DB_USER}:{pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"

# Crea el 'engine'
engine = create_engine(DB_URL, pool_pre_ping=True)

# Crea una fábrica de sesiones que usará la API
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependencia de FastAPI (para inyectar en los endpoints)
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()