# backend/db.py
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool # Asegúrate de tener este import
from urllib.parse import quote_plus
import logging

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

if not all([DB_HOST, DB_USER, DB_PASSWORD]):
    logging.warning("ADVERTENCIA: Faltan variables de BD. Usando DuckDB como fallback.")
    DB_URL = "duckdb:///data/tpo.duckdb"
else:
    pwd = quote_plus(DB_PASSWORD)
    DB_URL = f"postgresql+psycopg2://{DB_USER}:{pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"

# --- CORRECCIÓN FINAL ---
# Al usar NullPool, ELIMINAMOS pool_size y max_overflow
engine = create_engine(
    DB_URL, 
    pool_pre_ping=True,
    poolclass=NullPool 
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()