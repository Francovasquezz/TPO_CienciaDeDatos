# backend/db.py
import os
import logging
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# IMPORTANTE: Eliminamos NullPool y no es necesario importar QueuePool explícitamente 
# porque es el default de SQLAlchemy, pero configuramos sus parámetros abajo.
from urllib.parse import quote_plus

# Configuración de logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_SSLMODE = os.getenv("DB_SSLMODE", "require")

# Construcción de la URL de conexión
if not all([DB_HOST, DB_USER, DB_PASSWORD]):
    logger.warning("⚠️ Faltan variables de entorno para la BD. Usando DuckDB local como fallback.")
    DB_URL = "duckdb:///data/tpo.duckdb"
    # Configuración simple para DuckDB
    engine = create_engine(DB_URL)
else:
    pwd = quote_plus(DB_PASSWORD)
    DB_URL = f"postgresql+psycopg2://{DB_USER}:{pwd}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={DB_SSLMODE}"
    
    # --- CONFIGURACIÓN OPTIMIZADA (POOLING) ---
    logger.info("🔌 Conectando a PostgreSQL con Pooling activado...")
    engine = create_engine(
        DB_URL, 
        pool_pre_ping=True,  # Verifica que la conexión esté viva antes de usarla
        pool_size=10,        # Mantiene hasta 10 conexiones abiertas listas para usar
        max_overflow=20,     # Permite crear hasta 20 extras si hay mucho tráfico
        pool_recycle=1800    # Recicla conexiones cada 30 minutos para evitar timeouts
        # poolclass=NullPool  <-- ELIMINADO: Esto era lo que causaba la lentitud
    )

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Generador de dependencias para obtener una sesión de BD."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()