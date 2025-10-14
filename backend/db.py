import os
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine

DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
engine = create_engine(DB_URL, pool_pre_ping=True) if DB_URL else create_engine("duckdb:///data/tpo.duckdb")
