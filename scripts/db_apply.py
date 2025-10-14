# scripts/db_apply.py
import os, glob
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL") or os.getenv("DATABASE_URL")
if not DB_URL:
    raise SystemExit("❌ Falta DB_URL/DATABASE_URL en tu .env")

engine = create_engine(DB_URL, pool_pre_ping=True)
SCHEMA_DIR = os.getenv("SCHEMA_DIR", "database/schema")

def run_sql_file(path: str):
    print(f"▶ Applying {path}")
    with engine.begin() as conn:
        sql = open(path, "r", encoding="utf-8").read()
        for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
            conn.execute(text(stmt))

if __name__ == "__main__":
    files = sorted(glob.glob(os.path.join(SCHEMA_DIR, "*.sql")))
    if not files:
        raise SystemExit(f"❌ No se encontraron .sql en {SCHEMA_DIR}")
    for f in files:
        run_sql_file(f)
    print("✅ Schema applied")
