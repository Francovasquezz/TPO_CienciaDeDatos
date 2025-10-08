import LanusStats as ls
from pathlib import Path
import pandas as pd
from datetime import date

print("OK Python + venv")
print("Páginas:", list(ls.get_available_pages().keys()))

# Parámetros
PAGE   = "Fbref"
LEAGUE = "Primera Division Argentina"

# Crear carpetas de salida
today = date.today().isoformat()
raw_dir = Path("data/raw/fbref") / LEAGUE.replace(" ", "_") / today
proc_dir = Path("data/processed")
raw_dir.mkdir(parents=True, exist_ok=True)
proc_dir.mkdir(parents=True, exist_ok=True)

# Temporadas disponibles para la liga
seasons = ls.get_available_season_for_leagues(PAGE, LEAGUE)
print(f"Temporadas disponibles en {LEAGUE}:", seasons)

# Tomamos la temporada más reciente
season = seasons[0]
print("Usando temporada:", season)

# Descarga de tabla de liga
fb = ls.Fbref()
df = fb.get_league_table(LEAGUE, season)

# Guardar RAW (CSV tal cual bajado)
raw_csv = raw_dir / f"league_table_{season}.csv"
df.to_csv(raw_csv, index=False, encoding="utf-8")
print("RAW guardado:", raw_csv)

# Guardar PROCESSED (Parquet más eficiente)
proc_parquet = proc_dir / f"league_table_{LEAGUE.replace(' ','_')}_{season}.parquet"
df.to_parquet(proc_parquet, index=False)
print("PROCESSED guardado:", proc_parquet)

# Preview
print(df.head())
