import pandas as pd
from datetime import date

src = r"data/processed/tm_values_AR1N_2024_latest.csv"
dst = r"data/processed/tm_values_AR1N_2024_latest.normalized.csv"

tm = pd.read_csv(src, encoding="utf-8-sig")

# Renombrar SOLO lo necesario. NO toques market_value_eur (el xref la usa).
colmap = {
    "player_name": "name",
    "club_name": "team",
    "dob": "dob",
    # "market_value_eur":  # dejar como está
}
tm = tm.rename(columns=colmap)

# Duplicar como 'market_value' por conveniencia (pero mantener market_value_eur)
if "market_value_eur" in tm.columns and "market_value" not in tm.columns:
    tm["market_value"] = tm["market_value_eur"]

# tm_player_id (lo espera el xref)
if "player_id" in tm.columns and "tm_player_id" not in tm.columns:
    tm["tm_player_id"] = tm["player_id"]

# nationality si no existe (lo usa para agrupar)
if "nationality" not in tm.columns:
    tm["nationality"] = None

# last_update requerido por el xref; si no existe, hoy
if "last_update" not in tm.columns:
    tm["last_update"] = pd.to_datetime(date.today()).date()

tm.to_csv(dst, index=False, encoding="utf-8-sig")
print(f"OK -> {dst} (cols: {list(tm.columns)})")
