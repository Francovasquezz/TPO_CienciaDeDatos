import argparse, os
from pathlib import Path
import pandas as pd

def main(processed_csv, league_code, season):
    processed_csv = Path(processed_csv)
    out_dir = Path("data/processed")
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(processed_csv)

    # columnas base si existen
    cols = [c for c in ["Player","Born","Nation","Pos","Squad","AgeYears","IsGK"] if c in df.columns]
    if not cols:
        raise SystemExit("No se encuentran columnas base en el procesado.")

    view = df[cols].copy()

    # normalizar nombres
    rename = {
        "Player": "name",
        "Born": "dob",
        "Nation": "nationality",
        "Pos": "position",
        "Squad": "team",
        "AgeYears": "age_years",
        "IsGK": "is_gk",
    }
    view = view.rename(columns={k:v for k,v in rename.items() if k in view.columns})

    # metadatos mínimos
    view["season"] = str(season)
    view["league_code"] = str(league_code)
    # placeholder (no tenemos la URL de fbref acá)
    view["player_id_fb"] = ""

    part = out_dir / f"fbref_players_{league_code}_{season}.csv"
    view.to_csv(part, index=False)

    agg = out_dir / "fbref_players.csv"
    if agg.exists():
        base = pd.read_csv(agg)
        base = pd.concat([base, view], ignore_index=True)
        base = base.drop_duplicates(subset=["player_id_fb","name","dob","team","season","league_code"], keep="last")
        base.to_csv(agg, index=False)
    else:
        view.to_csv(agg, index=False)

    print(f"OK -> {part}")
    print(f"OK -> {agg}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--processed", required=True, help="Ruta al CSV procesado")
    ap.add_argument("--league", required=True, help="Código de liga, ej ARG1")
    ap.add_argument("--season", required=True, help="Temporada, ej 2024")
    args = ap.parse_args()
    main(args.processed, args.league, args.season)
