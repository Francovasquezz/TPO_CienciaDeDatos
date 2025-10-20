# scripts/run_etl.py
import argparse
import subprocess
import sys
import time
from pathlib import Path
import yaml


def call_backend_etl(repo_root: Path, league: str, season: str):
    """Invoca backend/etl.py por CLI para evitar issues de import."""
    etl_path = repo_root / "backend" / "etl.py"
    cmd = [sys.executable, str(etl_path), "--league", str(league), "--season", str(season)]
    print(" $", " ".join(cmd))
    subprocess.run(cmd, check=True)


def main():
    ap = argparse.ArgumentParser(description="Orquestador multi-liga/temporada para backend/etl.py")
    ap.add_argument("--config", default="configs/leagues.yaml", help="Ruta al YAML de ligas")
    ap.add_argument("--sleep", type=float, default=1.0, help="Segundos entre ejecuciones (rate-limit friendly)")
    ap.add_argument("--only-league", default=None, help="Filtra por un código/nombre de liga (ej: ARG1 o 'Primera Division Argentina')")
    ap.add_argument("--only-season", default=None, help="Filtra por una temporada (ej: 2024)")
    args = ap.parse_args()

    repo_root = Path(__file__).resolve().parents[1]

    # Si pasaste filtros, corré sin YAML como fallback
    if args.only_league and args.only_season:
        print(f"▶ ETL league={args.only_league} season={args.only_season}")
        call_backend_etl(repo_root, args.only_league, args.only_season)
        return

    # Caso normal: leer YAML
    cfg_path = (repo_root / args.config).resolve()
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    leagues = cfg.get("leagues", [])
    if not leagues:
        print(f"❌ No se encontraron ligas en {cfg_path}")
        sys.exit(1)

    for item in leagues:
        league_value = item.get("code") or item.get("name") or item.get("league")
        seasons = item.get("seasons") or []
        if not league_value or not seasons:
            print(f"⚠️  Liga ignorada por config incompleta: {item}")
            continue

        # Filtros opcionales
        if args.only_league and str(league_value).lower() != str(args.only_league).lower():
            continue

        for season in seasons:
            if args.only_season and str(season) != str(args.only_season):
                continue

            print(f"▶ ETL league={league_value} season={season}")
            call_backend_etl(repo_root, league_value, season)
            time.sleep(max(0.0, args.sleep))


if __name__ == "__main__":
    main()
