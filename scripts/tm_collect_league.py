# scripts/tm_collect_league.py
import argparse
import time
import re
from pathlib import Path
import random

import pandas as pd
import requests
from bs4 import BeautifulSoup
import cloudscraper # <-- AÑADE ESTA LÍNEA
import LanusStats as ls

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.transfermarkt.com.ar/",
}

def tm_value_to_eur(s: str) -> float | None:
    if s is None:
        return None
    s = str(s).strip().replace("\u2009","")  # thin space
    if not s or s in {"-", "—"}:
        return None
    s = s.lower().replace("€","").replace("eur","").replace("valor de mercado","").strip()
    mult = 1.0
    if s.endswith("bn"):
        mult, s = 1_000_000_000.0, s[:-2]
    elif s.endswith("m"):
        mult, s = 1_000_000.0, s[:-1]
    elif s.endswith("k"):
        mult, s = 1_000.0, s[:-1]
    s = s.replace(",", ".")
    try:
        return float(s) * mult
    except Exception:
        return None

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = [c.lower().strip() for c in df.columns]
    for cand in candidates:
        if cand.lower() in cols:
            return df.columns[cols.index(cand.lower())]
    return None

def bs_get(url: str) -> BeautifulSoup | None:
    # MUEVE la creación del scraper AQUÍ DENTRO.
    # Esto crea una sesión nueva y limpia para cada petición.
    scraper = cloudscraper.create_scraper()
    try:
        r = scraper.get(url, timeout=30)
        print(f" | Status: {r.status_code}", end="")
        if r.status_code != 200:
            return None
        return BeautifulSoup(r.text, "lxml")
    except Exception as e:
        print(f" | Request Error: {e}", end="")
        return None

def fallback_scrape_tm_squad(team_id: str, season: str) -> pd.DataFrame:
    """
    Scraper robusto directo a Transfermarkt si LanusStats falla.
    Intenta varios dominios y patrones de URL.
    Devuelve DF con columnas Player, MarketValueEUR o vacío si no pudo.
    """
    domains = ["transfermarkt.com.ar", "transfermarkt.com"]
    patterns = [
        "/kader/verein/{id}/plus/0/galerie/0?saison_id={season}",
        "/kader/verein/{id}/saison_id/{season}/plus/1",
        "/kader/verein/{id}?saison_id={season}&plus=1",
    ]

    for dom in domains:
        for pat in patterns:
            url = f"https://{dom}" + pat.format(id=team_id, season=season)
            soup = bs_get(url)
            if not soup:
                continue
            
            # El selector original a veces falla. Probamos con uno más genérico.
            table = soup.select_one("table.items")
            if not table:
                # Si no encuentra la tabla con la clase "items", 
                # intentamos buscar cualquier tabla que parezca ser la de jugadores
                # Este es un selector más genérico que podría funcionar en más casos.
                table = soup.select_one(".responsive-table")


            if not table:
                continue

            # Localizar índice de "Valor de mercado" por thead (es/pt/en)
            headers = [th.get_text(" ", strip=True).lower() for th in table.select("thead th")]
            mv_aliases = {"valor de mercado", "market value", "valor de mercado actual", "marketvalue"}
            mv_idx = None
            for i, h in enumerate(headers):
                if h in mv_aliases:
                    mv_idx = i
                    break
            # fallback: si no lo encuentra, asumir última col
            if mv_idx is None and headers:
                mv_idx = len(headers) - 1

            rows = []
            for tr in table.select("tbody tr"):
                # Se ha observado que en algunos casos la clase es 'spielprofil_tooltip tooltip tooltip-left'
                a = tr.select_one("a.spielprofil_tooltip")
                if not a:
                    continue
                name = a.get_text(strip=True)
                tds = tr.find_all("td")
                mv_txt = ""
                if mv_idx is not None and mv_idx < len(tds):
                    mv_txt = tds[mv_idx].get_text(" ", strip=True)
                else:
                    # otro intento: último td con símbolo €
                    for td in reversed(tds):
                        txt = td.get_text(" ", strip=True)
                        if "€" in txt:
                            mv_txt = txt
                            break
                rows.append({"Player": name, "MarketValueEUR": tm_value_to_eur(mv_txt)})

            if rows:
                return pd.DataFrame(rows)

            # Si no se pudo parsear, probar siguiente variante
    return pd.DataFrame()

def fetch_tm_team(team_name: str, team_id: str, season: str) -> pd.DataFrame:
    """
    Primero intenta LanusStats; si falla o viene vacío, usa fallback_scrape_tm_squad.
    """
    t = ls.Transfermarkt()
    try:
        df = t.scrape_players_for_teams(team_name=team_name, team_id=team_id, season=season)
    except Exception as e:
        print(f"\n[WARN] {team_name} ({team_id}) fallo LS: {e}")
        df = None

    # Si LanusStats trae algo, normalizamos columnas
    if df is not None and not df.empty:
        col_name = pick_col(df, ["Jugadores","Jugador","Players","Player"])
        col_val  = pick_col(df, ["Valor de mercado","Market value"])
        if col_name and col_val:
            out = pd.DataFrame()
            out["Player"] = df[col_name].astype(str).str.strip()
            out["MarketValueEUR"] = df[col_val].astype(str).map(tm_value_to_eur)
            return out

    # Fallback directo a TM
    fb = fallback_scrape_tm_squad(team_id, season)
    if not fb.empty:
        return fb

    return pd.DataFrame()

def load_team_ids(league: str, season: str, overrides_path: Path | None) -> pd.DataFrame:
    base = pd.read_csv("configs/tm_team_ids.csv", dtype=str)
    base.columns = [c.strip() for c in base.columns]
    req = {"league_code","season","team_id","team_name"}
    missing = req - set(base.columns)
    if missing:
        raise SystemExit(f"tm_team_ids.csv incompleto. Faltan columnas: {missing}")

    base = base[(base["league_code"]==league) & (base["season"]==str(season))].copy()
    base["team_id"] = base["team_id"].astype(str).str.replace(r"\.0$","",regex=True)
    base["team_id"] = base["team_id"].apply(lambda x: re.sub(r"\D", "", x or ""))

    if overrides_path and overrides_path.exists():
        ov = pd.read_csv(overrides_path, dtype=str)
        ov.columns = [c.strip() for c in ov.columns]
        need = {"league_code","season","team_name","team_id"}
        miss = need - set(ov.columns)
        if miss:
            raise SystemExit(f"Overrides mal formateado. Faltan: {miss}")
        ov = ov[(ov["league_code"]==league) & (ov["season"]==str(season))].copy()
        if not ov.empty:
            base = base.drop(columns=["team_id"]).merge(
                ov[["team_name","team_id"]], on="team_name", how="left", suffixes=("","_ov")
            )
            base["team_id"] = base["team_id"].fillna(base["team_id_ov"])
            base = base.drop(columns=["team_id_ov"])

    base = base[base["team_id"].notna() & (base["team_id"].str.len()>0)]
    base = base.drop_duplicates(subset=["team_id"])
    return base

def main(league: str, season: str, sleep: float, outdir: str, overrides: str | None):
    teams = load_team_ids(league, season, Path(overrides) if overrides else None)

    rows = []
    ok, bad = 0, 0
    for _, r in teams.iterrows():
        team_name = r["team_name"]
        team_id = r["team_id"]
        print(f"→ {team_name} ({team_id})...", end="", flush=True)
        df = fetch_tm_team(team_name, team_id, str(season))
        if df.empty:
            print(" FAIL")
            bad += 1
            continue
        print(f" OK ({len(df)} filas)")
        ok += 1
        df["team_name"] = team_name
        df["team_id"] = team_id
        rows.append(df)
        # Sleep con jitter para evitar bloqueos
        time.sleep(max(0.0, float(sleep) + random.uniform(0, 0.8)))

    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_csv = outdir / f"tm_values_{league}_{season}.csv"
    if rows:
        all_df = pd.concat(rows, ignore_index=True)
        all_df = all_df.drop_duplicates(subset=["team_id","Player"])
        all_df.to_csv(out_csv, index=False)
        print(f"\nResumen: equipos OK={ok}/{len(teams)}, jugadores={len(all_df)}")
        print(f"OK -> {out_csv}")
    else:
        print(f"\nResumen: equipos OK={ok}/{len(teams)}, jugadores=0")
        pd.DataFrame(columns=["Player","MarketValueEUR","team_name","team_id"]).to_csv(out_csv, index=False)
        print(f"OK -> {out_csv} (vacío)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--sleep", type=float, default=1.0)
    ap.add_argument("--outdir", default="data/processed")
    ap.add_argument("--overrides", default="configs/tm_team_overrides.csv")
    args = ap.parse_args()
    main(args.league, args.season, args.sleep, args.outdir, args.overrides)