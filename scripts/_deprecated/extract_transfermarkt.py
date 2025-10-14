# scripts/extract_transfermarkt.py
import argparse, time, csv, unicodedata, re
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
import LanusStats as ls

def norm(s):
    s = (s or "").strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if ch.isascii())
    return re.sub(r"\s+", " ", s)

def load_team_ids(path: str, league: str) -> List[Dict]:
    rows = list(csv.DictReader(open(path, newline="", encoding="utf-8")))
    out = [r for r in rows if str(r["league_code"]).upper()==str(league).upper() and r.get("tm_team_id")]
    if not out:
        raise SystemExit(f"No hay tm_team_id para {league} en {path}. Corré bootstrap y/o completa IDs.")
    return out

def main(league: str, season: str, outdir: str, sleep: float):
    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    # Resolver nombre que espera TM (LanusStats)
    league_tm = {
        "ARG1":"Primera Division Argentina","BRA1":"Brasileirao","ENG1":"Premier League",
        "ESP1":"La Liga","FRA1":"Ligue 1","GER1":"Bundesliga","ITA1":"Serie A","POR1":"Primeira Liga Portugal",
    }.get(league.upper(), league)

    tm = ls.Transfermarkt()
    teams = load_team_ids("configs/tm_team_ids.csv", league)

    rows=[]
    ok, fail = 0, 0
    for r in teams:
        tname = r["team_name"].strip()
        canon = r.get("tm_canonical_name", "").strip() or tname
        tid   = str(r["tm_team_id"]).strip()
        try:
            df = tm.scrape_players_for_teams(team_name=canon, team_id=tid, season=str(season))
        except Exception:
            df = pd.DataFrame()

        if df is None or df.empty:
            print(f"[WARN] {tname}({tid}) sin datos (LS).")
            fail += 1
        else:
            # normalizar y proyectar mínimo
            name_col = next((c for c in ["player_name","name","Player","Jugadores"] if c in df.columns), None)
            val_col  = next((c for c in ["market_value_eur","Market value","Valor de mercado","value"] if c in df.columns), None)
            dob_col  = next((c for c in ["dob","date_of_birth","born","F. Nacim./Edad","Edad"] if c in df.columns), None)
            pos_col  = next((c for c in ["position","primary_position","Posicion"] if c in df.columns), None)

            for _, p in df.iterrows():
                rows.append({
                    "name": str(p.get(name_col,"") or "").strip(),
                    "dob":  str(p.get(dob_col,"") or "").strip(),
                    "team": tname,
                    "league_code": league,
                    "season": str(season),
                    "tm_player_id": str(p.get("player_id") or p.get("tm_player_id") or ""),
                    "market_value_eur": p.get(val_col, None),
                    "as_of_date": None,
                    "primary_position": str(p.get(pos_col,"") or "").strip(),
                })
            ok += 1

        time.sleep(max(0.0, sleep))

    df = pd.DataFrame(rows).drop_duplicates()
    part = out / f"transfermarkt_players_{league}_{season}.csv"
    df.to_csv(part, index=False)
    agg  = out / "transfermarkt_players.csv"
    if agg.exists():
        base = pd.read_csv(agg)
        base = pd.concat([base, df], ignore_index=True)
        base = base.drop_duplicates(subset=["tm_player_id","name","team","season","league_code"], keep="last")
        base.to_csv(agg, index=False)
    else:
        df.to_csv(agg, index=False)

    print(f"OK equipos LS={ok}, sin_datos={fail}, jugadores={len(df)}")
    print(f"OK -> {part}")
    print(f"OK -> {agg}")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--outdir", default="data/processed")
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()
    main(args.league, args.season, args.outdir, args.sleep)
