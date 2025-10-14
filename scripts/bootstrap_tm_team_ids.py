# scripts/bootstrap_tm_team_ids.py
import argparse, csv, unicodedata, re, urllib.parse, requests
from pathlib import Path
import pandas as pd
import LanusStats as ls
from bs4 import BeautifulSoup

HEADERS = {"User-Agent":"Mozilla/5.0","Accept-Language":"en-US,en;q=0.8,es;q=0.6"}
TM_SEARCH = "https://www.transfermarkt.com/schnellsuche/ergebnis/schnellsuche"

def norm(s): 
    s = (s or "").strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if ch.isascii())
    return re.sub(r"\s+", " ", s)

def propose_tm_id_and_canon(team_name: str):
    # 1) búsqueda rápida en TM
    r = requests.get(TM_SEARCH, params={"query": team_name}, headers=HEADERS, timeout=20)
    if r.status_code != 200: return None, None
    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.select("a[href]"):
        href = a.get("href","")
        m = re.search(r"/([^/]+)/startseite/verein/(\d+)", href, flags=re.I)
        if m:
            slug, tid = m.group(1), m.group(2)
            canon = re.sub(r"\s+", " ", slug.replace("-", " ").title())
            return tid, canon
    m2 = re.search(r"/verein/(\d+)", r.text, flags=re.I)
    if m2:
        tid = m2.group(1)
        canon = re.sub(r"\s+", " ", unicodedata.normalize("NFKD", team_name).encode("ascii","ignore").decode("ascii")).title()
        return tid, canon
    return None, None

def main(league_code: str, season: str, out_csv: str):
    out = Path(out_csv)
    out.parent.mkdir(parents=True, exist_ok=True)

    # 1) equipos desde TM valuaciones (nombres “buenos” para ese contexto)
    tm = ls.Transfermarkt()
    league_name = {
        "ARG1":"Primera Division Argentina","BRA1":"Brasileirao","ENG1":"Premier League",
        "ESP1":"La Liga","FRA1":"Ligue 1","GER1":"Bundesliga","ITA1":"Serie A","POR1":"Primeira Liga Portugal",
    }.get(league_code.upper(), league_code)
    df = tm.get_league_teams_valuations(league=league_name, season=str(season))
    col_team = next((c for c in ["Club","Team","Equipo","team_name","team"] if c in df.columns), None)
    if not col_team: raise SystemExit(f"No encuentro nombre de club en columnas: {list(df.columns)}")
    teams = sorted({str(x).strip() for x in df[col_team].dropna().tolist()})

    # 2) cargar csv previo (si existe)
    rows = []
    if out.exists():
        rows = list(csv.DictReader(open(out, newline="", encoding="utf-8")))
    have = {(r["league_code"], r["team_name"]) for r in rows}

    # 3) agregar faltantes con propuestas
    for t in teams:
        key = (league_code, t)
        if key in have: 
            continue
        tm_id, canon = propose_tm_id_and_canon(t)
        rows.append({
            "league_code": league_code,
            "team_name": t,
            "tm_team_id": tm_id or "",
            "tm_canonical_name": canon or "",
        })

    # 4) escribir
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["league_code","team_name","tm_team_id","tm_canonical_name"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
    print(f"OK -> {out} ({sum(1 for _ in rows)} equipos)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True)
    ap.add_argument("--season", required=True)
    ap.add_argument("--out", default="configs/tm_team_ids.csv")
    args = ap.parse_args()
    main(args.league, args.season, args.out)
