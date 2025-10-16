# scripts/bootstrap_tm_team_ids.py
import argparse
import re
from pathlib import Path
import pandas as pd
import requests
from bs4 import BeautifulSoup

TM_WETTBEWERB = {
    "ARG1": "AR1N",  # Primera División Argentina
    # Agregá más si querés escalar
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9,es-AR;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.transfermarkt.com/",
}

def parse_verein_id(url: str) -> str | None:
    m = re.search(r"/verein/(\d+)", url or "")
    return m.group(1) if m else None

def canonical_text(a) -> str:
    # Nombre del club: intenta varios lugares
    txt = (a.get_text(strip=True) or "").strip()
    if txt:
        return txt
    title = (a.get("title") or "").strip()
    if title:
        return title
    img = a.find("img")
    if img:
        alt = (img.get("alt") or "").strip()
        if alt:
            return alt
    # A veces hay spans dentro
    span = a.find("span")
    if span:
        st = (span.get_text(strip=True) or "").strip()
        if st:
            return st
    return ""

def get_league_clubs(league_code: str, season: str) -> pd.DataFrame:
    wett = TM_WETTBEWERB.get(league_code)
    if not wett:
        raise SystemExit(f"No tengo mapeo de Transfermarkt para league_code={league_code}")

    url = f"https://www.transfermarkt.com/primera-division/startseite/wettbewerb/{wett}/saison_id/{season}"
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")

    clubs = []

    # 1) Selector clásico
    for a in soup.select("a.vereinprofil_tooltip[href*='/verein/']"):
        vid = parse_verein_id(a.get("href"))
        if not vid: 
            continue
        name = canonical_text(a)
        clubs.append({"tm_team_id": vid, "tm_canonical_name": name})

    # 2) Fallback: cualquier link con /verein/
    if not clubs:
        for a in soup.select("a[href*='/verein/']"):
            vid = parse_verein_id(a.get("href"))
            if not vid:
                continue
            name = canonical_text(a)
            if not name:
                # intenta buscar un td cercano
                td = a.find_parent("td")
                if td:
                    near_txt = (td.get_text(" ", strip=True) or "").strip()
                    name = near_txt or name
            clubs.append({"tm_team_id": vid, "tm_canonical_name": name})

    # 3) Fallback: tabla .items (formato viejo)
    if not clubs:
        for row in soup.select("table.items tbody tr"):
            a = row.select_one("a[href*='/verein/']")
            if not a:
                continue
            vid = parse_verein_id(a.get("href"))
            if not vid:
                continue
            name = canonical_text(a)
            if not name:
                name = (row.get_text(" ", strip=True) or "").strip()
            clubs.append({"tm_team_id": vid, "tm_canonical_name": name})

    df = pd.DataFrame(clubs)
    if df.empty:
        raise SystemExit("No pude detectar clubes en la página de la liga. Revisa si la URL cambió o si TM está bloqueando.")

    # Limpieza final
    df = df.dropna(subset=["tm_team_id"]).drop_duplicates(subset=["tm_team_id"])
    df["tm_team_id"] = df["tm_team_id"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    df["tm_canonical_name"] = df["tm_canonical_name"].fillna("").astype(str).str.strip()

    return df

def main(league: str, season: str, out_csv: str):
    df_tm = get_league_clubs(league, season)

    # Usa el nombre canónico de TM como team_name por defecto
    df_tm["team_name"] = df_tm["tm_canonical_name"].where(df_tm["tm_canonical_name"].ne(""), df_tm["tm_team_id"])

    df_tm["league_code"] = league
    df_tm["season"] = str(season)
    df_tm["team_id"] = (
        df_tm["tm_team_id"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()
    )

    cols = ["league_code", "season", "team_id", "team_name", "tm_canonical_name"]
    df_out = df_tm[cols].copy()

    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    df_out.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"OK -> {out_csv} ({len(df_out)} equipos)")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Bootstrap de team_ids de Transfermarkt por liga/temporada")
    ap.add_argument("--league", required=True, help="p.ej. ARG1")
    ap.add_argument("--season", required=True, help="p.ej. 2024")
    ap.add_argument("--out", default="configs/tm_team_ids.csv")
    args = ap.parse_args()
    main(args.league, args.season, args.out)
