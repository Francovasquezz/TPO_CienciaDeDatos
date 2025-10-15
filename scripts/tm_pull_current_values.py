# scripts/tm_pull_current_values.py
import argparse, time, re
from datetime import date
from pathlib import Path
from urllib.parse import urljoin
import requests
import pandas as pd
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/126.0.0.0 Safari/537.36"),
    # Forzamos EN para que los sufijos sean consistentes (k/m) y no “mill. €”
    "Accept-Language": "en,en-US;q=0.9,es-AR;q=0.8",
    "Referer": "https://www.transfermarkt.com/"
}
COOKIES = {"tmrkt_lang": "en"}

def get_html(url: str) -> BeautifulSoup:
    r = requests.get(url, headers=HEADERS, cookies=COOKIES, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def parse_season(league_url: str) -> str:
    m = re.search(r"/saison_id/(\d{4})", league_url)
    return m.group(1) if m else ""

def get_club_ids_from_league(league_url: str) -> list[int]:
    """
    Extrae los IDs de club desde la página de la liga (tabla .items).
    """
    soup = get_html(league_url)
    club_ids = set()
    # En la tabla de clubes, los links contienen /verein/<id>/
    for a in soup.select("table.items a[href*='/verein/']"):
        href = a.get("href", "")
        m = re.search(r"/verein/(\d+)", href)
        if m:
            club_ids.add(int(m.group(1)))
    return sorted(club_ids)

def build_roster_urls(club_id: int, season: str) -> list[str]:
    """
    Variantes de URL para el plantel del club (algunas devuelven 404).
    Probamos en orden hasta que una funcione.
    """
    base = "https://www.transfermarkt.com"
    variants = [
        f"{base}/kader/verein/{club_id}/saison_id/{season}/plus/1",
        f"{base}/startseite/verein/{club_id}/saison_id/{season}",
        f"{base}/kader/verein/{club_id}/saison_id/{season}"
    ]
    return variants

_VAL_RX = re.compile(r"([0-9]+(?:[.,][0-9]+)?)\s*([kKmM])?")

def to_eur(value_text: str) -> int | None:
    """
    Convierte '€3.5m' | '3.5 m' | '250k' | '€250,000' -> euros (int).
    Devuelve None si no hay número.
    """
    if not value_text:
        return None
    t = value_text.replace("€", "").replace("EUR", "").strip()
    if t in {"-", "--", ""}:
        return None
    # normalizar miles/puntos
    t = t.replace("\xa0", " ")
    m = _VAL_RX.search(t)
    if not m:
        # casos como '1.5 mill. €' (ES) -> tratarlos
        t2 = (t.lower()
              .replace("mill.", "m")
              .replace("mio.", "m")
              .replace("mil.", "k")
              .replace("millon", "m"))
        m = _VAL_RX.search(t2)
        if not m:
            return None
    num = m.group(1).replace(",", ".")
    try:
        x = float(num)
    except ValueError:
        return None
    suf = (m.group(2) or "").lower()
    if suf == "m":
        x *= 1_000_000
    elif suf == "k":
        x *= 1_000
    return int(round(x))

def parse_club_roster(club_id: int, season: str, sleep: float = 1.0) -> list[dict]:
    """
    Devuelve [{tm_player_id, player_name, market_value_eur, tm_player_url, club_id, season}]
    """
    for url in build_roster_urls(club_id, season):
        try:
            soup = get_html(url)
        except requests.HTTPError as e:
            # probar siguiente variante si 404/403
            if e.response is not None and e.response.status_code in (403, 404):
                continue
            else:
                raise
        table = soup.select_one("table.items")
        if not table:
            # página sin tabla (anti-bot u otra variante) -> probamos siguiente
            continue

        out = []
        for row in table.select("tbody tr"):
            a = row.select_one("td a[href*='/profil/spieler/']")
            if not a:
                a = row.select_one("td a[href*='/spieler/']")
            if not a:
                continue
            name = a.get_text(strip=True)
            href = urljoin(url, a.get("href", ""))
            m = re.search(r"/spieler/(\d+)", href)
            pid = int(m.group(1)) if m else None

            mv_cell = row.select_one("td.rechts.hauptlink, td.right.aligned.hauptlink")
            mv_txt = mv_cell.get_text(" ", strip=True) if mv_cell else ""
            eur = to_eur(mv_txt)

            out.append({
                "tm_player_id": pid,
                "player_name": name,
                "market_value_eur": eur,
                "tm_player_url": href,
                "club_id": club_id,
                "season": season
            })
        time.sleep(max(0.25, sleep))
        return out  # usamos la primera variante que funcionó
    # si ninguna variante funcionó:
    return []

def main(league_url: str, out_path: str, sleep: float, include_history: bool = False):
    season = parse_season(league_url)
    if not season:
        raise SystemExit("No pude detectar /saison_id/<YYYY> en la URL de la liga.")
    print(f"• Leyendo clubes de la liga {league_url} (season={season}) …")
    clubs = get_club_ids_from_league(league_url)
    print(f"  Clubs detectados: {len(clubs)}")

    all_rows = []
    ok = 0
    for cid in clubs:
        rows = parse_club_roster(cid, season, sleep=sleep)
        if rows:
            ok += 1
            all_rows.extend(rows)
        else:
            print(f"[WARN] club {cid}: no pude extraer el plantel.")
    print(f"Resumen: clubs OK={ok}/{len(clubs)}, jugadores={len(all_rows)}")

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["tm_player_id", "season"])
    df["last_update"] = pd.to_datetime(date.today()).date()
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"OK -> {out}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--league-url", required=True,
                    help="URL de la liga en Transfermarkt (en) con /saison_id/<YYYY>")
    ap.add_argument("--out", required=True, help="Ruta CSV de salida")
    ap.add_argument("--sleep", type=float, default=1.0, help="Segundos entre requests por club")
    ap.add_argument("--history", action="store_true", help="(Reservado) incluir histórico")
    args = ap.parse_args()
    main(args.league_url, args.out, args.sleep, include_history=args.history)
