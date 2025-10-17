# scripts/tm_pull_latest_values_squads.py
import argparse, csv, os, random, sys, time, urllib.parse
from typing import List, Tuple, Dict
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
]

def session_with_retries() -> requests.Session:
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.7,
                    status_forcelist=(429, 500, 502, 503, 504),
                    allowed_methods=("GET", "HEAD"),
                    raise_on_status=False)
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept-Language": "en-US,en;q=0.8",
        "Cache-Control": "no-cache",
    })
    return s

def normalize_value_eur(txt: str):
    if not txt: return None
    t = txt.replace("€", "").replace(",", "").strip()
    if t == "-": return None
    mult = 1
    tl = t.lower()
    if tl.endswith("bn"): mult, core = 1_000_000_000, t[:-2]
    elif tl.endswith("m"): mult, core = 1_000_000, t[:-1]
    elif tl.endswith("th."): mult, core = 1_000, t[:-3]
    elif tl.endswith("k"): mult, core = 1_000, t[:-1]
    else: core = t
    try:
        return int(float(core) * mult)
    except Exception:
        return None

def extract_text_or_img_alt(a_tag):
    if not a_tag: return None
    txt = a_tag.get_text(strip=True)
    if txt: return txt
    title = a_tag.get("title")
    if title and title.strip(): return title.strip()
    img = a_tag.find("img")
    if img and img.get("alt"): return img.get("alt").strip()
    return None

def find_club_links_on_league(startseite_url: str, s: requests.Session) -> Dict[str, tuple]:
    r = s.get(startseite_url, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    clubs = {}
    for a in soup.select("a[href*='/verein/']"):
        href = a.get("href", "")
        if "/verein/" not in href: 
            continue
        parts = href.split("/")
        try:
            idx = parts.index("verein")
            club_id = parts[idx+1]
        except Exception:
            continue
        name = extract_text_or_img_alt(a) or f"club_{club_id}"
        abs_url = urllib.parse.urljoin("https://www.transfermarkt.com", href)
        clubs[club_id] = (name, abs_url)
    return clubs

def to_squad_url(any_url: str, season_id: int) -> str:
    parts = any_url.split("/")
    base = "https://www.transfermarkt.com"
    if "verein" in parts:
        idx = parts.index("verein")
        club_id = parts[idx+1]
        slug = parts[3] if parts[0].startswith("https") and len(parts) > 3 else "club"
        return f"{base}/{slug}/kader/verein/{club_id}/plus/1/galerie/0?saison_id={season_id}"
    return any_url

def scrape_squad(squad_url: str, s: requests.Session) -> List[Tuple[str, str, int]]:
    r = s.get(squad_url, timeout=30)
    if r.status_code != 200:
        return []
    soup = BeautifulSoup(r.text, "lxml")
    club_name = None
    h1 = soup.select_one("h1")
    if h1:
        club_name = h1.get_text(" ", strip=True)
    if not club_name:
        bc = soup.select_one("div.dataBild > img[alt]")
        if bc and bc.get("alt"): club_name = bc.get("alt").strip()

    rows = []
    for tr in soup.select("table.items > tbody > tr"):
        a_player = tr.select_one("td a[href*='/profil/spieler/']")
        mv_td    = tr.select_one("td.rechts.hauptlink")
        if not (a_player and mv_td): 
            continue
        player = a_player.get_text(strip=True)
        price  = normalize_value_eur(mv_td.get_text(strip=True))
        if player and price is not None:
            rows.append((player, club_name or "", price))
    return rows

def main():
    ap = argparse.ArgumentParser(description="Liga → clubes → planteles → (player, club, price)")
    ap.add_argument("--league-clubs-url", required=True)
    ap.add_argument("--season-id", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--sleep", type=float, default=0.7)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    s = session_with_retries()
    clubs = find_club_links_on_league(args.league_clubs_url, s)
    if not clubs:
        os.makedirs("data/tmp", exist_ok=True)
        with open("data/tmp/league_snapshot.html", "w", encoding="utf-8") as f:
            f.write(s.get(args.league_clubs_url, timeout=30).text)
        print("No se detectaron clubes. Snapshot en data/tmp/league_snapshot.html", file=sys.stderr)
        sys.exit(1)

    all_rows, seen = [], set()
    for club_id, (club_name, any_url) in clubs.items():
        squad_url = to_squad_url(any_url, args.season_id)
        rows = scrape_squad(squad_url, s)
        for p, c, v in rows:
            key = (p, c)
            if key in seen: 
                continue
            seen.add(key)
            all_rows.append((p, c, v))
        time.sleep(args.sleep + random.random()*0.6)

    with open(args.out, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f); w.writerow(["player_name","club_name","market_value_eur"]); w.writerows(all_rows)

    print(f"Wrote {len(all_rows)} rows → {args.out}")

if __name__ == "__main__":
    main()
