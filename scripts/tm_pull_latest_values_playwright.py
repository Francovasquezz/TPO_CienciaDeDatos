# scripts/tm_pull_latest_values_playwright.py
# Liga → clubes → planteles → (player_name, club_name, market_value_eur, player_id, dob, age)
# Robusto: Playwright (Chromium), perfil persistente, cookies, retries, snapshots.
#
# Uso (PowerShell):
#   python scripts\tm_pull_latest_values_playwright.py ^
#     --league-clubs-url "https://www.transfermarkt.com/primera-division/startseite/wettbewerb/AR1N" ^
#     --season-id 2024 ^
#     --out "data/processed/tm_values_AR1N_2024_latest.csv" ^
#     --headful

import argparse
import csv
import os
import pathlib
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse

from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

BASE = "https://www.transfermarkt.com"


def normalize_value_eur(txt: str):
    if not txt:
        return None
    t = txt.replace("€", "").replace(",", "").strip()
    if t == "-":
        return None
    mult = 1
    tl = t.lower()
    if tl.endswith("bn"):
        mult, core = 1_000_000_000, t[:-2]
    elif tl.endswith("m"):
        mult, core = 1_000_000, t[:-1]
    elif tl.endswith("th."):
        mult, core = 1_000, t[:-3]
    elif tl.endswith("k"):
        mult, core = 1_000, t[:-1]
    else:
        core = t
    try:
        return int(float(core) * mult)
    except Exception:
        return None


def click_cookies_if_any(page):
    # Varias variantes de OneTrust / consent
    selectors = [
        "#onetrust-accept-btn-handler",
        "button#onetrust-accept-btn-handler",
        "button[aria-label*='Accept']",
        "button:has-text('Accept all')",
        "button:has-text('I agree')",
        "button:has-text('Aceptar')",
    ]
    for sel in selectors:
        try:
            page.locator(sel).first.wait_for(state="visible", timeout=2500)
            page.locator(sel).first.click()
            time.sleep(0.5)
            return True
        except Exception:
            continue
    return False


def wait_dom_ready(page):
    page.wait_for_load_state("domcontentloaded")
    try:
        page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass


def get_club_links(page, league_url: str) -> Dict[str, Tuple[str, str]]:
    page.goto(league_url, wait_until="domcontentloaded")
    click_cookies_if_any(page)
    wait_dom_ready(page)
    # algunos listados cargan más al scrollear
    page.mouse.wheel(0, 20000)
    wait_dom_ready(page)

    clubs = {}
    anchors = page.locator("a[href*='/verein/']").all()
    for a in anchors:
        href = a.get_attribute("href") or ""
        m = re.search(r"/verein/(\d+)", href)
        if not m:
            continue
        club_id = m.group(1)
        # nombre del club (texto o alt de img)
        name = (a.inner_text() or "").strip()
        if not name:
            try:
                img = a.locator("img").first
                if img and img.count() > 0:
                    alt = img.get_attribute("alt")
                    if alt:
                        name = alt.strip()
            except Exception:
                pass
        name = name or f"club_{club_id}"
        abs_url = urljoin(BASE, href)
        clubs[club_id] = (name, abs_url)
    return clubs


def to_squad_url(any_url: str, season_id: int) -> str:
    # ruta estable de squad: /{slug}/kader/verein/{id}/saison_id/{season}/plus/1/galerie/0
    path = urlparse(any_url).path.strip("/")
    parts = path.split("/")
    slug = parts[0] if parts else "club"
    m = re.search(r"/verein/(\d+)", "/" + path)
    if not m:
        return any_url
    club_id = m.group(1)
    return f"{BASE}/{slug}/kader/verein/{club_id}/saison_id/{season_id}/plus/1/galerie/0"


# ------------------ DOB / AGE helpers ------------------ #
def _parse_dob_to_iso(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.strip()
    # dd/mm/yyyy o dd.mm.yyyy
    m = re.search(r"(\d{1,2})[\/\.-](\d{1,2})[\/\.-](\d{4})", txt)
    if m:
        d, mth, y = map(int, m.groups())
        try:
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    # Month dd, yyyy (en)
    m = re.search(r"([A-Za-z]{3,})\s+(\d{1,2}),\s*(\d{4})", txt)
    if m:
        mon_map = {
            m: i
            for i, m in enumerate(
                [
                    "january",
                    "february",
                    "march",
                    "april",
                    "may",
                    "june",
                    "july",
                    "august",
                    "september",
                    "october",
                    "november",
                    "december",
                ],
                1,
            )
        }
        mon = mon_map.get(m.group(1).lower())
        if mon:
            try:
                return datetime(int(m.group(3)), mon, int(m.group(2))).strftime(
                    "%Y-%m-%d"
                )
            except ValueError:
                return ""
    return ""


def _extract_dob_age_from_tr(tr):
    """Intenta leer DOB/Edad de la celda de nacimiento (suele contener '(edad)').
    No dependemos de selectores exactos de idioma/columna; buscamos el primer TD cuyo texto contenga '(NN)'.
    """
    dob_iso, age_val = "", ""
    tds = tr.locator("td")
    try:
        count = tds.count()
    except Exception:
        count = 0

    for j in range(count):
        td = tds.nth(j)
        try:
            txt = (td.inner_text() or "").strip()
        except Exception:
            continue
        # edad entre paréntesis
        m_age = re.search(r"\((\d{1,2})\)", txt)
        if m_age and not age_val:
            try:
                age_val = int(m_age.group(1))
            except Exception:
                age_val = ""
        # data-sort con fecha ISO
        ds = td.get_attribute("data-sort")
        if ds and re.match(r"\d{4}-\d{2}-\d{2}$", ds) and not dob_iso:
            dob_iso = ds
        # si no hay data-sort, intentar parsear el texto
        if not dob_iso:
            guess = _parse_dob_to_iso(txt)
            if guess:
                dob_iso = guess

        if age_val and dob_iso:
            break

    return dob_iso, age_val


# ------------------ scraping de una plantilla ------------------ #
def scrape_squad(page, squad_url: str, club_id: str, outdir_snap: str) -> List[tuple]:
    tries = 3
    last_err = None
    for attempt in range(1, tries + 1):
        try:
            page.goto(squad_url, wait_until="domcontentloaded")
            click_cookies_if_any(page)
            wait_dom_ready(page)
            # scrolleo suave por si hay lazy-load
            page.mouse.wheel(0, 8000)
            wait_dom_ready(page)
            # esperar tabla
            page.locator("table.items").first.wait_for(
                state="visible", timeout=20000
            )

            # club
            club_name = ""
            try:
                club_name = page.locator("h1").first.inner_text().strip()
            except Exception:
                try:
                    club_name = (
                        page.locator("div.dataBild img[alt]")
                        .first.get_attribute("alt")
                        or ""
                    )
                except Exception:
                    club_name = ""

            rows = []
            trs = page.locator("table.items > tbody > tr")
            n = trs.count()
            for i in range(n):
                tr = trs.nth(i)
                a_player = tr.locator("td a[href*='/profil/spieler/']").first
                if a_player.count() == 0:
                    continue
                player = a_player.inner_text().strip()

                # player_id desde href
                href = a_player.get_attribute("href") or ""
                m_id = re.search(r"/spieler/(\d+)", href)
                player_id = m_id.group(1) if m_id else ""

                # precio
                mv_td = tr.locator("td.rechts.hauptlink").first
                if mv_td.count() == 0:
                    continue
                price = normalize_value_eur(mv_td.inner_text().strip())

                # DOB y edad
                dob_iso, age_val = _extract_dob_age_from_tr(tr)

                if player and price is not None:
                    rows.append((player, club_name, price, player_id, dob_iso, age_val))

            return rows
        except Exception as e:
            last_err = e
            # snapshot para debug
            os.makedirs(outdir_snap, exist_ok=True)
            page.screenshot(
                path=os.path.join(outdir_snap, f"squad_{club_id}_try{attempt}.png"),
                full_page=True,
            )
            try:
                html = page.content()
                with open(
                    os.path.join(outdir_snap, f"squad_{club_id}_try{attempt}.html"),
                    "w",
                    encoding="utf-8",
                ) as f:
                    f.write(html)
            except Exception:
                pass
            time.sleep(1.2 * attempt)  # backoff
    # si agotamos
    print(f"[WARN] {squad_url} failed after retries: {last_err}")
    return []


def main():
    ap = argparse.ArgumentParser(
        description="Liga → clubes → planteles → (player, club, price, player_id, dob, age) con Playwright"
    )
    ap.add_argument("--league-clubs-url", required=True)
    ap.add_argument("--season-id", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument(
        "--headful", action="store_true", help="Mostrar navegador (default headless)"
    )
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    outdir_snap = "data/tmp"
    pathlib.Path(outdir_snap).mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        # Perfil persistente: cookies quedan guardadas en .pw-profile
        user_data_dir = ".pw-profile"
        context = pw.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=(not args.headful),
            viewport={"width": 1366, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"
            ),
            locale="en-GB",
            timezone_id="Europe/London",
        )
        page = context.new_page()

        clubs = get_club_links(page, args.league_clubs_url)
        if not clubs:
            page.screenshot(
                path=os.path.join(outdir_snap, "league_snapshot.png"), full_page=True
            )
            with open(
                os.path.join(outdir_snap, "league_snapshot.html"),
                "w",
                encoding="utf-8",
            ) as f:
                f.write(page.content())
            print("No clubs found. Snapshots saved in data/tmp/")
            context.close()
            return

        seen = set()
        all_rows = []
        for club_id, (club_name, any_url) in clubs.items():
            squad_url = to_squad_url(any_url, args.season_id)
            rows = scrape_squad(page, squad_url, club_id, outdir_snap)
            for p, c, v, pid, dob, age in rows:
                # de-dup más robusto usando ID si hay, o DOB en su defecto
                key = (p, c, pid or dob or "")
                if key in seen:
                    continue
                seen.add(key)
                all_rows.append((p, c, v, pid, dob, age))
            time.sleep(0.5 + random.random() * 0.6)

        with open(args.out, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                ["player_name", "club_name", "market_value_eur", "player_id", "dob", "age"]
            )
            w.writerows(all_rows)

        print(f"Wrote {len(all_rows)} rows → {args.out}")
        context.close()


if __name__ == "__main__":
    main()
