# scripts/tm_pull_latest_values_playwright.py
# Liga (Premier o la que sea) → clubes → planteles → CSV listo para join
# Basado en la versión AR "probada", con parámetros para EPL y dominio.

# ============================================================
# Transfermarkt scraper — Liga → clubes → planteles → valores €
# ------------------------------------------------------------
# Propósito
#   Dada una liga (p.ej. Premier League) y una temporada TM (season_id),
#   navega con Playwright a Transfermarkt, detecta clubes y extrae el
#   plantel de cada club con el “market value” actual. Diseñado para
#   ser reutilizable (com/.com.ar), persistir cookies y tolerar 403/tiempos.
#
# Entradas (flags)
#   --league        : alias/código opcional (ENG1/EPL/GB1/ARG1, etc.). Sólo usado
#                     para nombrar el archivo de salida; el scraping usa URLs.
#   --season        : temporada TM como 'YYYY-YYYY' o 'YYYY'. Internamente se
#                     convierte a season_id='YYYY'. Para PL 24/25 usar '2024' o '2024-2025'.
#   --tm-domain     : dominio TM (com, com.ar, de, es...). Default: com
#   --league-url    : URL de la liga (opcional). Si no se pasa, se construyen
#                     URLs típicas de /premier-league/.../GB1?saison_id=YYYY.
#   --out           : CSV de salida (si no, se autogenera en data/processed).
#   --parquet       : además del CSV guarda Parquet.
#   --profile-dir   : directorio de perfil persistente Playwright (cookies OneTrust).
#   --use-chrome    : usar canal “chrome” (en lugar de Chromium embedded).
#   --no-headless   : mostrar navegador (útil 1ra vez para aceptar cookies).
#   --delay         : delay base entre clubes (anti-bot leve + jitter).
#   --max-retries   : reintentos por club (cambia de dominio com↔com.ar si falla).
#
# Salida
#   CSV/Parquet con columnas:
#     player_name, club_name, market_value_eur, player_id, dob, age
#   (nombres pensados para join posterior; market_value_eur es EUR como número)
#
# Flujo (resumen)
#   1) Playwright launch PERSISTENTE (perfil) → cookies se guardan en <profile-dir>.
#   2) Detección de clubes desde la página de liga:
#        - intenta varias rutas (/plus/1, query vs segment, /tabelle/…)
#        - selectores + fallback por regex del HTML (robusto multi-idioma)
#   3) Por cada club:
#        - deriva URL de “kader” (roster) estable:
#            /<slug>/kader/verein/{id}/saison_id/{YYYY}/plus/1/galerie/0
#          (con variantes de fallback)
#        - parsea tabla “table.items” → nombre jugador + valor de mercado
#        - normaliza “€”, “m/Mio.”, “Th./k”, etc. → market_value_eur (float/int)
#        - reintenta y puede alternar dominio com ↔ com.ar si aparece 403
#   4) CSV/Parquet en data/processed/tm_values_<CODE>_<YYYY>_latest.*
#
# Particularidades / gotchas
#   - **season (TM)**: usar '2024' para 24/25. El scraper acepta '2024-2025'
#     pero convierte internamente a '2024'.
#   - Cookies: la 1ra corrida conviene con --no-headless para aceptar OneTrust;
#     luego headless va fluido (perfil persistente).
#   - Anti-bot: reintentos, backoff exponencial corto y “mismo page” para simular sesión humana.
#   - Diferencias por idioma: parsing tolera “Mio.”/“mill.”/“Th.”/“k”, etc.
#
# Ejemplos
#   Premier League 24/25 (TM usa season_id=2024):
#     python scripts/tm_pull_latest_values_playwright.py --league ENG1 --season 2024 --tm-domain com.ar --headful
#     # luego ya podés sin headful:
#     python scripts/tm_pull_latest_values_playwright.py --league ENG1 --season 2024 --tm-domain com.ar --parquet
# ============================================================


import argparse
import csv
import os
import pathlib
import random
import re
import time
from datetime import datetime
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

# -------------------- Helpers de parámetros --------------------

LEAGUE_ALIASES = {
    "ENG1": ("premier-league", "GB1"),
    "EPL":  ("premier-league", "GB1"),
    "GB1":  ("premier-league", "GB1"),
    "ARG1": ("primera-division", "AR1N"),
    "AR1N": ("primera-division", "AR1N"),
    # podés sumar más ligas acá si querés reutilizar
}

def season_to_tm(s: str) -> int:
    s = str(s).strip()
    m = re.match(r"^(\d{4})\s*[-/]\s*\d{4}$", s)
    if m:
        return int(m.group(1))
    m2 = re.match(r"^\d{4}$", s)
    if m2:
        return int(m2.group(0))
    raise ValueError(f"Temporada inválida '{s}' (use 'YYYY-YYYY' o 'YYYY').")

def build_league_url(league: str = None, tm_domain: str = "com", league_url: str = None, season_id: int = None) -> str:
    """
    Si pasás league (ENG1), arma:
      https://www.transfermarkt.<dominio>/<slug>/startseite/wettbewerb/<code>?saison_id=<year>
    Si pasás league_url, respeta ese URL y sólo asegura que tenga ?saison_id=...
    """
    if league_url:
        u = urlparse(league_url)
        base = urlunparse((u.scheme or "https", u.netloc or f"www.transfermarkt.{tm_domain}", u.path, "", "", ""))
        if season_id:
            sep = "&" if "?" in base else "?"
            return f"{base}{sep}saison_id={season_id}"
        return base

    if not league:
        raise ValueError("Debes pasar --league o --league-url")

    slug, code = LEAGUE_ALIASES.get(league.upper(), (None, None))
    if not slug or not code:
        raise ValueError(f"Liga '{league}' no soportada en este atajo. Pasá --league-url manualmente.")
    root = f"https://www.transfermarkt.{tm_domain}"
    return f"{root}/{slug}/startseite/wettbewerb/{code}?saison_id={season_id or ''}".rstrip("?")

# -------------------- Utilidades parsing --------------------

def normalize_value_eur(txt: str):
    """Soporta: '20,00 mill. €', '800 mil €', '€20.0m', '€800k', '12.3m', '1.2bn', '1,200,000'."""
    if not txt:
        return None
    t = txt.strip().lower()
    # normalizaciones multi-idioma
    t = t.replace("€", "").replace("eur", "").replace("\u2009", "")
    t = t.replace("mill.", "m").replace("mio.", "m")
    t = re.sub(r"\bmil\b\.?", "k", t)
    t = t.replace(" ", "").replace(",", ".")
    if t == "-" or t == "":
        return None

    mult = 1
    if t.endswith("bn"):
        mult, core = 1_000_000_000, t[:-2]
    elif t.endswith("m"):
        mult, core = 1_000_000, t[:-1]
    elif t.endswith("k") or t.endswith("th."):
        mult, core = 1_000, t[:-1] if t.endswith("k") else t[:-3]
    else:
        core = t

    try:
        return int(float(core) * mult)
    except Exception:
        return None

def click_cookies_if_any(page):
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
            time.sleep(0.3)
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

# -------------------- Liga → links de clubes --------------------

def get_club_links(page, league_url: str) -> Dict[str, Tuple[str, str]]:
    """
    Devuelve: { club_id: (club_name, href_abs_startseite) }
    """
    page.goto(league_url, wait_until="domcontentloaded")
    click_cookies_if_any(page)
    wait_dom_ready(page)
    page.mouse.wheel(0, 20000)  # por si hay lazy-load
    wait_dom_ready(page)

    u = urlparse(league_url)
    root = f"{u.scheme or 'https'}://{u.netloc}"

    clubs = {}
    anchors = page.locator("a[href*='/startseite/verein/']").all()
    if not anchors:
        # fallback genérico
        anchors = page.locator("a[href*='/verein/']").all()

    for a in anchors:
        href = a.get_attribute("href") or ""
        m = re.search(r"/verein/(\d+)", href)
        if not m:
            continue
        club_id = m.group(1)
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
        abs_url = urljoin(root, href)
        # nos quedamos con el primer link a /startseite/verein/…
        if club_id not in clubs and "/startseite/verein/" in abs_url:
            clubs[club_id] = (name, abs_url)

    return clubs

def to_squad_url(any_startseite_url: str, season_id: int) -> str:
    """
    Deriva una URL estable de plantilla desde un href real de /startseite/verein/<id>
    Conserva el slug del club (evita 404/403 por slug inventado).
    """
    u = urlparse(any_startseite_url)
    # limpiamos cualquier query/segmento de saison_id anterior
    path = re.sub(r"/saison_id/\d+", "", u.path)
    query = re.sub(r"([?&])saison_id=\d+", r"\1", u.query)
    query = re.sub(r"[?&]+$", "", query)

    m = re.search(r"^(.*)/startseite/verein/(\d+)", path)
    if not m:
        return any_startseite_url
    prefix, club_id = m.group(1), m.group(2)
    p = f"{prefix}/kader/verein/{club_id}/saison_id/{season_id}/plus/1/galerie/0"
    return urlunparse((u.scheme, u.netloc, p, "", "", ""))

# -------------------- DOB / AGE helpers --------------------

def _parse_dob_to_iso(txt: str) -> str:
    if not txt:
        return ""
    txt = txt.strip()
    m = re.search(r"(\d{1,2})[\/\.-](\d{1,2})[\/\.-](\d{4})", txt)
    if m:
        d, mth, y = map(int, m.groups())
        try:
            return datetime(y, mth, d).strftime("%Y-%m-%d")
        except ValueError:
            return ""
    m = re.search(r"([A-Za-z]{3,})\s+(\d{1,2}),\s*(\d{4})", txt)
    if m:
        mon_map = {m: i for i, m in enumerate(
            ["january","february","march","april","may","june",
             "july","august","september","october","november","december"], 1)}
        mon = mon_map.get(m.group(1).lower())
        if mon:
            try:
                return datetime(int(m.group(3)), mon, int(m.group(2))).strftime("%Y-%m-%d")
            except ValueError:
                return ""
    return ""

def _extract_dob_age_from_tr(tr):
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
        m_age = re.search(r"\((\d{1,2})\)", txt)
        if m_age and not age_val:
            try:
                age_val = int(m_age.group(1))
            except Exception:
                age_val = ""
        ds = td.get_attribute("data-sort")
        if ds and re.match(r"\d{4}-\d{2}-\d{2}$", ds) and not dob_iso:
            dob_iso = ds
        if not dob_iso:
            guess = _parse_dob_to_iso(txt)
            if guess:
                dob_iso = guess
        if age_val and dob_iso:
            break
    return dob_iso, age_val

# -------------------- scraping de una plantilla --------------------

def scrape_squad(page, squad_url: str, club_id: str, outdir_snap: str) -> List[tuple]:
    tries = 3
    last_err = None
    for attempt in range(1, tries + 1):
        try:
            page.goto(squad_url, wait_until="domcontentloaded")
            click_cookies_if_any(page)
            wait_dom_ready(page)
            page.mouse.wheel(0, 8000)  # lazy-load
            wait_dom_ready(page)
            page.locator("table.items").first.wait_for(state="visible", timeout=20000)

            # club
            club_name = ""
            try:
                club_name = page.locator("h1").first.inner_text().strip()
            except Exception:
                try:
                    club_name = (page.locator("div.dataBild img[alt]").first.get_attribute("alt") or "").strip()
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

                href = a_player.get_attribute("href") or ""
                m_id = re.search(r"/spieler/(\d+)", href)
                player_id = m_id.group(1) if m_id else ""

                mv_td = tr.locator("td.rechts.hauptlink").first
                if mv_td.count() == 0:
                    continue
                price = normalize_value_eur(mv_td.inner_text().strip())

                dob_iso, age_val = _extract_dob_age_from_tr(tr)

                if player and price is not None:
                    rows.append((player, club_name, price, player_id, dob_iso, age_val))

            return rows
        except Exception as e:
            last_err = e
            os.makedirs(outdir_snap, exist_ok=True)
            page.screenshot(path=os.path.join(outdir_snap, f"squad_{club_id}_try{attempt}.png"), full_page=True)
            try:
                html = page.content()
                with open(os.path.join(outdir_snap, f"squad_{club_id}_try{attempt}.html"), "w", encoding="utf-8") as f:
                    f.write(html)
            except Exception:
                pass
            time.sleep(1.2 * attempt)
    print(f"[WARN] {squad_url} failed after retries: {last_err}")
    return []

# -------------------- Main --------------------

def main():
    ap = argparse.ArgumentParser(description="Liga → clubes → planteles con Playwright (persistente)")
    ap.add_argument("--league", help="Alias (ENG1/EPL/GB1) o usar --league-url")
    ap.add_argument("--league-url", help="URL de la liga en TM (si querés pasarla directo)")
    ap.add_argument("--tm-domain", default="com", help="Dominio TM: com, com.ar, de, es…")
    ap.add_argument("--season", required=True, help="Temporada: '2024-2025' o '2024'")
    ap.add_argument("--out", required=False, help="Ruta CSV de salida")
    ap.add_argument("--headful", action="store_true", help="Mostrar navegador (default headless)")
    ap.add_argument("--parquet", action="store_true", help="Guardar Parquet además del CSV")
    args = ap.parse_args()

    season_id = season_to_tm(args.season)

    # armar URL de liga (si no la pasó)
    league_url = build_league_url(args.league, args.tm_domain, args.league_url, season_id)
    u = urlparse(league_url)
    base_root = f"{u.scheme}://{u.netloc}"

    # salida
    if args.out:
        out_csv = args.out
    else:
        code = "GB1" if "/GB1" in league_url else "LEAGUE"
        out_csv = f"data/processed/tm_values_{code}_{season_id}_latest.csv"
    os.makedirs(os.path.dirname(out_csv), exist_ok=True)

    outdir_snap = "data/tmp"
    pathlib.Path(outdir_snap).mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        # Perfil persistente: cookies quedan guardadas en .pw-profile
        user_data_dir = ".pw-profile"
        context = pw.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=(not args.headful),
            viewport={"width": 1366, "height": 900},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36"),
            locale="en-GB",
            timezone_id="Europe/London",
        )
        page = context.new_page()

        # 1) Liga -> clubes
        clubs = get_club_links(page, league_url)
        if not clubs:
            page.screenshot(path=os.path.join(outdir_snap, "league_snapshot.png"), full_page=True)
            with open(os.path.join(outdir_snap, "league_snapshot.html"), "w", encoding="utf-8") as f:
                f.write(page.content())
            print(f"[ERROR] No clubs found. Snapshots in {outdir_snap}/")
            context.close()
            return

        # 2) Club -> plantilla (derivado del href real)
        seen = set()
        all_rows = []
        for club_id, (club_name, start_href) in clubs.items():
            # forzar dominio elegido por si league_url está en otro dominio
            su = urlparse(start_href)
            start_href = urlunparse((su.scheme, u.netloc, su.path, "", "", ""))

            squad_url = to_squad_url(start_href, season_id)
            rows = scrape_squad(page, squad_url, club_id, outdir_snap)
            if not rows:
                # intento alterno: ?saison_id=...
                alt = start_href + ("&" if "?" in start_href else "?") + f"saison_id={season_id}"
                squad_url = to_squad_url(alt, season_id)
                rows = scrape_squad(page, squad_url, club_id, outdir_snap)

            for p, c, v, pid, dob, age in rows:
                key = (p, c, pid or dob or "")
                if key in seen:
                    continue
                seen.add(key)
                all_rows.append((p, c, v, pid, dob, age))

            time.sleep(0.5 + random.random() * 0.6)

        # 3) Guardado
        with open(out_csv, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f)
            w.writerow(["player_name", "club_name", "market_value_eur", "player_id", "dob", "age"])
            w.writerows(all_rows)

        print(f"[OK] Wrote {len(all_rows)} rows → {out_csv}")
        context.close()

    if args.parquet:
        import pandas as pd
        df = pd.DataFrame(all_rows, columns=["player_name", "club_name", "market_value_eur", "player_id", "dob", "age"])
        pq = os.path.splitext(out_csv)[0] + ".parquet"
        df.to_parquet(pq, index=False)
        print(f"[OK] Parquet: {pq}")

if __name__ == "__main__":
    main()
