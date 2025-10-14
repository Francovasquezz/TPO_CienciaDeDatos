# scripts/player_xref_builder.py
import argparse, re, unicodedata
from pathlib import Path
import pandas as pd
from rapidfuzz import process, fuzz

def norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(ch for ch in unicodedata.normalize("NFKD", s) if ch.isascii())
    s = re.sub(r"\s+", " ", s)
    return s

def canon_date(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)): return ""
    digits = re.sub(r"[^0-9]", "", str(s))
    if len(digits) >= 8: return digits[:8]   # YYYYMMDD
    if len(digits) >= 4: return digits[:4]   # YYYY
    return ""

def load_fbref(path: Path, league: str|None, season: str|None) -> pd.DataFrame:
    df = pd.read_csv(path)
    # renombres tolerantes
    if "name" not in df.columns and "Player" in df.columns: df = df.rename(columns={"Player":"name"})
    if "dob" not in df.columns and "Born" in df.columns:    df = df.rename(columns={"Born":"dob"})
    # nationality puede venir como Nation / Nationality / country
    for cand in ["nationality","Nation","Nationality","country"]:
        if cand in df.columns:
            if cand != "nationality":
                df = df.rename(columns={cand:"nationality"})
            break
    if "nationality" not in df.columns:
        df["nationality"] = ""

    # filtrar si están las columnas
    if league and "league_code" in df.columns:
        df = df[df["league_code"].astype(str).str.upper().eq(league.upper())]
    if season and "season" in df.columns:
        df = df[df["season"].astype(str).eq(str(season))]

    df["name_norm"] = df["name"].map(norm)
    df["dob_key"]   = df["dob"].map(canon_date)
    return df

def load_tm(path: Path) -> pd.DataFrame:
    tm = pd.read_csv(path)
    if "nationality" not in tm.columns:
        # en nuestro import consolidado debería existir; si no, dejamos vacío
        tm["nationality"] = ""
    tm["name_norm"] = tm["name"].map(norm)
    tm["dob_key"]   = tm["dob"].map(canon_date)
    return tm

def build_xref(fb: pd.DataFrame, tm: pd.DataFrame, fuzzy_threshold=92):
    fb["k1"] = fb["name_norm"] + "|" + fb["dob_key"]
    tm["k1"] = tm["name_norm"] + "|" + tm["dob_key"]

    right_cols = ["k1","tm_player_id","market_value_eur","last_update","nationality"]
    right_cols = [c for c in right_cols if c in tm.columns]  # por si falta algo
    exact = fb.merge(tm[right_cols], on="k1", how="left")

    # coalesce nationality (por si quedó _x/_y)
    if "nationality" not in exact.columns:
        nat_cols = [c for c in exact.columns if c.startswith("nationality")]
        if "nationality_x" in nat_cols or "nationality_y" in nat_cols:
            exact["nationality"] = exact.get("nationality_x", pd.Series(dtype=object)).fillna(
                exact.get("nationality_y", pd.Series(dtype=object))
            )
            # limpia
            drop_cols = [c for c in ["nationality_x","nationality_y"] if c in exact.columns]
            if drop_cols:
                exact.drop(columns=drop_cols, inplace=True)
        else:
            # si no hay en ninguna, creamos vacía
            exact["nationality"] = ""

    # Fuzzy para filas sin tm_id
    exact["fuzzy_score"] = pd.NA
    pending = exact[exact["tm_player_id"].isna()].copy()
    if not pending.empty:
        # índice por nacionalidad del TM
        tm_by_nat = {}
        for nat, chunk in tm.groupby(tm["nationality"].fillna("").map(norm)):
            tm_by_nat[nat] = list(zip(chunk["name_norm"], chunk["tm_player_id"], chunk["market_value_eur"], chunk["last_update"]))
        # fallback global
        global_pool = list(zip(tm["name_norm"], tm["tm_player_id"], tm["market_value_eur"], tm["last_update"]))

        for i, row in pending.iterrows():
            name = row["name_norm"]
            nat  = norm(row.get("nationality",""))
            pool = tm_by_nat.get(nat, []) or global_pool
            names = [p[0] for p in pool]
            choice = process.extractOne(name, names, scorer=fuzz.WRatio)
            if choice and choice[1] >= fuzzy_threshold:
                idx = choice[2]
                pid, val, upd = pool[idx][1], pool[idx][2], pool[idx][3]
                exact.loc[i, "tm_player_id"] = pid
                exact.loc[i, "market_value_eur"] = val
                exact.loc[i, "last_update"] = upd
                exact.loc[i, "fuzzy_score"] = choice[1]

    # UUID simple (podés cambiar a hash estable si querés)
    uu = (exact["name_norm"].fillna("") + "|" + exact["dob_key"].fillna(""))
    exact["player_uuid"] = uu.where(uu != "", None)
    return exact

def main(fbref_csv: str, tm_csv: str, outdir: str, league: str|None, season: str|None):
    out = Path(outdir); out.mkdir(parents=True, exist_ok=True)
    fb = load_fbref(Path(fbref_csv), league, season)
    tm = load_tm(Path(tm_csv))
    joined = build_xref(fb, tm)

    # Dump completos
    joined.to_csv(out/"player_values_joined.csv", index=False)

    # XREF resumido siempre con columna nationality (ya coalescida)
    cols = ["player_uuid","name","dob","nationality","tm_player_id","market_value_eur","last_update","fuzzy_score"]
    for c in cols:
        if c not in joined.columns:
            joined[c] = pd.NA
    joined[cols].to_csv(out/"player_xref.csv", index=False)

    print("OK ->", out/"player_values_joined.csv")
    print("OK ->", out/"player_xref.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fbref", default="data/processed/fbref_players.csv")
    ap.add_argument("--tm",    default="data/processed/tm_market_values_latest.csv")
    ap.add_argument("--out",   default="data/processed")
    ap.add_argument("--league", default=None)
    ap.add_argument("--season", default=None)
    args = ap.parse_args()
    main(args.fbref, args.tm, args.out, args.league, args.season)
