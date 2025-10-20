# scripts/join_tm_fbref.py  (v4.2, sin manual mapping)
# Cascada de joins:
# 1) player_norm + club_norm + birth_year
# 2) player_fl   + club_norm + birth_year
# 2b) player_fl  + club_norm + birth_year±1 (único en TM)
# 2c) player_fl  + dob (DOB exacto, ignora club)  <-- NUEVO
# 3) player_fl   + birth_year
# 3b) player_fl  + birth_year±1 (único en TM, ignora club)
# 4) player_norm + club_norm (sin año)
# 5) fuzzy por club (token_set_ratio >= 90)
# 6) fuzzy global (sin club), birth_year ±1, inequívoco
#
# Salida: dob ISO, breakdown por método.

# ============================================================
# JOIN FBref ↔ Transfermarkt — jugador/club/temporada
# ------------------------------------------------------------
# Propósito
#   Unir el dataset “clean” de FBref con los valores de mercado de
#   Transfermarkt por jugador/club/temporada, usando normalización de
#   nombre y varias heurísticas de emparejamiento (exactas y fuzzy).
#
# Entradas (flags)
#   --fbref    : CSV de FBref limpio (salida de backend/etl.py), p.ej.:
#                data/processed/player_stats_Premier_League_2024-2025.clean.csv
#   --tm       : CSV de TM (salida del scraper), p.ej.:
#                data/processed/tm_values_GB1_2024_latest.csv
#   --out      : CSV de salida con las columnas de FBref + TM_Value_EUR
#   --season-year           : 'YYYY' (ej. 2024 para 24/25). Se usa en claves auxiliares.
#   --fuzzy-global-thresh   : umbral de similitud (0–100) para emparejamiento fuzzy
#                             global (recomendado 90–94; default del repo).
#
# Salidas
#   - data/processed/<archivo_out>.csv  (dataset unido)
#   - data/tmp/unmatched_<LIGA>_<YYYY>.csv  (muestras no matcheadas para diagnóstico)
#   - Log de breakdown con conteo por heurística: 
#       first+last+birth_year, key+birth_year, key_no_year,
#       fuzzy_global(x%), fuzzy_club(x%), first+last+club+birth_year, etc.
#
# Heurísticas (resumen)
#   1) Exactas sobre claves normalizadas (first/last, birth_year, club).
#   2) “key” normalizada (nombre sin acentos/espacios) + (opcional) birth_year.
#   3) Fuzzy matching global (ratio ≥ umbral) y por club.
#   4) Fall-back: key sin año si no hay DOB.
#
# Buenas prácticas
#   - Alinear formatos de temporada: FBref usa '2024-2025'; TM → season_id '2024'.
#   - Mantener nombres de club consistentes (normalizador ya maneja “&”, apóstrofos,
#     acentos y espacios; los extremos pueden quedar en unmatched).
#   - Ajustar `--fuzzy-global-thresh` si hay muchos pocos matches o demasiados falsos.
#
# Ejemplos
#   Premier League 24/25:
#     python scripts/join_tm_fbref.py `
#       --fbref "data/processed/player_stats_Premier_League_2024-2025.clean.csv" `
#       --tm    "data/processed/tm_values_GB1_2024_latest.csv" `
#       --out   "data/processed/join_pl_2024_2025.csv" `
#       --season-year 2024 `
#       --fuzzy-global-thresh 92
#
#   (Cobertura esperada ~95–97% sin mapping manual; unmatched quedan en data/tmp/)
# ============================================================


import argparse, os, re
import pandas as pd
from unidecode import unidecode
from rapidfuzz import process, fuzz

# -------------------- normalización --------------------
def norm_txt(x: str) -> str:
    if x is None: return ""
    t = unidecode(str(x)).lower()
    t = re.sub(r"\([^)]*\)", " ", t)             # (LP), (SdE), etc.
    t = re.sub(r"\bclub\s+atletico\b", " ", t)   # "club atletico"
    t = re.sub(r"\bc\.?a\.?\b", " ", t)          # "c.a." / "ca"
    t = re.sub(r"[^a-z0-9\s]", " ", t)           # signos
    t = re.sub(r"\s+", " ", t).strip()
    return t

STOPWORDS = {"de","del","da","do","das","dos","la","las","los","san","santa","club","atletico"}
def first_last_key(name_norm: str) -> str:
    toks = [t for t in name_norm.split() if t not in STOPWORDS]
    if not toks: return name_norm
    if len(toks) == 1: return toks[0]
    return f"{toks[0]} {toks[-1]}"

CANON = {
    "boca juniors":"boca juniors","boca":"boca juniors",
    "river plate":"river plate","river":"river plate",
    "racing club":"racing club","racing":"racing club","racing avellaneda":"racing club",
    "independiente":"independiente",
    "san lorenzo":"san lorenzo","san lorenzo de almagro":"san lorenzo",
    "velez sarsfield":"velez sarsfield","velez":"velez sarsfield","velez sarfield":"velez sarsfield",
    "lanus":"lanus","ca lanus":"lanus",
    "huracan":"huracan",
    "argentinos juniors":"argentinos juniors","argentinos":"argentinos juniors",
    "estudiantes lp":"estudiantes lp","estudiantes de la plata":"estudiantes lp","estudiantes":"estudiantes lp",
    "gimnasia lp":"gimnasia lp","gimnasia la plata":"gimnasia lp","gimnasia":"gimnasia lp",
    "newells old boys":"newells old boys","newell s old boys":"newells old boys","newells":"newells old boys",
    "rosario central":"rosario central",
    "talleres":"talleres","talleres de cordoba":"talleres","ca talleres":"talleres",
    "banfield":"banfield",
    "defensa y justicia":"defensa y justicia","defensa y just":"defensa y justicia",
    "godoy cruz":"godoy cruz",
    "platense":"platense","club atletico platense":"platense",
    "sarmiento":"sarmiento",
    "barracas central":"barracas central",
    "central cordoba sde":"central cordoba sde","central cordoba":"central cordoba sde",
    "tigre":"tigre",
    "union":"union","union santa fe":"union",
    "atletico tucuman":"atletico tucuman",
    "belgrano":"belgrano",
    "instituto":"instituto",
    "independiente rivadavia":"independiente rivadavia",
    "deportivo riestra":"deportivo riestra",
}
def canon_club(x: str) -> str:
    k = norm_txt(x); return CANON.get(k, k)

def safe_int(x):
    try:
        s = str(x)
        if "-" in s: s = s.split("-")[0]
        return int(float(s))
    except: return None

def parse_dob_to_iso(x: str) -> str:
    if x is None or str(x).strip()=="":
        return ""
    s = str(x).strip().replace("\xa0"," ")
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})$", s)
    if m: return s
    m = re.search(r"(\d{1,2})[\/\.](\d{1,2})[\/\.](\d{4})", s)
    if m:
        d, mth, y = map(int, m.groups())
        try:
            return f"{y:04d}-{mth:02d}-{d:02d}"
        except: return s
    return s

def year_from_dob(dob: str):
    if not dob: return None
    m = re.search(r"(\d{4})", str(dob))
    return int(m.group(1)) if m else None

# -------------------- helpers de matching --------------------
def fuzzy_fill_by_club(left_df, right_df, target_mask, out_df, thresh=90):
    """Fuzzy por club en player_norm (sin processor)."""
    fill_cols = ["market_value_eur","player_id","dob","age"]
    for club in sorted(left_df.loc[target_mask, "club_norm"].dropna().unique()):
        L = left_df[target_mask & (left_df["club_norm"]==club)]
        R = right_df[right_df["club_norm"]==club]
        if L.empty or R.empty: continue
        candidates = R.reset_index(drop=True)
        names = [(str(x) if pd.notna(x) else "") for x in candidates["player_norm"].tolist()]
        for idx, row in L.iterrows():
            q = str(row.get("player_norm","") or "")
            if not q: continue
            best = process.extractOne(q, names, scorer=fuzz.token_set_ratio)
            if not best: continue
            cand_name, score, pos = best
            if score < thresh: continue
            rrow = candidates.iloc[pos]
            out_df.loc[idx, fill_cols] = [
                rrow["market_value_eur"], rrow.get("player_id", pd.NA),
                rrow.get("dob",""), rrow.get("age", pd.NA)
            ]
            out_df.loc[idx, "join_method"] = f"fuzzy_club({score})"

def fuzzy_fill_global(left_df, right_df, target_mask, out_df, thresh=93, allow_year_tolerance=True):
    """
    Fuzzy global SIN club. Filtra por birth_year (±1 si se permite).
    Acepta solo si gap con el segundo >= 5.
    """
    fill_cols = ["market_value_eur","player_id","dob","age"]
    R = right_df.dropna(subset=["birth_year"]).copy()
    for idx, row in left_df[target_mask].iterrows():
        by = row.get("birth_year", pd.NA)
        if pd.isna(by): continue
        by_set = {int(by)}
        if allow_year_tolerance:
            by_set |= {int(by)-1, int(by)+1}
        sub = R[R["birth_year"].isin(by_set)].copy()
        if sub.empty: continue
        sub = sub.sort_values(["player_fl","birth_year","market_value_eur"], ascending=[True, True, False])
        sub = sub.drop_duplicates(subset=["player_fl","birth_year"], keep="first").reset_index(drop=True)

        names = [(str(x) if pd.notna(x) else "") for x in sub["player_norm"].tolist()]
        q = str(row.get("player_norm","") or "")
        if not q: continue
        topk = process.extract(q, names, scorer=fuzz.token_set_ratio, limit=2)
        if not topk: continue
        _, score1, pos1 = topk[0]
        score2 = topk[1][1] if len(topk) > 1 else -1
        if score1 < thresh: continue
        if not ((score2 == -1) or (score1 - score2 >= 5)):  # inequívoco
            continue
        rrow = sub.iloc[pos1]
        out_df.loc[idx, fill_cols] = [
            rrow["market_value_eur"], rrow.get("player_id", pd.NA),
            rrow.get("dob",""), rrow.get("age", pd.NA)
        ]
        out_df.loc[idx, "join_method"] = f"fuzzy_global({score1})"

def unique_only(df, keys):
    dup = df.duplicated(subset=keys, keep=False)
    return df[~dup].copy()

# -------------------- main --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fbref", required=True)
    ap.add_argument("--tm", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--season-year", type=int, default=2024)
    ap.add_argument("--fuzzy-global-thresh", type=int, default=93)
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    # Carga
    df_f = pd.read_csv(args.fbref)
    df_t = pd.read_csv(args.tm, encoding="utf-8-sig", dtype=str)

    # TM tipos/normalización
    df_t["market_value_eur"] = pd.to_numeric(df_t["market_value_eur"], errors="coerce").astype("Int64")
    if "dob" in df_t.columns: df_t["dob"] = df_t["dob"].apply(parse_dob_to_iso)
    if "player_id" in df_t.columns: df_t["player_id"] = pd.to_numeric(df_t["player_id"], errors="coerce").astype("Int64")

    # FBref norms
    df_f["player_norm"] = df_f["Player"].apply(norm_txt)
    df_f["player_fl"]   = df_f["player_norm"].apply(first_last_key)
    df_f["club_norm"]   = df_f["Squad"].apply(canon_club)

    # Born → birth_year_fb y dob_fb
    if "Born" in df_f.columns:
        df_f["dob_fb"] = df_f["Born"].apply(parse_dob_to_iso)
        df_f["birth_year_fb"] = df_f["dob_fb"].apply(year_from_dob)
    else:
        df_f["dob_fb"] = ""
        df_f["birth_year_fb"] = None

    # Completa birth_year_fb con Age/AgeYears si falta
    if df_f["birth_year_fb"].isna().any():
        age_col = "AgeYears" if "AgeYears" in df_f.columns else ("Age" if "Age" in df_f.columns else None)
        if age_col:
            ages = df_f[age_col].apply(safe_int)
            est = args.season_year - ages
            df_f.loc[df_f["birth_year_fb"].isna() & ages.notna(), "birth_year_fb"] = est

    # TM norms
    df_t["player_norm"] = df_t["player_name"].apply(norm_txt)
    df_t["player_fl"]   = df_t["player_norm"].apply(first_last_key)
    df_t["club_norm"]   = df_t["club_name"].apply(canon_club)

    df_t["birth_year_tm"] = df_t.get("dob", pd.Series([""]*len(df_t))).apply(year_from_dob)
    if "age" in df_t.columns:
        age_int = df_t["age"].apply(lambda x: int(x) if str(x).isdigit() else None)
        df_t.loc[df_t["birth_year_tm"].isna() & pd.Series(age_int).notna(), "birth_year_tm"] = args.season_year - pd.Series(age_int)

    # Bases
    left  = df_f.copy()
    left["birth_year"]  = pd.to_numeric(left["birth_year_fb"], errors="coerce").astype("Int64")
    right = df_t.copy()
    right["birth_year"] = pd.to_numeric(right["birth_year_tm"], errors="coerce").astype("Int64")

    keep_tm = ["player_norm","player_fl","club_norm","birth_year","market_value_eur","player_id","dob","age"]

    # JOIN 1
    m = pd.merge(left, right[keep_tm], on=["player_norm","club_norm","birth_year"], how="left")
    m["join_method"] = m["market_value_eur"].apply(lambda x: "key+birth_year" if pd.notna(x) else "")

    # JOIN 2
    mask = m["market_value_eur"].isna()
    if mask.any():
        r2 = right[keep_tm].drop(columns=["player_norm"]).drop_duplicates(subset=["player_fl","club_norm","birth_year"])
        m2 = pd.merge(left[mask], r2, on=["player_fl","club_norm","birth_year"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m2[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "first+last+club+birth_year"

    # JOIN 2b (±1, único)
    for delta in (-1, 1):
        mask = m["market_value_eur"].isna() & m["birth_year"].notna()
        if mask.any():
            r2b = right[keep_tm].copy()
            r2b["birth_year_shift"] = r2b["birth_year"] + delta
            r2b_u = unique_only(r2b, ["player_fl","club_norm","birth_year_shift"])
            l2b = left[mask].copy(); l2b["birth_year_shift"] = l2b["birth_year"]
            m2b = pd.merge(l2b, r2b_u[["player_fl","club_norm","birth_year_shift","market_value_eur","player_id","dob","age"]],
                           on=["player_fl","club_norm","birth_year_shift"], how="left")
            fill = ["market_value_eur","player_id","dob","age"]
            pick = m2b["market_value_eur"].notna()
            m.loc[mask, fill] = m2b[fill].values
            m.loc[mask & pick, "join_method"] = f"first+last+club+birth_year±{abs(delta)}"

    # JOIN 2c (DOB exacto): player_fl + dob
    mask = m["market_value_eur"].isna() & left.get("dob_fb","").astype(str).ne("").values
    if mask.any():
        r2c = right[keep_tm].drop(columns=["player_norm","club_norm","birth_year"]).drop_duplicates(subset=["player_fl","dob"])
        l2c = left[mask].copy()
        m2c = pd.merge(l2c, r2c, left_on=["player_fl","dob_fb"], right_on=["player_fl","dob"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m2c[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "first+last+dob"

    # JOIN 3
    mask = m["market_value_eur"].isna()
    if mask.any():
        r3 = right[keep_tm].drop(columns=["player_norm","club_norm"]).drop_duplicates(subset=["player_fl","birth_year"])
        m3 = pd.merge(left[mask], r3, on=["player_fl","birth_year"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m3[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "first+last+birth_year"

    # JOIN 3b (±1, único, ignora club)
    for delta in (-1, 1):
        mask = m["market_value_eur"].isna() & m["birth_year"].notna()
        if mask.any():
            r6 = right[keep_tm].copy(); r6["birth_year_shift"] = r6["birth_year"] + delta
            r6_u = unique_only(r6, ["player_fl","birth_year_shift"])
            l6 = left[mask].copy(); l6["birth_year_shift"] = l6["birth_year"]
            m6 = pd.merge(l6, r6_u[["player_fl","birth_year_shift","market_value_eur","player_id","dob","age"]],
                          on=["player_fl","birth_year_shift"], how="left")
            fill = ["market_value_eur","player_id","dob","age"]
            pick = m6["market_value_eur"].notna()
            m.loc[mask, fill] = m6[fill].values
            m.loc[mask & pick, "join_method"] = f"first+last+birth_year±{abs(delta)}(unique)"

    # JOIN 4 (sin año)
    mask = m["market_value_eur"].isna()
    if mask.any():
        r4 = right[keep_tm].drop(columns=["birth_year"]).drop_duplicates(subset=["player_norm","club_norm"])
        m4 = pd.merge(left[mask], r4, on=["player_norm","club_norm"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m4[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "key_no_year"

    # JOIN 5 (fuzzy por club)
    mask = m["market_value_eur"].isna()
    if mask.any():
        fuzzy_fill_by_club(left, right, mask, m, thresh=90)

    # JOIN 6 (fuzzy global sin club, birth_year ±1)
    mask = m["market_value_eur"].isna()
    if mask.any():
        fuzzy_fill_global(left, right, mask, m, thresh=args.fuzzy_global_thresh, allow_year_tolerance=True)

    # -------- Salida
    if "player_fl" not in m.columns and "player_norm" in m.columns:
        m["player_fl"] = m["player_norm"].apply(first_last_key)
    if "player_id" in m.columns:
        m["player_id"] = pd.to_numeric(m["player_id"], errors="coerce").astype("Int64")
    if "market_value_eur" in m.columns:
        m["market_value_eur"] = pd.to_numeric(m["market_value_eur"], errors="coerce").astype("Int64")
    if "dob" in m.columns:
        m["dob"] = m["dob"].apply(parse_dob_to_iso)

    base_cols  = list(df_f.columns)
    # Incluimos dob_fb en la salida para trazabilidad (opcional)
    if "dob_fb" in m.columns and "dob_fb" not in base_cols:
        base_cols = base_cols + ["dob_fb"]
    extra_cols = ["market_value_eur","player_id","dob","age","join_method"]
    out_cols   = [c for c in (base_cols + extra_cols) if c in m.columns]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    m[out_cols].to_csv(args.out, index=False, encoding="utf-8-sig")

    if "join_method" in m.columns:
        counts = m["join_method"].fillna("").value_counts().to_dict()
        print("Join breakdown:", counts)

    unmatched = m[m["market_value_eur"].isna()]
    if not unmatched.empty:
        cols = [c for c in ["Player","Squad","Born","dob_fb","player_norm","player_fl","club_norm","birth_year_fb"] if c in unmatched.columns]
        os.makedirs("data/tmp", exist_ok=True)
        unmatched[cols].to_csv("data/tmp/unmatched_ARG_2024.csv", index=False, encoding="utf-8-sig")

    matched = (~m["market_value_eur"].isna()).sum()
    print(f"Rows FBref: {len(df_f)} | Matched: {matched} | Unmatched: {len(unmatched)}")
    print(f"Wrote → {args.out}")
    if not unmatched.empty:
        print("Unmatched saved → data/tmp/unmatched_ARG_2024.csv")

if __name__ == "__main__":
    main()
