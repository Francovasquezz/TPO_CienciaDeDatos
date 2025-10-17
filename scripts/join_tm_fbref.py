# scripts/join_tm_fbref.py  (v3.2)
# Cascada robusta de joins:
# 1) player_norm + club_norm + birth_year
# 2) player_fl   + club_norm + birth_year
# 3) player_fl   + birth_year
# 4) player_norm + club_norm
# 5) fuzzy(player_norm por club) con token_set_ratio >= 90
# 6) player_fl + birth_year si es único en TM (independiente del club)
#
# Además: dob estandarizado a ISO en salida y reporte por método.

import argparse, os, re
import pandas as pd
from unidecode import unidecode
from rapidfuzz import process, fuzz

def norm_txt(x: str) -> str:
    if x is None: return ""
    t = unidecode(str(x)).lower()
    t = re.sub(r"\([^)]*\)", " ", t)             # quita (LP), (SdE), etc.
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
        if "-" in s: s = s.split("-")[0]  # "22-105" → 22
        return int(float(s))
    except: return None

def parse_dob_to_iso(x: str) -> str:
    """dd/mm/yyyy, d/m/yyyy, dd.mm.yyyy, yyyy-mm-dd → yyyy-mm-dd si posible."""
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

def fuzzy_fill(left_df, right_df, target_mask, out_df, thresh=90):
    """Match por club usando fuzzy en player_norm. Modifica out_df in-place."""
    fill_cols = ["market_value_eur","player_id","dob","age"]
    used_right_idx = set()
    for club in sorted(left_df.loc[target_mask, "club_norm"].dropna().unique()):
        L = left_df[target_mask & (left_df["club_norm"]==club)]
        R = right_df[right_df["club_norm"]==club]
        if L.empty or R.empty: continue
        choices = R["player_norm"].tolist()
        for idx, row in L.iterrows():
            q = row["player_norm"]
            best = process.extractOne(q, choices, scorer=fuzz.token_set_ratio)
            if not best: continue
            cand_name, score, pos = best
            if score < thresh: continue
            rrow = R.iloc[pos]
            ridx = rrow.name
            if ridx in used_right_idx: continue
            out_df.loc[idx, fill_cols] = [
                rrow["market_value_eur"], rrow.get("player_id", pd.NA),
                rrow.get("dob",""), rrow.get("age", pd.NA)
            ]
            out_df.loc[idx, "join_method"] = f"fuzzy({score})"
            used_right_idx.add(ridx)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fbref", required=True)
    ap.add_argument("--tm", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--season-year", type=int, default=2024)
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

    if "Born" in df_f.columns:
        df_f["birth_year_fb"] = df_f["Born"].apply(lambda x: safe_int(str(x)[:4]) if pd.notna(x) else None)
    else:
        df_f["birth_year_fb"] = None
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
    left  = df_f.copy(); left["birth_year"]  = left["birth_year_fb"]
    right = df_t.copy(); right["birth_year"] = right["birth_year_tm"]

    keep_tm_cols = ["player_norm","player_fl","club_norm","birth_year","market_value_eur","player_id","dob","age"]

    # JOIN 1
    m = pd.merge(left, right[keep_tm_cols], on=["player_norm","club_norm","birth_year"], how="left")
    m["join_method"] = m["market_value_eur"].apply(lambda x: "key+birth_year" if pd.notna(x) else "")

    # JOIN 2
    mask = m["market_value_eur"].isna()
    if mask.any():
        r2 = right[keep_tm_cols].drop(columns=["player_norm"]).drop_duplicates(subset=["player_fl","club_norm","birth_year"])
        m2 = pd.merge(left[mask], r2, on=["player_fl","club_norm","birth_year"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m2[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "first+last+club+birth_year"

    # JOIN 3
    mask = m["market_value_eur"].isna()
    if mask.any():
        r3 = right[keep_tm_cols].drop(columns=["player_norm","club_norm"]).drop_duplicates(subset=["player_fl","birth_year"])
        m3 = pd.merge(left[mask], r3, on=["player_fl","birth_year"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m3[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "first+last+birth_year"

    # JOIN 4
    mask = m["market_value_eur"].isna()
    if mask.any():
        r4 = right[keep_tm_cols].drop(columns=["birth_year"]).drop_duplicates(subset=["player_norm","club_norm"])
        m4 = pd.merge(left[mask], r4, on=["player_norm","club_norm"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m4[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "key_no_year"

    # JOIN 5 (fuzzy por club)
    mask = m["market_value_eur"].isna()
    if mask.any():
        fuzzy_fill(left, right, mask, m, thresh=90)

    # JOIN 6 (único por player_fl + birth_year en TM, ignorando club)
    mask = m["market_value_eur"].isna()
    if mask.any():
        r6_counts = right.groupby(["player_fl","birth_year"]).size().reset_index(name="n")
        r6_unique = r6_counts[r6_counts["n"]==1].drop(columns=["n"])
        r6 = pd.merge(r6_unique, right[keep_tm_cols], on=["player_fl","birth_year"], how="left")
        r6 = r6.drop_duplicates(subset=["player_fl","birth_year"])
        m6 = pd.merge(left[mask], r6[["player_fl","birth_year","market_value_eur","player_id","dob","age"]], on=["player_fl","birth_year"], how="left")
        fill = ["market_value_eur","player_id","dob","age"]
        m.loc[mask, fill] = m6[fill].values
        m.loc[mask & m["market_value_eur"].notna(), "join_method"] = "first+last+birth_year(unique)"

    # -------- Salida robusta --------
    # Garantizar player_fl
    if "player_fl" not in m.columns and "player_norm" in m.columns:
        m["player_fl"] = m["player_norm"].apply(first_last_key)

    # tipado y estandarización de dob
    if "player_id" in m.columns:
        m["player_id"] = pd.to_numeric(m["player_id"], errors="coerce").astype("Int64")
    if "market_value_eur" in m.columns:
        m["market_value_eur"] = pd.to_numeric(m["market_value_eur"], errors="coerce").astype("Int64")
    if "dob" in m.columns:
        m["dob"] = m["dob"].apply(parse_dob_to_iso)

    base_cols  = list(df_f.columns)  # incluye player_fl
    extra_cols = ["market_value_eur","player_id","dob","age","join_method"]
    out_cols   = [c for c in (base_cols + extra_cols) if c in m.columns]

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    m[out_cols].to_csv(args.out, index=False, encoding="utf-8-sig")

    # Reporte
    if "join_method" in m.columns:
        counts = m["join_method"].fillna("").value_counts().to_dict()
        print("Join breakdown:", counts)

    unmatched = m[m["market_value_eur"].isna()][["Player","Squad","Born","player_norm","player_fl","club_norm","birth_year_fb"]] if "player_fl" in m.columns else \
                m[m["market_value_eur"].isna()][["Player","Squad","Born","player_norm","club_norm","birth_year_fb"]]
    if len(unmatched):
        os.makedirs("data/tmp", exist_ok=True)
        unmatched.to_csv("data/tmp/unmatched_ARG_2024.csv", index=False, encoding="utf-8-sig")

    matched = (~m["market_value_eur"].isna()).sum()
    print(f"Rows FBref: {len(df_f)} | Matched: {matched} | Unmatched: {len(unmatched)}")
    print(f"Wrote → {args.out}")
    if len(unmatched):
        print("Unmatched saved → data/tmp/unmatched_ARG_2024.csv")

if __name__ == "__main__":
    main()
