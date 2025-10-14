import argparse, pandas as pd, unicodedata, uuid
from rapidfuzz import fuzz, process

def norm(s):
    if pd.isna(s): return ""
    s = s.strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    return " ".join(s.split())

def uuid5_player(primary_id: str, fallback: str = ""):
    ns = uuid.UUID("12345678-1234-5678-1234-567812345678")  # fija para el proyecto
    return str(uuid.uuid5(ns, f"{primary_id}|{fallback}"))

def build_xref(fbref_df, tm_df, det_score=96, fuzzy_score=90):
    # columnas esperadas: name, dob(YYYY-MM-DD), team, nationality, height_cm, player_id (propio de la fuente)
    fbref_df = fbref_df.copy()
    tm_df = tm_df.copy()
    for df in (fbref_df, tm_df):
        for c in ["name","team","nationality"]:
            if c in df.columns: df[c+"_n"] = df[c].map(norm)
        if "dob" in df.columns: df["dob_n"] = df["dob"].fillna("")
        if "height_cm" in df.columns: df["height_cm"] = pd.to_numeric(df["height_cm"], errors="coerce")

    # 1) Match determinista estricto: name_n + dob_n (y opcional team_n)
    det = pd.merge(
        fbref_df, tm_df,
        left_on=["name_n","dob_n"], right_on=["name_n","dob_n"],
        suffixes=("_fb", "_tm")
    )
    det["match_type"] = "deterministic"
    det["score"] = 100

    # 2) Fuzzy por nombre (si no hay dob), filtrando por equipo/país/altura cuando ayuda
    fb_left = fbref_df[~fbref_df["name_n"].isin(det["name_n"])]
    candidates = []
    for _, r in fb_left.iterrows():
        pool = tm_df
        if r.get("team_n"):
            pool = pool[pool["team_n"] == r["team_n"]]
            if pool.empty: pool = tm_df
        match = process.extractOne(
            r["name_n"], pool["name_n"].tolist(), scorer=fuzz.WRatio
        )
        if match and match[1] >= fuzzy_score:
            tm_row = pool.iloc[pool["name_n"].tolist().index(match[0])]
            candidates.append({
                "name_n": r["name_n"], "dob_n": r.get("dob_n",""),
                "player_id_fb": r.get("player_id",""),
                "player_id_tm": tm_row.get("player_id",""),
                "score": match[1],
                "match_type": "fuzzy_team"
            })
    fuzzy = pd.DataFrame(candidates)

    # Unificación a xref
    if not fuzzy.empty:
        xref = pd.concat([
            det[["player_id_fb","player_id_tm","score","match_type"]],
            fuzzy[["player_id_fb","player_id_tm","score","match_type"]]
        ], ignore_index=True).drop_duplicates(["player_id_fb","player_id_tm"])
    else:
        xref = det[["player_id_fb","player_id_tm","score","match_type"]].copy()

    # Generar player_uuid (preferí el id de Transfermarkt como semilla estable)
    xref["player_uuid"] = xref.apply(
        lambda r: uuid5_player(r["player_id_tm"] or r["player_id_fb"], r["player_id_fb"]), axis=1
    )
    return xref

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--fbref", default="data/processed/fbref_players.csv")
    ap.add_argument("--tm",    default="data/processed/transfermarkt_players.csv")
    ap.add_argument("--out",   default="data/processed/player_xref.csv")
    args = ap.parse_args()
    fb = pd.read_csv(args.fbref)
    tm = pd.read_csv(args.tm)
    xref = build_xref(fb, tm)
    xref.to_csv(args.out, index=False)
    print(f"OK -> {args.out} ({len(xref)} matches)")
