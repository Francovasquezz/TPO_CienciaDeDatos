import argparse
from pathlib import Path
import pandas as pd

def main(src_dir: str, out_dir: str):
    src = Path(src_dir)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    # players.* (varía según la versión del dataset)
    players_csv = None
    for cand in ["players.csv", "player.csv", "player_overview.csv"]:
        p = src / cand
        if p.exists():
            players_csv = p
            break
    if not players_csv:
        raise SystemExit(f"No encontré players.csv en {src}")

    dfp = pd.read_csv(players_csv, low_memory=False)

    # Renombres típicos
    rename = {
        "player_id": "tm_player_id",
        "name": "name",
        "date_of_birth": "dob",
        "current_club_id": "tm_club_id",
        "market_value_in_eur": "market_value_eur",
        "last_season": "last_season",
        "last_season_end": "last_update",
        "position": "primary_position",
        "sub_position": "sub_position",
        "country_of_citizenship": "nationality",
    }
    present = {c: rename[c] for c in rename if c in dfp.columns}
    dfp = dfp.rename(columns=present)

    # Si no trae market_value_eur en players, tomamos la última valuation
    if "market_value_eur" not in dfp.columns:
        vals = None
        for cand in ["player_valuations.csv", "players_valuations.csv", "valuations.csv"]:
            p = src / cand
            if p.exists():
                vals = pd.read_csv(p, low_memory=False)
                break
        if vals is None:
            raise SystemExit("No encontré tabla de valuations ni market_value_in_eur en players.csv")
        c_id = next((c for c in ["player_id", "tm_player_id", "id"] if c in vals.columns), None)
        c_val = next((c for c in ["market_value_in_eur", "market_value_eur", "value"] if c in vals.columns), None)
        c_dt = next((c for c in ["date", "last_update", "date_of_update"] if c in vals.columns), None)
        if not (c_id and c_val):
            raise SystemExit("Tabla de valuations sin columnas esperadas")
        vals = vals[[c_id, c_val, c_dt] if c_dt else [c_id, c_val]].copy()
        if c_dt:
            vals = vals.sort_values(c_dt)
        vals = vals.drop_duplicates(subset=[c_id], keep="last")
        vals = vals.rename(columns={c_id: "tm_player_id", c_val: "market_value_eur", c_dt: "last_update"})
        dfp = dfp.merge(vals, on="tm_player_id", how="left")

    keep = ["tm_player_id", "name", "dob", "nationality",
            "tm_club_id", "primary_position", "sub_position",
            "market_value_eur", "last_update"]
    for k in keep:
        if k not in dfp.columns:
            dfp[k] = None

    out_path = out / "tm_market_values_latest.csv"
    dfp[keep].dropna(subset=["name"]).to_csv(out_path, index=False)
    print(f"OK -> {out_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", default="data/external/kaggle_transfermarkt")
    ap.add_argument("--out", default="data/processed")
    args = ap.parse_args()
    main(args.src, args.out)
