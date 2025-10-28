# -*- coding: utf-8 -*-
import os
import sys
import glob
import argparse
import pandas as pd

DATA_DIR_DEFAULT = "data/processed"

REDUNDANT_COLS = [
    "Player","player_fl","player_norm",
    "club_norm","Squad",
    "join_method",
    "dob_fb","birth_year_fb",
    "Age","AgeYears","Born"
]

FRONT_COLS = ["player_id","player_name","club","Nation","Pos","dob","age","market_value_eur"]

def coalesce(*vals):
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s != "" and s.lower() not in ("nan","none","null"):
            return v
    return None

def parse_code_season_from_name(path):
    # "join_bra_2025.csv" -> ("join_bra_2025","bra_2025","_mv.csv")
    base = os.path.basename(path)
    name, ext = os.path.splitext(base)
    if not name.lower().startswith("join_"):
        return name, name, "_mv" + ext
    # para output: "join_<lo-que-sigue>_mv.csv"
    return name, name[5:], "_mv" + ext

def process_one(input_path, output_path=None, verbose=True):
    if verbose:
        print(f"[*] Leyendo: {input_path}")
    df = pd.read_csv(input_path)
    # quitar duplicados de nombre de columna
    df = df.loc[:, ~df.columns.duplicated()]

    # columnas mínimas esperadas (si faltan, se crean vacías)
    needed = set(["Player","player_fl","player_norm","club","club_norm","Squad",
                  "Nation","Pos","dob","age","market_value_eur","player_id","IsGK"])
    for c in needed:
        if c not in df.columns:
            df[c] = None

    # canónicas: player_name y club
    df["player_name"] = df.apply(
        lambda r: coalesce(r.get("player_name"), r.get("Player"), r.get("player_fl"), r.get("player_norm")),
        axis=1
    )
    df["club"] = df.apply(
        lambda r: coalesce(r.get("club"), r.get("club_norm"), r.get("Squad")),
        axis=1
    )

    # market_value_eur a numérico
    if "market_value_eur" in df.columns:
        df["market_value_eur"] = pd.to_numeric(df["market_value_eur"], errors="coerce")
        df = df[df["market_value_eur"].notna() & (df["market_value_eur"] > 0)]

    # ordenar por market value desc
    if "market_value_eur" in df.columns:
        df = df.sort_values(by="market_value_eur", ascending=False)

    # dropear columnas redundantes si existen
    drop_cols = [c for c in REDUNDANT_COLS if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")

    # ordenar columnas: primero FRONT_COLS, luego el resto en orden original
    front = [c for c in FRONT_COLS if c in df.columns]
    tail = [c for c in df.columns if c not in front]
    df = df[front + tail]

    # nombre de salida
    if output_path is None:
        in_name, rest, suffix = parse_code_season_from_name(input_path)
        output_path = os.path.join(os.path.dirname(input_path), f"{in_name}_mv.csv")

    # guardar
    df.to_csv(output_path, index=False)
    if verbose:
        print(f"[✓] Guardado: {output_path} (filas: {len(df)})")

def main():
    ap = argparse.ArgumentParser(description="Generar CSV _mv (filtrado por market_value_eur y ordenado).")
    ap.add_argument("--input", help="Ruta a un CSV join_*.csv (procesa solo ese archivo).")
    ap.add_argument("--dir", default=DATA_DIR_DEFAULT, help=f"Directorio de búsqueda (default: {DATA_DIR_DEFAULT}).")
    ap.add_argument("--all", action="store_true", help="Procesa todos los join_*.csv del directorio (omite los que ya terminan en _mv.csv).")
    args = ap.parse_args()

    if args.input and args.all:
        print("No uses --input y --all a la vez. Elegí uno.", file=sys.stderr)
        sys.exit(1)

    if args.input:
        process_one(args.input)
        return

    # por defecto, si no hay --input y no pusiste --all, igual procesamos todos
    do_all = True if args.all or not args.input else False
    if do_all:
        pattern = os.path.join(args.dir, "join_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f"No se encontraron archivos con patrón: {pattern}")
            return
        for f in files:
            base = os.path.basename(f).lower()
            # saltar si ya es un _mv.csv
            if base.endswith("_mv.csv"):
                continue
            process_one(f)
    else:
        print("Nada para hacer. Usá --input <file> o --all.", file=sys.stderr)

if __name__ == "__main__":
    main()
