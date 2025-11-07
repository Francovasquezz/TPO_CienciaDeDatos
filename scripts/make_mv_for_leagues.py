# -*- coding: utf-8 -*-
import os
import sys
import glob
import argparse
import pandas as pd
import re

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
    #     s = str(v).strip()
        s = str(v).strip()
        if s != "" and s.lower() not in ("nan","none","null"):
            return v
    return None

def parse_code_season_from_name(path):
    base = os.path.basename(path)
    name, ext = os.path.splitext(base)
    if not name.lower().startswith("join_"):
        return name, name, "_mv" + ext
    return name, name[5:], "_mv" + ext

def _read_with(sep, encoding, path):
    return pd.read_csv(path, sep=sep, encoding=encoding)

def read_csv_safely(path, sep=None, encoding=None):
    # 1) Intento: auto-separador con engine='python'
    if sep is None:
        try:
            return pd.read_csv(path, sep=None, engine="python", encoding=encoding or "utf-8-sig")
        except Exception:
            pass
    # 2) Intentos explícitos: ; y , con utf-8-sig y latin1
    trials = []
    if sep is not None:
        trials.append((sep, encoding or "utf-8-sig"))
    else:
        trials += [(";", "utf-8-sig"), (",", "utf-8-sig"), (";", "latin1"), (",", "latin1")]
    last_err = None
    for s, enc in trials:
        try:
            df = _read_with(s, enc, path)
            if df.shape[1] > 1:
                return df
        except Exception as e:
            last_err = e
            continue
    if last_err:
        raise last_err
    # fallback
    return pd.read_csv(path)

def clean_market_value_series(s):
    """
    Convierte market_value_eur a numérico robusto:
    - acepta strings con €, espacios, etc.
    - quita todo lo que no sea dígito o separador decimal.
    - si hay coma y no hay punto => usa coma como decimal.
    - si hay ambos, asume puntos como miles y coma decimal.
    """
    s = s.astype(str)

    def parse_one(x):
        x = x.strip()
        if x == "" or x.lower() in ("nan","none","null"):
            return None
        # normalizar
        x = x.replace("\u00a0", " ")  # nbsp
        # quitar símbolos (€, k, m, etc. no previstos)
        x_clean = re.sub(r"[^0-9\.,]", "", x)

        if x_clean.count(",") > 0 and x_clean.count(".") == 0:
            # solo coma -> decimal europeo
            x_clean = x_clean.replace(",", ".")
        elif x_clean.count(",") > 0 and x_clean.count(".") > 0:
            # ambos: asumir puntos = miles, coma = decimal
            x_clean = x_clean.replace(".", "")
            x_clean = x_clean.replace(",", ".")
        # ahora debería ser parseable
        try:
            return float(x_clean)
        except:
            # última chance: quitar todo excepto dígitos
            digits = re.sub(r"[^0-9]", "", x_clean)
            return float(digits) if digits != "" else None

    return s.map(parse_one)

def process_one(input_path, output_path=None, force_sep=None, force_encoding=None, debug=False):
    if debug:
        print(f"[*] Leyendo: {input_path} (sep={force_sep or 'auto'}, enc={force_encoding or 'auto'})")
    df = read_csv_safely(input_path, sep=force_sep, encoding=force_encoding)

    # columnas únicas
    df = df.loc[:, ~df.columns.duplicated()]

    # columnas esperadas mínimas
    needed = set(["Player","player_fl","player_norm","club","club_norm","Squad",
                  "Nation","Pos","dob","age","market_value_eur","player_id","IsGK"])
    for c in needed:
        if c not in df.columns:
            df[c] = None

    # coalesce nombre/club
    df["player_name"] = df.apply(
        lambda r: coalesce(r.get("player_name"), r.get("Player"), r.get("player_fl"), r.get("player_norm")),
        axis=1
    )
    df["club"] = df.apply(
        lambda r: coalesce(r.get("club"), r.get("club_norm"), r.get("Squad")),
        axis=1
    )

    # market_value_eur robusto
    if "market_value_eur" in df.columns:
        df["market_value_eur"] = clean_market_value_series(df["market_value_eur"])
        mv_before = len(df)
        df = df[df["market_value_eur"].notna() & (df["market_value_eur"] > 0)]
        if debug:
            print(f"    MV>0: {len(df)} / {mv_before}")

    # ordenar por MV desc
    if "market_value_eur" in df.columns:
        df = df.sort_values(by="market_value_eur", ascending=False)

    # dropear columnas redundantes si existen
    drop_cols = [c for c in REDUNDANT_COLS if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols, errors="ignore")

    # ordenar columnas: primero FRONT_COLS, luego el resto
    front = [c for c in FRONT_COLS if c in df.columns]
    tail = [c for c in df.columns if c not in front]
    df = df[front + tail]

    if debug:
        print("    Columns:", df.columns.tolist()[:12], "...")

    # salida
    if output_path is None:
        in_name, rest, suffix = parse_code_season_from_name(input_path)
        output_path = os.path.join(os.path.dirname(input_path), f"{in_name}_mv.csv")

    df.to_csv(output_path, index=False, encoding="utf-8")
    if debug:
        print(f"[✓] Guardado: {output_path} (filas: {len(df)})")

def main():
    ap = argparse.ArgumentParser(description="Generar CSV _mv (filtrado por market_value_eur y ordenado).")
    ap.add_argument("--input", help="Ruta a un CSV join_*.csv (procesa solo ese archivo).")
    ap.add_argument("--dir", default=DATA_DIR_DEFAULT, help=f"Directorio de búsqueda (default: {DATA_DIR_DEFAULT}).")
    ap.add_argument("--all", action="store_true", help="Procesa todos los join_*.csv del directorio (omite los que ya terminan en _mv.csv).")
    ap.add_argument("--debug", action="store_true", help="Imprime info de depuración.")
    ap.add_argument("--sep", help="Forzar separador: ';' o ','. Si se omite, autodetecta.", choices=[";", ","], default=None)
    ap.add_argument("--encoding", help="Forzar encoding (por ej. 'latin1', 'utf-8-sig'). Si se omite, autodetecta.", default=None)
    args = ap.parse_args()

    if args.input and args.all:
        print("No uses --input y --all a la vez. Elegí uno.", file=sys.stderr)
        sys.exit(1)

    if args.input:
        process_one(args.input, force_sep=args.sep, force_encoding=args.encoding, debug=args.debug)
        return

    # por defecto, si no hay --input y no pusiste --all, procesamos todos
    do_all = True if args.all or not args.input else False
    if do_all:
        pattern = os.path.join(args.dir, "join_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            print(f"No se encontraron archivos con patrón: {pattern}")
            return
        for f in files:
            base = os.path.basename(f).lower()
            if base.endswith("_mv.csv"):
                continue
            try:
                process_one(f, force_sep=args.sep, force_encoding=args.encoding, debug=args.debug)
            except Exception as e:
                print(f"[!] Error procesando {f}: {e}")
    else:
        print("Nada para hacer. Usá --input <file> o --all.", file=sys.stderr)

if __name__ == "__main__":
    main()
