# backend/etl.py

import LanusStats as ls
import pandas as pd
from pandas.api import types as ptypes
from pathlib import Path
from functools import reduce
import numpy as np
import unicodedata
import re

# --- CONFIGURACIÓN ---
RAW_DATA_PATH = Path("data/raw")
PROCESSED_DATA_PATH = Path("data/processed")
LEAGUE = "Primera Division Argentina"
PAGE = "Fbref"
SEASON_TO_FETCH = "2024"

JOIN_KEY = "Player"  # clave de unión principal


# ---------- Utilidades ----------

def normalize_key_series(s: pd.Series) -> pd.Series:
    """Normaliza la clave de unión: quita acentos, espacios extra y baja a minúsculas."""
    def _norm(x):
        if pd.isna(x):
            return ""
        x = str(x).strip()
        x = "".join(ch for ch in x if ch.isprintable())
        x = unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode("ascii")
        x = re.sub(r"\s+", " ", x)
        return x.lower()
    return s.map(_norm)


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Aplana MultiIndex si existiera."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in tup if str(x) != ""])
                      for tup in df.columns.values]
    else:
        df.columns = [str(c) for c in df.columns]
    return df


def ensure_unique(names):
    """Devuelve una lista de nombres únicos, agregando sufijos __dupN cuando haga falta."""
    seen = {}
    result = []
    for n in names:
        if n not in seen:
            seen[n] = 0
            result.append(n)
        else:
            seen[n] += 1
            new = f"{n}__dup{seen[n]}"
            while new in seen:
                seen[n] += 1
                new = f"{n}__dup{seen[n]}"
            seen[new] = 0
            result.append(new)
    return result


def add_df_prefix(df: pd.DataFrame, prefix: str, keep_cols=None) -> pd.DataFrame:
    """
    Prefija columnas sin prefijo claro para evitar choques entre DFs.
    No toca las claves (default: JOIN_KEY).
    """
    if keep_cols is None:
        keep_cols = {JOIN_KEY}

    prefixes = [re.match(r"([A-Za-z]+)_", c).group(1).lower()
                for c in df.columns if re.match(r"([A-Za-z]+)_", c)]
    major = None
    if prefixes:
        major = max(set(prefixes), key=prefixes.count)

    new_cols = {}
    for c in df.columns:
        if c in keep_cols:
            new_cols[c] = c
            continue
        if re.match(r"^[A-Za-z]+_", c):
            new_cols[c] = c
        else:
            use_prefix = major if major else prefix
            new_cols[c] = f"{use_prefix}_{c}"
    df = df.rename(columns=new_cols)
    return df


def coerce_numeric(df: pd.DataFrame, text_cols: set):
    """Convierte a numérico todas las columnas no textuales; limpia %, comas y espacios finos."""
    for c in df.columns:
        if c in text_cols:
            df[c] = df[c].fillna("").astype(str)
        else:
            s = df[c].astype(str)
            s = s.str.replace("%", "", regex=False)
            s = s.str.replace(",", "", regex=False)
            s = s.str.replace("\u2009", "", regex=False)  # thin space
            df[c] = pd.to_numeric(s, errors="coerce").fillna(0)
    return df


def sanitize_object_for_arrow(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas object/categorical a strings seguras para Arrow/Parquet."""
    df = df.copy()
    for c in df.columns:
        s = df[c]
        if ptypes.is_categorical_dtype(s):
            df[c] = s.astype("string")
            continue
        if s.dtype == object:
            def _to_str(x):
                if x is None or (isinstance(x, float) and np.isnan(x)):
                    return None
                if isinstance(x, (bytes, bytearray)):
                    try:
                        return x.decode("utf-8", "ignore")
                    except Exception:
                        return str(x)
                return str(x)
            df[c] = s.map(_to_str).astype("string")
    return df


def write_parquet_safe(df: pd.DataFrame, path: Path):
    """Escribe parquet con fallback de engine y columnas únicas + saneo Arrow."""
    if not df.columns.is_unique:
        df.columns = ensure_unique(list(df.columns))
    df = sanitize_object_for_arrow(df)
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except Exception:
        df.to_parquet(path, index=False)


# ---------- Pipeline ----------

def run_etl():
    """
    Ejecuta el pipeline de ETL completo: Extract -> Transform -> Load.
    """
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

    print(f"Iniciando ETL para '{LEAGUE}' - Temporada '{SEASON_TO_FETCH}'...")

    try:
        # --- 1) EXTRACT ---
        print("Paso 1: Extrayendo datos crudos...")
        fbref = ls.Fbref()
        tuple_of_dfs = fbref.get_all_player_season_stats(
            league=LEAGUE,
            season=SEASON_TO_FETCH
        )

        if not isinstance(tuple_of_dfs, tuple) or not tuple_of_dfs:
            print("La extracción no devolvió datos. Finalizando.")
            return

        print(f"Se recibieron {len(tuple_of_dfs)} tablas desde LanusStats.")

        # --- 2) TRANSFORM — Pre-limpieza por DF ---
        print("Paso 2a: Pre-limpieza individual por tabla...")
        cleaned_dfs = []
        for i, df in enumerate(tuple_of_dfs):
            if not isinstance(df, pd.DataFrame):
                continue

            df = flatten_columns(df)

            if JOIN_KEY not in df.columns:
                # si no trae Player, no sirve para unión principal
                continue

            # normaliza clave JOIN_KEY
            df[JOIN_KEY] = normalize_key_series(df[JOIN_KEY])

            # desduplica columnas dentro del DF
            df = df.loc[:, ~df.columns.duplicated()]

            # colapsa duplicados por jugador (evita joins cartesianos)
            df = df.drop_duplicates(subset=[JOIN_KEY])

            # agrega prefijo a columnas "genéricas"
            df = add_df_prefix(df, prefix=f"t{i}", keep_cols={JOIN_KEY})

            cleaned_dfs.append(df)

        if not cleaned_dfs:
            print("No se encontraron tablas válidas para unir.")
            return

        # --- 2) TRANSFORM — Unión controlada ---
        print("Paso 2b: Unión por Player con control de columnas.")
        def safe_merge(left, right):
            overlap = [c for c in left.columns.intersection(right.columns) if c != JOIN_KEY]
            if overlap:
                ren = {c: f"{c}__r" for c in overlap}
                right = right.rename(columns=ren)
            merged = pd.merge(left, right, on=JOIN_KEY, how="outer")
            return merged

        raw_df = reduce(safe_merge, cleaned_dfs)

        if not raw_df.columns.is_unique:
            raw_df.columns = ensure_unique(list(raw_df.columns))

        raw_path = RAW_DATA_PATH / f"raw_merged_{SEASON_TO_FETCH}.parquet"
        write_parquet_safe(raw_df, raw_path)
        print(f"Raw unido guardado en: {raw_path}")

        # --- 2) TRANSFORM — Selección, renombre y tipado ---
        print("Paso 2c: Seleccionando y normalizando métricas finales...")
        metrics_map = {
            "Player": "Player",
            "stats_Nation": "Nation",
            "stats_Pos": "Pos",
            "stats_Squad": "Squad",
            "stats_Age": "Age",
            "stats_Born": "Born",
            "stats_90s": "MatchesPlayed",
            "stats_Gls": "Gls",
            "stats_Ast": "Ast",
            "stats_xG": "xG",
            "stats_xAG": "xAG",
            "shooting_Sh": "Shots",
            "shooting_SoT": "SoT",
            "passing_Cmp": "PassCmp",
            "passing_Att": "PassAtt",
            "passing_Cmp%": "PassCmpPct",
            "defense_Tkl": "Tkl",
            "defense_TklW": "TklW",
            "defense_Blocks": "Blocks",
            "defense_Int": "Int",
        }

        available = [c for c in metrics_map.keys() if c in raw_df.columns]
        if not available:
            fallback = []
            for want in metrics_map.keys():
                base = want.split("_", 1)[-1] if "_" in want else want
                cand = [c for c in raw_df.columns if c.endswith("_" + base)]
                if cand:
                    fallback.append(cand[0])
            available = list(set(available) | set(fallback))

        effective_map = {}
        for src, dst in metrics_map.items():
            if src in raw_df.columns:
                effective_map[src] = dst
            else:
                base = src.split("_", 1)[-1] if "_" in src else src
                cands = [c for c in raw_df.columns if c.endswith("_" + base)]
                if cands:
                    effective_map[cands[0]] = dst

        if not effective_map:
            print("No se encontraron las columnas esperadas para el dataset procesado.")
            return

        df_processed = raw_df[list(effective_map.keys())].rename(columns=effective_map)

        # tipado
        text_cols = {"Player", "Nation", "Pos", "Squad", "Born"}
        df_processed = coerce_numeric(df_processed, text_cols=text_cols)

        # --- 3) LOAD ---
        print("Paso 3: Guardando dataset limpio en 'processed'...")
        processed_file_name = f"player_stats_{LEAGUE.replace(' ', '_')}_{SEASON_TO_FETCH}.parquet"
        processed_path = PROCESSED_DATA_PATH / processed_file_name
        write_parquet_safe(df_processed, processed_path)

        print("\n✅ ETL finalizado correctamente.")
        print(f"   Dataset limpio: {processed_path}")

    except Exception as e:
        print(f"\nOcurrió un error inesperado durante el ETL: {e}")
        raise


if __name__ == "__main__":
    run_etl()
