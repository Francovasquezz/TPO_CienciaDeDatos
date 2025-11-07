import sys
import argparse
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

# ---------- Helpers ----------
def coalesce(*vals):
    for v in vals:
        if pd.notna(v) and v != "" and str(v).strip().lower() not in {"nan", "none", "null"}:
            return v
    return pd.NA

def compute_age(dob_str, birth_year):
    """Calcula edad a hoy. Si hay dob (YYYY-MM-DD), usa esa fecha.
       Si no, usa birth_year (al 1 de julio de ese año como aproximación)."""
    today = date.today()
    try:
        if pd.notna(dob_str):
            d = datetime.strptime(str(dob_str), "%Y-%m-%d").date()
        elif pd.notna(birth_year):
            # Tomamos mitad de año para no subestimar/sobreestimar demasiado
            d = date(int(float(birth_year)), 7, 1)
        else:
            return pd.NA
        return relativedelta(today, d).years
    except Exception:
        return pd.NA

def normalize_str(s):
    if pd.isna(s):
        return pd.NA
    s = str(s).strip()
    # podrías agregar más normalizaciones si querés (lowercase, quitar acentos, etc.)
    return s

# ---------- Main ----------
def main():
    parser = argparse.ArgumentParser(description="Limpieza de dataset de jugadores (joins).")
    parser.add_argument("--input", required=True, help="Ruta del CSV de entrada (joins).")
    parser.add_argument("--output", required=True, help="Ruta del CSV limpio de salida.")
    args = parser.parse_args()

    # Leer
    df = pd.read_csv(args.input)

    # Quitar columnas duplicadas exactas de nombre
    df = df.loc[:, ~df.columns.duplicated()]

    # Lista de columnas redundantes que solemos ver tras los joins (ajustable)
    redundantes = [
        "player_norm", "player_fl",
        "dob_fb", "birth_year_fb",
        "Age", "AgeYears", "Born",
        # Si preferís conservar 'Squad' además de 'club_norm', borrá la siguiente línea:
        "Squad",
    ]
    df = df.drop(columns=[c for c in redundantes if c in df.columns], errors="ignore")

    # Crear columnas canónicas
    # Nombre del jugador
    df["player_name"] = df.apply(
        lambda r: coalesce(r.get("Player"), r.get("player_fl"), r.get("player_norm")),
        axis=1
    ).map(normalize_str)

    # Club (preferimos 'club_norm' si existe)
    df["club"] = df.apply(
        lambda r: coalesce(r.get("club_norm"), r.get("Squad")),
        axis=1
    ).map(normalize_str)

    df = df.drop(columns=[c for c in ["Player","player_fl","player_norm","club_norm","Squad","join_method"] if c in df.columns], errors="ignore")


    # Edad: priorizamos recalcular a partir de 'dob' y si no, usamos birth_year si está
    birth_year_alt = None
    # Si todavía tenés alguna copia de año de nacimiento (ej. 'birth_year_fb' ya la borramos),
    # podés setear acá otra col alternativa. Lo dejamos en None por ahora.

    df["age_clean"] = df.apply(
        lambda r: compute_age(r.get("dob"), birth_year_alt),
        axis=1
    )

    # Si había una columna 'age' original con 0.0 o inválidos, preferimos age_clean cuando exista
    if "age" in df.columns:
        df["age"] = df.apply(
            lambda r: r["age_clean"] if pd.notna(r["age_clean"]) else r["age"],
            axis=1
        )
    else:
        df["age"] = df["age_clean"]

    df = df.drop(columns=["age_clean"], errors="ignore")

    # Asegurar tipos numéricos razonables
    for col in ["market_value_eur"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ordenar columnas para que lo importante vaya primero (opcional)
    cols_prioridad = [
        "player_id", "player_name", "club", "Nation", "Pos",
        "dob", "age", "market_value_eur"
    ]
    # Traemos primero las que existan y luego el resto
    cols_final = [c for c in cols_prioridad if c in df.columns] + \
                 [c for c in df.columns if c not in cols_prioridad]
    df = df[cols_final]

    # Guardar
    df.to_csv(args.output, index=False)
    print(f"✔ Limpieza terminada. Archivo generado: {args.output}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
