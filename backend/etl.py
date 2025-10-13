import LanusStats as ls
import pandas as pd
from pathlib import Path
from functools import reduce

# --- CONFIGURACIÓN ---
RAW_DATA_PATH = Path("data/raw")
PROCESSED_DATA_PATH = Path("data/processed")
LEAGUE = "Primera Division Argentina"
PAGE = "Fbref"
SEASON_TO_FETCH = '2024'

def run_etl():
    """
    Ejecuta el pipeline de ETL final que extrae, une, limpia y guarda todas las
    estadísticas de jugadores de una liga y temporada.
    """
    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

    print(f"Iniciando ETL de jugadores para '{LEAGUE}' - Temporada '{SEASON_TO_FETCH}' desde '{PAGE}'...")

    try:
        fbref = ls.Fbref()
        
        # 1. Extraemos la tupla de DataFrames
        tuple_of_dfs = fbref.get_all_player_season_stats(league=LEAGUE, season=SEASON_TO_FETCH)

        if not isinstance(tuple_of_dfs, tuple) or not tuple_of_dfs:
            print(f"La extracción no devolvió datos. Resultado: {tuple_of_dfs}")
            return

        print(f"Se extrajeron {len(tuple_of_dfs)} tablas de estadísticas. Uniéndolas...")

        # 2. Unimos los DataFrames
        dataframes_to_merge = [df for df in tuple_of_dfs if 'Player' in df.columns]
        if not dataframes_to_merge:
            print("No se encontraron tablas con la columna 'Player' para unir.")
            return
            
        merged_df = reduce(lambda left, right: pd.merge(left, right, on='Player', how='outer'), dataframes_to_merge)
        merged_df = merged_df.loc[:, ~merged_df.columns.duplicated()]

        print(f"Se unieron los datos de {len(merged_df)} jugadores.")

        # --- !! CORRECCIÓN CLAVE !! ---
        # 3. Convertimos todas las columnas de tipo 'object' a 'string' para evitar errores al guardar
        for col in merged_df.select_dtypes(include=['object']).columns:
            merged_df[col] = merged_df[col].astype(str)

        # 4. Guardado de los datos
        file_name = f"player_stats_{LEAGUE.replace(' ', '_')}_{SEASON_TO_FETCH}.parquet"
        raw_path = RAW_DATA_PATH / file_name
        processed_path = PROCESSED_DATA_PATH / file_name

        merged_df.to_parquet(raw_path, index=False)
        merged_df.to_parquet(processed_path, index=False)

        print(f"\n✅ ¡ÉXITO TOTAL! El pipeline de ETL se completó correctamente.")
        print(f"   El dataset final de jugadores está guardado en: {processed_path}")

    except Exception as e:
        print(f"\nOcurrió un error inesperado durante el ETL: {e}")


if __name__ == "__main__":
    run_etl()