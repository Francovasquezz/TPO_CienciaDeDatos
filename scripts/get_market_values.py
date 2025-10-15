# scripts/get_market_values.py (Versión con Workaround)

import LanusStats as ls
import pandas as pd
from pathlib import Path
import time

# --- CONFIGURACIÓN ---
PROCESSED_DATA_PATH = Path("data/processed")
LEAGUE = "Primera Division Argentina"
SEASON_TO_FETCH = "2024"

# --- !! WORKAROUND: LISTA MANUAL DE URLs !! ---
# Obtenidas directamente de Transfermarkt.com para la Primera División Argentina
TEAM_URLS = [
    "https://www.transfermarkt.com/club-atletico-river-plate/startseite/verein/209",
    "https://www.transfermarkt.com/club-atletico-boca-juniors/startseite/verein/189",
    "https://www.transfermarkt.com/club-atletico-velez-sarsfield/startseite/verein/1139",
    "https://www.transfermarkt.com/club-atletico-talleres/startseite/verein/3948",
    "https://www.transfermarkt.com/club-atletico-san-lorenzo-de-almagro/startseite/verein/1134",
    # Podes seguir agregando las URLs del resto de los equipos aquí
]

def run_market_value_etl():
    """
    Extrae el valor de mercado de los jugadores usando una lista manual de URLs
    de equipos de Transfermarkt.
    """
    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)
    print(f"Iniciando ETL de Valor de Mercado para '{LEAGUE}' ({SEASON_TO_FETCH})...")

    try:
        tm = ls.Transfermarkt()
        all_players_market_value_df = pd.DataFrame()

        print(f"Se usarán {len(TEAM_URLS)} URLs de equipos cargadas manualmente.")
        print("\nPaso 1: Extrayendo valor de mercado de jugadores por equipo...")

        for url in TEAM_URLS:
            team_name = url.split('/')[-4].replace('-', ' ').title()
            print(f"--- Equipo: {team_name} ---")
            try:
                # La función para obtener datos de jugadores sí funciona
                players_mv_df = tm.get_team_players_market_value(url)
                
                if players_mv_df is not None and not players_mv_df.empty:
                    all_players_market_value_df = pd.concat([all_players_market_value_df, players_mv_df], ignore_index=True)
                    print(f"Se extrajeron datos de {len(players_mv_df)} jugadores.")
                else:
                    print(f"No se encontraron datos para {team_name}.")
                
                time.sleep(3) # Pausa para no saturar el servidor

            except Exception as e:
                print(f"Error extrayendo jugadores de {team_name}: {e}")

        if all_players_market_value_df.empty:
            print("\nNo se pudo extraer ningún dato de valor de mercado.")
            return

        print("\nPaso 2: Limpiando y guardando los datos...")
        if 'player' in all_players_market_value_df.columns:
            all_players_market_value_df.rename(columns={'player': 'Player'}, inplace=True)

        final_df = all_players_market_value_df[['Player', 'market_value(€)']].copy()
        
        file_name = f"market_values_{LEAGUE.replace(' ', '_')}_{SEASON_TO_FETCH}.parquet"
        output_path = PROCESSED_DATA_PATH / file_name
        
        final_df.to_parquet(output_path, index=False)
        
        print(f"\n✅ ¡ÉXITO! Se guardó el valor de mercado de {len(final_df)} jugadores en:")
        print(f"   - {output_path}")

    except Exception as e:
        print(f"\nOcurrió un error inesperado: {e}")

if __name__ == "__main__":
    run_market_value_etl()