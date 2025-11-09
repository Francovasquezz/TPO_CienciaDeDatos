# backend/value.py

import logging
import json
from pathlib import Path

MODEL_DIR = Path("models")
OPPORTUNITIES_FILE = MODEL_DIR / "market_opportunities.json"

class MarketValueService:
    def __init__(self):
        logging.info("Cargando servicio de oportunidades de mercado...")
        
        try:
            with open(OPPORTUNITIES_FILE, "r") as f:
                # Cargamos toda la lista (son solo 200)
                self.opportunities_cache = json.load(f)
            
            logging.info(f"✅ Oportunidades de mercado cargadas: {len(self.opportunities_cache)} jugadores.")
        
        except FileNotFoundError:
            logging.error(f"Error: No se encontró el archivo '{OPPORTUNITIES_FILE}'.")
            logging.error("Asegúrate de haber ejecutado 'python scripts/build_market_opportunities.py' primero.")
            self.opportunities_cache = [] # Iniciar vacío si falla
        except Exception as e:
            logging.error(f"Error al cargar {OPPORTUNITIES_FILE}: {e}")
            self.opportunities_cache = []
            raise

    def get_opportunities(self, limit: int = 50):
        """
        Devuelve la lista pre-calculada de oportunidades de mercado.
        """
        if not self.opportunities_cache:
            return []
            
        # Devuelve la cantidad pedida (hasta 50 por defecto)
        return self.opportunities_cache[:limit]