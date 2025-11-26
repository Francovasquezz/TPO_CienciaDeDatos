# ⚽ TPO_Futbol: Plataforma de Analítica y Machine Learning

**Visión del proyecto:**  
Construir una plataforma web integral para la **extracción, análisis y visualización de datos de fútbol**.  
El sistema utiliza **modelos de Machine Learning** para generar *insights* sobre el rendimiento de jugadores/equipos y estimar su **valor de mercado**, todo servido a través de una **API RESTful (FastAPI)** y una **interfaz de usuario interactiva (React)**.

---

### 🏛️ Arquitectura y Flujo de Datos

El sistema sigue un flujo modular, desde la ingesta de datos crudos hasta la entrega de predicciones a través de la UI.

1. **Extracción (ETL - Extract):**  
   `backend/etl.py` utiliza la librería `LanusStats` para obtener datos de fuentes como **FBref**, **FotMob** y **Transfermarkt**.

2. **Almacenamiento Crudo (Raw):**  
   Los datos se guardan sin procesar en `data/raw/` (formato **Parquet**) particionados por fuente, liga y temporada.

3. **Procesamiento (Transform & Load):**  
   El ETL limpia, normaliza y unifica IDs. Las tablas maestras (`teams`, `players`, `matches`, etc.) se guardan en `data/processed/`.

4. **Ingeniería de Features:**  
   `scripts/generate_features.py` genera variables para el modelo de ML (promedios móviles, ratios, rankings, etc.), almacenadas en `data/features/`.

5. **Entrenamiento de Modelos:**  
   `scripts/train_model.py` entrena, valida y versiona modelos. Los modelos serializados se guardan en `models/`.

6. **Backend (API):**  
   `backend/app.py` es una **API FastAPI** que expone endpoints para:
   - Servir datos procesados (equipos, jugadores, partidos)
   - Cargar y ejecutar predicciones del modelo entrenado

7. **Frontend (UI):**  
   `frontend/` contiene una **app React + Vite** que consume la API para mostrar estadísticas, rankings y predicciones en tiempo real.

**Diagrama de Flujo:**
(LanusStats) → data/raw → ETL (backend/etl.py) → data/processed
→ Feature Engineering (scripts/generate_features.py)
→ data/features → Entrenamiento (scripts/train_model.py)
→ models/ → API (backend/app.py) → Frontend (frontend/)


---

### 🚀 Instalación y Ejecución (Windows / PowerShell)

#### **1. Clonar y Configurar el Entorno**
```powershell
# Clona el repositorio
git clone https://github.com/Francovasquezz/TPO_CienciaDeDatos.git
cd TPO_Futbol

# Crea y activa el entorno virtual
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Instala dependencias del backend
python -m pip install --upgrade pip
pip install -r backend/requirements.txt


cd frontend
npm install
cd ..
# Backend (API)
cd backend
uvicorn app:app --reload

# Frontend (en otra terminal)
cd frontend
npm run dev

📁 Estructura del Proyecto

TPO_Futbol/
├── backend/
│   ├── app.py                # FastAPI REST endpoints
│   ├── etl.py                # Extracción y limpieza (LanusStats)
│   ├── db.py                 # Conexión a base de datos o DuckDB
│   ├── model.py              # Carga y predicción del modelo
│   ├── requirements.txt
│   └── .env
│
├── scripts/
│   ├── dataset.py            # Descarga inicial de datos
│   ├── generate_features.py  # Feature engineering
│   └── train_model.py        # Entrenamiento de ML
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── features/
│
├── models/                   # Modelos serializados (.pkl / .joblib)
├── notebooks/                # Exploración y análisis EDA
├── frontend/                 # Interfaz React + Vite
├── .gitignore
└── README.md

🧠 Tecnologías Clave

| Componente      | Tecnología                                  |
| --------------- | ------------------------------------------- |
| Backend         | Python 3.10+, FastAPI, Uvicorn              |
| ETL / Data      | LanusStats, Pandas, Selenium, BeautifulSoup |
| ML              | Scikit-learn, Joblib                        |
| Base de datos   | Parquet / DuckDB                            |
| Frontend        | React, Vite, Axios                          |
| Infraestructura | Git + VS Code (Windows PowerShell)          |




# FBref (PL 24/25)
python backend/etl.py --league "Premier League" --season "2024-2025"

# Transfermarkt (season_id=2024)
python scripts/tm_pull_latest_values_playwright.py --league ENG1 --season 2024 --tm-domain com.ar --parquet

# Join
python scripts/join_tm_fbref.py --fbref "data/processed/player_stats_Premier_League_2024-2025.clean.csv" --tm "data/processed/tm_values_GB1_2024_latest.csv" --out "data/processed/join_pl_2024_2025.csv" --season-year 2024 --fuzzy-global-thresh 92



# Servicios de Machine Learning
El núcleo de esta API reside en dos servicios de scouting inteligente que proveen análisis avanzados sobre jugadores, ambos construidos con scikit-learn.

1. Servicio de Similitud (Endpoint: /player/{uuid}/similar)
Este servicio permite encontrar jugadores con perfiles estadísticos y estilos de juego similares.

Algoritmo: K-Nearest Neighbors (k-NN).

Funcionamiento: Las estadísticas de todos los jugadores se estandarizan (usando StandardScaler) y se representan como vectores en un espacio multidimensional. Cuando se consulta por un jugador, el modelo k-NN identifica a los k jugadores más cercanos (vecinos) en ese espacio utilizando la distancia euclidiana. El resultado es una lista de jugadores que, estadísticamente, rinden de forma más parecida al jugador objetivo.

2. Servicio de Oportunidades de Mercado (Endpoint: /market-opportunities)
Este servicio proactivamente identifica a los jugadores "infravalorados" o "gangas" del mercado.

Algoritmo: RandomForestRegressor (Regresor de Bosque Aleatorio).

Funcionamiento: Se entrenó un modelo de regresión supervisada donde las features (X) son las estadísticas de rendimiento de los jugadores y el target (Y) es su valor de mercado real (latest_mv_eur). El modelo RandomForestRegressor aprende la compleja relación no lineal entre el rendimiento en el campo y el valor de mercado.

Identificación de Oportunidades: El modelo se usa para predecir un "valor esperado" para cada jugador basado únicamente en sus estadísticas. El endpoint devuelve una lista de jugadores ordenada por la mayor diferencia positiva (Valor Predicho - Valor Real), señalando a aquellos que rinden como jugadores mucho más caros de lo que su precio actual indica.




