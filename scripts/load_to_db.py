import pandas as pd
from sqlalchemy import create_engine

# Configurar conexión directa
DATABASE_URL = "postgresql://postgres:LettitPrime@db.eaipsfbrivaiqumhijdc.supabase.co:5432/postgres"

engine = create_engine(DATABASE_URL)

# Leer el CSV de Argentina
print("Leyendo archivo CSV...")
df = pd.read_csv('data/processed/join_arg_2025.csv')
print(f"Total de jugadores: {len(df)}")

# Separar jugadores de campo y arqueros
goalkeepers = df[df['IsGK'] == True].copy()
field_players = df[df['IsGK'] == False].copy()

print(f"Arqueros encontrados: {len(goalkeepers)}")
print(f"Jugadores de campo encontrados: {len(field_players)}")

# Columnas específicas para arqueros (las que tienen datos)
gk_columns = [col for col in df.columns if col.startswith('GK_') or col in [
    'Player', 'Nation', 'Pos', 'Squad', 'Age', 'Born', 'MatchesPlayed', 
    'IsGK', 'AgeYears', 'player_norm', 'player_fl', 'club_norm', 
    'dob_fb', 'birth_year_fb', 'market_value_eur', 'player_id', 
    'dob', 'age', 'join_method'
]]

# Columnas para jugadores de campo (sin columnas GK_)
field_columns = [col for col in df.columns if not col.startswith('GK_')]

# Limpiar datos de arqueros
goalkeepers_clean = goalkeepers[gk_columns]

# Limpiar datos de jugadores de campo
field_players_clean = field_players[field_columns]

# Crear tablas en la BD
print("\nSubiendo datos a Supabase...")

with engine.connect() as conn:
    # Tabla de arqueros
    if len(goalkeepers_clean) > 0:
        goalkeepers_clean.to_sql(
            'goalkeepers_arg', 
            conn, 
            if_exists='replace', 
            index=False
        )
        print(f"✓ {len(goalkeepers_clean)} arqueros cargados en tabla 'goalkeepers_arg'")
    
    # Tabla de jugadores de campo
    if len(field_players_clean) > 0:
        field_players_clean.to_sql(
            'field_players_arg', 
            conn, 
            if_exists='replace', 
            index=False
        )
        print(f"✓ {len(field_players_clean)} jugadores de campo cargados en tabla 'field_players_arg'")
    
    conn.commit()

print("\n✅ Datos de Argentina cargados exitosamente a Supabase")
print("\nPara ver los datos, ve a:")
print("https://supabase.com → Tu proyecto → Table Editor")