import pandas as pd
from sqlalchemy import create_engine
import sys

# Credenciales de Supabase (Connection Pooling - Transaction mode)
PASSWORD = "LettitPrime" 

DATABASE_URL = f"postgresql://postgres.eaipsfbrivaiqumhijdc:{PASSWORD}@aws-1-sa-east-1.pooler.supabase.com:6543/postgres"

print("Intentando conectar a Supabase...")

try:
    engine = create_engine(DATABASE_URL, connect_args={'connect_timeout': 10})
    # Probar conexión
    with engine.connect() as test_conn:
        print("✓ Conexión exitosa a Supabase\n")
except Exception as e:
    print(f"❌ Error de conexión: {str(e)}")
    print("\nVerifica:")
    print("1. Que la contraseña sea correcta")
    print("2. Que el proyecto esté activo en Supabase")
    sys.exit(1)

# Leer el CSV de Argentina
print("Leyendo archivo CSV...")
df = pd.read_csv('data/processed/join_arg_2025.csv')
print(f"Total de jugadores: {len(df)}")

# Separar jugadores de campo y arqueros
goalkeepers = df[df['IsGK'] == True].copy()
field_players = df[df['IsGK'] == False].copy()

print(f"Arqueros encontrados: {len(goalkeepers)}")
print(f"Jugadores de campo encontrados: {len(field_players)}")

# Columnas específicas para arqueros
gk_columns = [col for col in df.columns if col.startswith('GK_') or col in [
    'Player', 'Nation', 'Pos', 'Squad', 'Age', 'Born', 'MatchesPlayed', 
    'IsGK', 'AgeYears', 'player_norm', 'player_fl', 'club_norm', 
    'dob_fb', 'birth_year_fb', 'market_value_eur', 'player_id', 
    'dob', 'age', 'join_method'
]]

# Columnas para jugadores de campo
field_columns = [col for col in df.columns if not col.startswith('GK_')]

# Limpiar datos
goalkeepers_clean = goalkeepers[gk_columns]
field_players_clean = field_players[field_columns]

# Subir a Supabase
print("\nSubiendo datos a Supabase...")

try:
    with engine.connect() as conn:
        # Tabla de arqueros
        if len(goalkeepers_clean) > 0:
            goalkeepers_clean.to_sql(
                'goalkeepers_arg', 
                conn, 
                if_exists='replace', 
                index=False
            )
            print(f"✓ {len(goalkeepers_clean)} arqueros cargados → tabla 'goalkeepers_arg'")
        
        # Tabla de jugadores de campo
        if len(field_players_clean) > 0:
            field_players_clean.to_sql(
                'field_players_arg', 
                conn, 
                if_exists='replace', 
                index=False
            )
            print(f"✓ {len(field_players_clean)} jugadores de campo cargados → tabla 'field_players_arg'")
        
        conn.commit()
    
    print("\n✅ ¡Datos cargados exitosamente!")
    print("\n📊 Para ver tus datos:")
    print("   https://supabase.com/dashboard/project/eaipsfbrivaiqumhijdc/editor")
    
except Exception as e:
    print(f"\n❌ Error al subir datos: {str(e)}")
    sys.exit(1)