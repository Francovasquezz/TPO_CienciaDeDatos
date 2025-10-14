from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os, pandas as pd

app = FastAPI(title="TPO Futbol API")

DATA_DIR = os.getenv("DATA_DIR", "data/processed")

class Team(BaseModel):
    team_id: str
    name: str
    league: str
    season: int

class Player(BaseModel):
    player_uuid: str
    full_name: str
    team_id: str | None = None
    season: int | None = None

def load_df(name: str):
    pqt = os.path.join(DATA_DIR, f"{name}.parquet")
    csv = os.path.join(DATA_DIR, f"{name}.csv")
    if os.path.exists(pqt): return pd.read_parquet(pqt)
    if os.path.exists(csv): return pd.read_csv(csv)
    raise FileNotFoundError(name)

@app.get("/teams")
def get_teams(league: str | None = None, season: int | None = None):
    df = load_df("teams")
    if league: df = df[df["league"] == league]
    if season is not None: df = df[df["season"] == season]
    return [Team(team_id=str(r["team_id"]), name=r["name"], league=r["league"], season=int(r["season"])).dict()
            for _, r in df.iterrows()]

@app.get("/players")
def get_players(team_id: str | None = None, season: int | None = None):
    df = load_df("players")  # exportalo en tu ETL
    if team_id: df = df[df["team_id"].astype(str) == str(team_id)]
    if season is not None: df = df[df["season"] == season]
    cols = {"player_uuid","full_name","team_id","season"}
    cols = [c for c in df.columns if c in cols]
    return [Player(**{k: (None if pd.isna(v) else v) for k,v in r[cols].to_dict().items()}).dict()
            for _, r in df.iterrows()]
