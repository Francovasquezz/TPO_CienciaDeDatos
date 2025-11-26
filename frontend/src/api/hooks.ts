// frontend/src/api/hooks.ts
import { useQuery } from '@tanstack/react-query';
import axiosClient from './client';
import { PlayerDetail, PlayerDetailSchema } from '../lib/schemas'; 

// --- 1. Tipos para la Búsqueda ---
export interface SearchResult {
    player_uuid: string;
    full_name: string;
    primary_position: string;
    team_name?: string; 
    market_value_eur?: number | null; 
}

const fetchSearchPlayers = async (query: string): Promise<SearchResult[]> => {
    const { data } = await axiosClient.get(`/players/search?query=${query}&limit=10`);
    return data; 
};

export const useSearchPlayers = (query: string) => {
    return useQuery({
        queryKey: ['playerSearch', query],
        queryFn: () => fetchSearchPlayers(query),
        enabled: query.length > 2, 
        staleTime: 0, 
        gcTime: 0, 
    });
};

// --- 2. Hook para Detalles del Jugador ---
interface PlayerDetailParams {
    season: string; 
}

const fetchPlayerDetails = async (uuid: string, params: PlayerDetailParams): Promise<PlayerDetail> => {
  const { data } = await axiosClient.get(`/player/${uuid}/details`);
  // Tomamos el PRIMER elemento (la temporada más reciente)
  const playerData = Array.isArray(data) ? data[0] : data;
  return PlayerDetailSchema.parse(playerData); 
};

export const usePlayerDetails = (uuid: string, params: PlayerDetailParams) => {
  return useQuery({
    queryKey: ['playerDetail', uuid, params],
    queryFn: () => fetchPlayerDetails(uuid, params),
    enabled: !!uuid,
    staleTime: 10 * 60 * 1000, 
  });
};

// --- 3. Hook para Jugadores Similares ---
export interface SimilarPlayerResult extends SearchResult {
    team?: string;
    market_value_eur?: number | null;
}

const fetchSimilarPlayers = async (uuid: string, n: number): Promise<SimilarPlayerResult[]> => {
    const { data } = await axiosClient.get(`/player/${uuid}/similar?n=${n}`);
    return data; 
};

export const useSimilarPlayers = (uuid: string, n: number = 5) => {
    return useQuery({
        queryKey: ['similarPlayers', uuid, n],
        queryFn: () => fetchSimilarPlayers(uuid, n),
        enabled: !!uuid,
    });
};

// --- 4. Hook para Oportunidades de Mercado ---

// 👇 CAMBIO IMPORTANTE AQUÍ: Actualizamos la interfaz para que coincida con tu JSON
export interface MarketOpportunity extends SearchResult {
    age?: number;
    league_name?: string;
    season_code?: string;
    
    // Estos son los campos que te daban error:
    actual_value_eur?: number;     // El valor real que viene del JSON
    predicted_value_eur?: number;  // La predicción de la IA
    value_diff_eur?: number;       // La diferencia (ganancia potencial)
    value_ratio?: number;          // El ratio de oportunidad
    
    MatchesPlayed?: number;
}

const fetchMarketOpportunities = async (limit: number = 50): Promise<MarketOpportunity[]> => {
    const { data } = await axiosClient.get(`/market-opportunities?limit=${limit}`);
    return data; 
};

export const useMarketOpportunities = (limit: number = 50) => {
    return useQuery({
        queryKey: ['marketOpportunities', limit],
        queryFn: () => fetchMarketOpportunities(limit),
        staleTime: 10 * 60 * 1000, 
    });
};

// --- 5. Hooks para Ligas y Clubes ---

const fetchLeagues = async (): Promise<string[]> => {
    const { data } = await axiosClient.get('/leagues');
    return data;
};

export const useLeagues = () => {
    return useQuery({
        queryKey: ['leagues'],
        queryFn: fetchLeagues,
        staleTime: 24 * 60 * 60 * 1000,
    });
};

const fetchClubsByLeague = async (leagueName: string): Promise<string[]> => {
    const { data } = await axiosClient.get(`/leagues/${encodeURIComponent(leagueName)}/clubs`);
    return data;
};

export const useClubsByLeague = (leagueName: string) => {
    return useQuery({
        queryKey: ['clubs', leagueName],
        queryFn: () => fetchClubsByLeague(leagueName),
        enabled: !!leagueName,
    });
};

const fetchPlayersByClub = async (clubName: string): Promise<SearchResult[]> => {
    const { data } = await axiosClient.get(`/clubs/${encodeURIComponent(clubName)}/players`);
    return data;
};

export const usePlayersByClub = (clubName: string) => {
    return useQuery({
        queryKey: ['clubPlayers', clubName],
        queryFn: () => fetchPlayersByClub(clubName),
        enabled: !!clubName,
    });
};