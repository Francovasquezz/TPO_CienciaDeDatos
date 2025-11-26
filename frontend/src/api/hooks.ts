// frontend/src/api/hooks.ts
import { useQuery } from '@tanstack/react-query';
import axiosClient from './client';
import { PlayerDetail, PlayerDetailSchema } from '../lib/schemas'; 

// --- 1. Tipos para la Búsqueda ---
export interface SearchResult {
    player_uuid: string;
    full_name: string;
    primary_position: string;
    // Hacemos opcionales los campos que quizás no vengan en la búsqueda
    team_name?: string; 
    market_value_eur?: number | null; 
}

const fetchSearchPlayers = async (query: string): Promise<SearchResult[]> => {
    // Llamada al endpoint real de búsqueda
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
  // El backend devuelve una LISTA de temporadas.
  const { data } = await axiosClient.get(`/player/${uuid}/details`);
  
  // Tomamos el PRIMER elemento (la temporada más reciente según el orden del backend)
  const playerData = Array.isArray(data) ? data[0] : data;
  
  // Validamos y devolvemos un solo objeto
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

// frontend/src/api/hooks.ts

// ... (imports anteriores)

// --- 4. Hook para Oportunidades de Mercado ---
// Reutilizamos SearchResult porque la estructura es similar (lista de jugadores)
// Pero idealmente el backend devuelve más datos de 'valor real' vs 'predicho'.
// Por ahora usamos una interfaz genérica que extienda SearchResult.

export interface MarketOpportunity extends SearchResult {
    age?: number;
    market_value_eur?: number | null;
    predicted_value_eur?: number | null; // Asumo que el backend devuelve esto para oportunidades
}

const fetchMarketOpportunities = async (limit: number = 50): Promise<MarketOpportunity[]> => {
    // Endpoint real: /market-opportunities
    const { data } = await axiosClient.get(`/market-opportunities?limit=${limit}`);
    return data; 
};

export const useMarketOpportunities = (limit: number = 50) => {
    return useQuery({
        queryKey: ['marketOpportunities', limit],
        queryFn: () => fetchMarketOpportunities(limit),
        staleTime: 10 * 60 * 1000, // 10 minutos de cache
    });
};

// frontend/src/api/hooks.ts

// ... (imports anteriores)

// --- 5. Hooks para Ligas y Clubes ---

// Obtener lista de nombres de ligas
const fetchLeagues = async (): Promise<string[]> => {
    const { data } = await axiosClient.get('/leagues');
    return data;
};

export const useLeagues = () => {
    return useQuery({
        queryKey: ['leagues'],
        queryFn: fetchLeagues,
        staleTime: 24 * 60 * 60 * 1000, // Cache de 24hs (las ligas no cambian seguido)
    });
};

// Obtener clubes de una liga
const fetchClubsByLeague = async (leagueName: string): Promise<string[]> => {
    // encodeURIComponent es vital porque las ligas tienen espacios
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

// Obtener jugadores de un club
// Reutilizamos la interfaz SearchResult porque devuelve uuid, nombre, posicion
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