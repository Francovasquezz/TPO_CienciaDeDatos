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