// frontend/src/api/hooks.ts
import { useQuery } from '@tanstack/react-query';
import axiosClient from './client';
// Asumo que el PlayerDetailSchema y otros schemas están listos en lib/schemas
import { PlayerDetail, PlayerDetailSchema, PagedPlayers } from '../lib/schemas'; 

// --- Tipos de Datos (Deben coincidir con los Schemas Zod de tu backend) ---

// 1. Tipos para la Búsqueda (Paso 1 del flujo)
export interface SearchResult {
    player_uuid: string;
    full_name: string;
    primary_position: string;
    team_name: string; 
    market_value_eur?: number | null; // Añadido para el listado de jugadores
}

const fetchSearchPlayers = async (query: string): Promise<SearchResult[]> => {
    // Endpoint real: /players/search?query=...
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

// --- 2. Hook para Detalles del Jugador (Paso 2 del flujo) ---
interface PlayerDetailParams {
    season: string; // Parámetro del endpoint
}

const fetchPlayerDetails = async (uuid: string, params: PlayerDetailParams): Promise<PlayerDetail> => {
  // Endpoint real: /player/{uuid}/details?season=...
  const { data } = await axiosClient.get(`/player/${uuid}/details`, { params }); 
  return PlayerDetailSchema.parse(data); 
};

export const usePlayerDetails = (uuid: string, params: PlayerDetailParams) => {
  return useQuery({
    queryKey: ['playerDetail', uuid, params],
    queryFn: () => fetchPlayerDetails(uuid, params),
    enabled: !!uuid,
    // La data de detalle debe ser fresca
    staleTime: 10 * 60 * 1000, 
  });
};

// --- 3. Hook para Jugadores Similares (Paso 3 del flujo) ---
const fetchSimilarPlayers = async (uuid: string, n: number): Promise<SearchResult[]> => {
    // Endpoint real: /player/{uuid}/similar?n=...
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

// El hook usePlayers del PR1 se deja inactivo por ahora, pero se puede reactivar si se usa la página /players
// export const usePlayers = (params: PlayerQueryParams = {}) => { /* ... */ };