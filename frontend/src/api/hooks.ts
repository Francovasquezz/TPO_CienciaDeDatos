// frontend/src/api/hooks.ts
import { useQuery } from '@tanstack/react-query';
import axiosClient from './client';
import { PagedPlayers, PagedPlayersSchema, PlayerDetail, PlayerDetailSchema } from '../lib/schemas';

// --- Hook del PR1 (Lista de Jugadores) ---
const fetchPlayers = async (): Promise<PagedPlayers> => {
  const { data } = await axiosClient.get('/players');
  return PagedPlayersSchema.parse(data); // Validamos
};

export const usePlayers = () => {
  return useQuery({
    queryKey: ['players'],
    queryFn: fetchPlayers,
    staleTime: 5 * 60 * 1000, 
  });
};

// --- Hook del PR2 (Detalle de Jugador) ---
const fetchPlayerDetail = async (id: string): Promise<PlayerDetail> => {
  const { data } = await axiosClient.get(`/players/${id}`);
  return PlayerDetailSchema.parse(data); // Validamos
};

export const usePlayerDetail = (id: string) => {
  return useQuery({
    queryKey: ['player', id],
    queryFn: () => fetchPlayerDetail(id),
    enabled: !!id, // Solo se ejecuta si el ID no es nulo
    staleTime: 5 * 60 * 1000,
  });
};