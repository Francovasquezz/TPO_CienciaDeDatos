// frontend/src/api/hooks.ts (VERSIÓN FINAL CON ZOD ACTIVO)
import { useQuery } from '@tanstack/react-query';
import axiosClient from './client';
import { PagedPlayers, PagedPlayersSchema } from '../lib/schemas'; // ⬅️ ZOD ACTIVO

const fetchPlayers = async (): Promise<PagedPlayers> => {
  const { data } = await axiosClient.get('/players');
  return PagedPlayersSchema.parse(data); // ⬅️ VALIDACIÓN ACTIVA
};
// ...
export const usePlayers = () => {
  return useQuery({
    queryKey: ['players'],
    queryFn: fetchPlayers,
    // Agregamos un tiempo de "stale" para simular el caching de datos
    staleTime: 5 * 60 * 1000, 
  });
};