// frontend/src/pages/PlayerDetail.tsx
import React from 'react';
import { useParams, Link } from 'react-router-dom';
import { usePlayerDetails } from '../api/hooks'; 
import { PlayerHeader } from '../components/PlayerHeader';
import { PlayerStatsTable } from '../components/PlayerStatsTable'; 
import { SimilarPlayersCard } from '../components/SimilarPlayersCard';

export const PlayerDetailPage: React.FC = () => {
  const { uuid } = useParams<{ uuid: string }>(); 
  const playerUuid = uuid || '';
  
  // Llamamos a la API real
  const { data: player, isLoading, isError, error } = usePlayerDetails(playerUuid, { season: '2024' });

  if (isLoading) {
    return <div className="p-8 text-center text-blue-400 animate-pulse">Cargando perfil del jugador...</div>;
  }
  
  if (isError) {
    console.error("Error detallado:", error);
    return (
        <div className="p-8 text-center text-red-500">
            <h2 className="text-xl font-bold">Error al cargar</h2>
            <p>No se pudieron obtener los datos. Revisa la consola.</p>
        </div>
    );
  }
  
  if (!player) {
    return <div className="p-8 text-center text-zinc-400">Jugador no encontrado.</div>;
  }
  
  return (
    <div className="space-y-8">
      <Link to="/" className="text-sm text-blue-400 hover:text-blue-300 transition-colors">&larr; Volver al Inicio</Link>
      
      {/* 1. Header con Datos Reales */}
      <PlayerHeader player={player} />
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        
        {/* Columna Izquierda: Jugadores Similares */}
        {/* (Movemos esto a la izquierda ya que eliminamos partidos y features) */}
        <div className="lg:col-span-1 space-y-6">
          <SimilarPlayersCard playerUuid={playerUuid} />
        </div>

        {/* Columna Derecha: Tabla de Rendimiento */}
        <div className="lg:col-span-2">
          <PlayerStatsTable player={player} />
        </div>
      </div>
      
      <p className="text-xs text-zinc-600 pt-8 border-t border-zinc-800 text-center">
          Datos provistos por TPO Fútbol Analytics • Temporada {player.season_code || 'Actual'}
      </p>
    </div>
  );
};