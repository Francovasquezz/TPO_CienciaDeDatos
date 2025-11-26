// frontend/src/pages/PlayerDetail.tsx
import React from 'react';
import { useParams, Link, useLocation } from 'react-router-dom';
import { TrendingUp, Info } from 'lucide-react'; // ⬅️ Iconos para la tarjeta de oportunidad
import { usePlayerDetails } from '../api/hooks'; 
import { PlayerHeader } from '../components/PlayerHeader';
import { PlayerStatsTable } from '../components/PlayerStatsTable'; 
import { SimilarPlayersCard } from '../components/SimilarPlayersCard';

// 1. Definimos la interfaz para los datos que vienen por navegación
interface OpportunityState {
    opportunityData?: {
        predicted_value: number;
        actual_value: number;
        diff: number;
        is_opportunity: boolean;
    }
}

export const PlayerDetailPage: React.FC = () => {
  const { uuid } = useParams<{ uuid: string }>(); 
  const playerUuid = uuid || '';
  
  // 2. Hook para leer el estado de la navegación (datos de oportunidad)
  const location = useLocation();
  const state = location.state as OpportunityState;
  const opportunityData = state?.opportunityData;

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
    <div className="space-y-8 animate-in fade-in duration-500">
      <Link to="/" className="text-sm text-blue-400 hover:text-blue-300 transition-colors">&larr; Volver al Inicio</Link>
      
      {/* Header con Datos Reales */}
      <PlayerHeader player={player} />
      
      {/* 3. SECCIÓN NUEVA: Banner de Oportunidad de Mercado */}
      {/* Solo se muestra si venimos desde la pantalla de Oportunidades */}
      {opportunityData && (
        <div className="relative overflow-hidden rounded-xl border border-green-500/30 bg-gradient-to-r from-green-900/40 to-black/60 p-6 shadow-lg shadow-green-900/10">
            <div className="absolute top-0 right-0 p-4 opacity-10">
                <TrendingUp className="w-32 h-32 text-green-400" />
            </div>
            
            <div className="relative z-10 flex flex-col md:flex-row items-center justify-between gap-6">
                <div className="flex items-start gap-4">
                    <div className="p-3 rounded-full bg-green-500/10 border border-green-500/20 text-green-400">
                        <TrendingUp className="w-6 h-6" />
                    </div>
                    <div>
                        <h3 className="text-xl font-bold text-white flex items-center gap-2">
                            Oportunidad de Mercado Detectada
                        </h3>
                        <p className="text-zinc-400 text-sm mt-1 max-w-md">
                            Nuestro modelo de IA indica que el rendimiento de este jugador supera significativamente su valoración actual.
                        </p>
                    </div>
                </div>

                <div className="flex items-center gap-8 bg-black/30 p-4 rounded-lg border border-white/5">
                    <div className="text-right">
                        <p className="text-xs text-zinc-500 uppercase mb-0.5">Valor Actual</p>
                        <p className="text-lg font-medium text-zinc-300">
                            €{(opportunityData.actual_value / 1000000).toFixed(1)}M
                        </p>
                    </div>
                    
                    <div className="h-8 w-px bg-zinc-700"></div>

                    <div className="text-right">
                        <p className="text-xs text-green-400 uppercase mb-0.5 font-bold">Predicción IA</p>
                        <p className="text-2xl font-bold text-white">
                            €{(opportunityData.predicted_value / 1000000).toFixed(1)}M
                        </p>
                        <span className="text-xs font-mono text-green-400 bg-green-900/30 px-1.5 py-0.5 rounded">
                            +€{(opportunityData.diff / 1000000).toFixed(1)}M
                        </span>
                    </div>
                </div>
            </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        
        {/* Columna Izquierda: Jugadores Similares */}
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