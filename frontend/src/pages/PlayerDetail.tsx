// frontend/src/pages/PlayerDetail.tsx
import React from 'react';
import { useParams, Link } from 'react-router-dom';
// ⬅️ HOOKS ACTUALIZADOS
import { usePlayerDetails } from '../api/hooks'; 
import { PlayerHeader } from '../components/PlayerHeader';
import { PlayerStatsTable } from '../components/PlayerStatsTable'; 
import { SimilarPlayersCard } from '../components/SimilarPlayersCard'; // ⬅️ NUEVO COMPONENTE
// COMPONENTES UI CON RUTAS RELATIVAS (PARA EVITAR PROBLEMAS DE RESOLUCIÓN)
import { Card, CardHeader, CardTitle, CardContent } from '../components/ui/card';

// Componente para Partidos Recientes (Requerido para la estructura)
const RecentMatches: React.FC<{ matches: any[] }> = ({ matches }) => {
    // ... (El código de RecentMatches es el mismo)
    const getResultColor = (result: string) => {
        if (result === 'W') return 'bg-green-500';
        if (result === 'L') return 'bg-red-500';
        return 'bg-gray-500';
    };

    return (
        <Card>
            <CardHeader>
                <CardTitle>Últimos Partidos</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
                {matches.map((match, index) => (
                    <div key={index} className="flex items-center text-sm p-2 rounded-md hover:bg-gray-50">
                        <span className={`w-6 h-6 flex items-center justify-center rounded-full text-white font-bold text-xs ${getResultColor(match.result)}`}>
                            {match.result}
                        </span>
                        <div className="ml-3 flex-grow">
                            <span className="font-medium">{match.home_team}</span>
                            <span className="mx-2 font-bold">{match.home_score} - {match.away_score}</span>
                            <span className="font-medium">{match.away_team}</span>
                        </div>
                        <span className="text-xs text-gray-500">{match.competition}</span>
                    </div>
                ))}
            </CardContent>
        </Card>
    );
};

// Componente para el Aporte del Modelo (Feature Attribution)
const FeatureAttribution: React.FC<{ features: any[] }> = ({ features }) => (
  // ... (El código de FeatureAttribution es el mismo)
  <Card>
    <CardHeader>
      <CardTitle>Aporte del Modelo (ML)</CardTitle>
    </CardHeader>
    <CardContent>
      <p className="text-sm text-gray-600 mb-3">
        Variables que más influyeron en el valor de mercado estimado:
      </p>
      <ul className="space-y-2">
        {features.map((feat, index) => (
          <li key={index} className="flex justify-between text-sm">
            <span>{feat.feature}</span>
            <span className={`font-medium ${feat.contribution > 0 ? 'text-green-600' : 'text-red-600'}`}>
              {(feat.contribution * 100).toFixed(0)}%
            </span>
          </li>
        ))}
      </ul>
    </CardContent>
  </Card>
);


export const PlayerDetailPage: React.FC = () => {
  // ⬅️ LEEMOS EL UUID DEL JUGADOR
  const { uuid } = useParams<{ uuid: string }>(); 
  const playerUuid = uuid || '';
  
  // ⬅️ LLAMAMOS AL HOOK DE DETALLES CON LA TEMPORADA
  const { data: player, isLoading, isError } = usePlayerDetails(playerUuid, { season: '2024' });

  // --- Manejo de Estados de Carga/Error ---
  if (isLoading) {
    return <div className="p-4">Cargando detalles del jugador...</div>;
  }
  if (isError) {
    return <div className="p-4 text-red-600">Error al cargar el detalle del jugador (UUID: {playerUuid}). Asegúrate que el backend esté sirviendo /player/{playerUuid}/details.</div>;
  }
  if (!player) {
    return <div className="p-4">Jugador no encontrado.</div>;
  }
  
  return (
    <div className="space-y-6">
      <Link to="/" className="text-sm text-blue-600 hover:underline">&larr; Volver al Inicio</Link>
      
      {/* 1. Header (Foto, Datos, Valor) */}
      <PlayerHeader player={player} />
      
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        
        {/* Columna Izquierda (Partidos y Aporte ML) */}
        <div className="lg:col-span-1 space-y-6">
          {/* 2. Últimos Partidos */}
          {player.recent_matches && <RecentMatches matches={player.recent_matches} />}
          
          {/* 3. Aporte del Modelo */}
          {player.market_value_is_estimated && player.feature_attribution && (
            <FeatureAttribution features={player.feature_attribution} />
          )}
          
          {/* ⬅️ 4. Sección de Jugadores Similares */}
          <SimilarPlayersCard playerUuid={playerUuid} />
        </div>

        {/* Columna Derecha (Rendimiento) */}
        <div className="lg:col-span-2">
          {/* 5. Tabla de Rendimiento (GK / Campo) */}
          <PlayerStatsTable player={player} />
        </div>
      </div>
      
      {/* Footer de la página */}
      <p className="text-xs text-gray-500 pt-4 border-t">
          Fuentes: {player.sources.join(', ')}. Última actualización: {new Date(player.last_updated).toLocaleDateString()}.
      </p>
    </div>
  );
};