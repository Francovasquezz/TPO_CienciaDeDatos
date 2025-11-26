// frontend/src/components/PlayerHeader.tsx
import React, { useState } from 'react';
import { PlayerDetail } from '../lib/schemas';
import { Card, CardContent } from './ui/card';
import { Badge } from './ui/badge';

interface PlayerHeaderProps {
  player: PlayerDetail;
}

// --- Componente de Bandera ---
const FlagImage: React.FC<{ nation: string }> = ({ nation }) => {
    const cleanNation = nation?.split(' ').pop() || nation;
    return <div className="font-bold text-zinc-400 text-lg bg-black/50 px-2 rounded">{cleanNation}</div>;
};

// --- Helper para generar URL de Escudo ---
// Intenta buscar el logo en el repo público basado en el nombre del equipo
const getTeamLogoUrl = (teamName: string | null | undefined) => {
    if (!teamName) return null;
    
    // Normalizamos el nombre para que coincida con el formato de archivos (ej: "Racing Club" -> "racing-club")
    // Esto es una aproximación, funcionará para muchos equipos comunes.
    const slug = teamName.toLowerCase()
        .replace(/\s+/g, '-')     // Espacios a guiones
        .replace(/\./g, '')       // Quitar puntos
        .normalize("NFD").replace(/[\u0300-\u036f]/g, ""); // Quitar acentos

    // URL del repositorio público de logos (Asumimos Argentina por defecto para este TPO)
    return `https://raw.githubusercontent.com/Leo4815162342/football-logos/main/assets/logos/argentina/${slug}.png`;
};

export const PlayerHeader: React.FC<PlayerHeaderProps> = ({ player }) => {
  // Manejo de Valor de Mercado (Muestra "Sin Cotización" si es null/0)
  const formattedValue = (player.MarketValueEUR !== null && player.MarketValueEUR !== undefined && player.MarketValueEUR > 0)
    ? player.MarketValueEUR.toLocaleString('es-AR', { style: 'currency', currency: 'EUR', minimumFractionDigits: 0 }) 
    : 'Sin Cotización';

  const teamName = player.Squad || 'Sin Club';
  // Intentamos obtener el logo real
  const teamLogoUrl = getTeamLogoUrl(teamName);
  
  // URL para el avatar del jugador usando sus iniciales (mejor que un gris plano)
  const playerAvatarUrl = `https://ui-avatars.com/api/?name=${player.player_name}&background=18181b&color=fff&size=256&font-size=0.33&bold=true`;

  // Estado para manejar error de carga de imagen de escudo
  const [logoError, setLogoError] = useState(false);

  return (
    <div className="grid grid-cols-1 lg:grid-cols-[auto_1fr] gap-8 mb-8">
        
        {/* 1. BLOQUE FOTO (Izquierda) */}
        <Card className="bg-zinc-900 border-zinc-800 shadow-xl overflow-hidden max-w-[220px] self-start">
            <CardContent className="p-0 relative">
                {/* Foto del Jugador (Avatar con Iniciales) */}
                <div className="w-[220px] h-[260px] bg-zinc-950 flex flex-col items-center justify-center relative overflow-hidden">
                    <img 
                        src={playerAvatarUrl} 
                        alt={player.player_name} 
                        className="w-full h-full object-cover opacity-80 hover:opacity-100 transition-opacity"
                    />
                    
                    {/* Bandera */}
                    {player.Nation && (
                        <div className="absolute bottom-2 right-2">
                            <FlagImage nation={player.Nation} />
                        </div>
                    )}
                </div>
                
                {/* Barra del Club */}
                <div className="bg-zinc-950 p-3 flex items-center justify-center gap-3 border-t border-zinc-800 h-14">
                    {!logoError && teamLogoUrl ? (
                        <img 
                            src={teamLogoUrl} 
                            alt={teamName} 
                            className="w-8 h-8 object-contain"
                            onError={() => setLogoError(true)} // Si falla, ocultamos la imagen
                        />
                    ) : (
                        // Fallback si no encuentra el logo: Escudo genérico
                        <div className="w-8 h-8 rounded-full bg-zinc-800 flex items-center justify-center text-xs border border-zinc-700">🛡️</div>
                    )}
                    <span className="font-bold text-sm text-white truncate max-w-[150px] capitalize">
                        {teamName}
                    </span>
                </div>
            </CardContent>
        </Card>

        {/* 2. BLOQUE INFORMACIÓN */}
        <div className="flex flex-col justify-center py-2 space-y-6">
            
            <div>
                <h1 className="text-5xl font-bold text-white tracking-tight mb-3 capitalize">
                    {player.player_name}
                </h1>
                <div className="flex items-center gap-4">
                    <Badge className="bg-blue-600 text-white hover:bg-blue-700 px-3 py-1 text-md border-none">
                        {player.Pos}
                    </Badge>
                    <span className="text-zinc-400 text-xl font-light">
                        {player.Age ? `${player.Age} Años` : 'Edad N/A'}
                    </span>
                </div>
            </div>

            {/* Valor de Mercado Destacado */}
            <div className="bg-zinc-900/80 border border-zinc-800 p-5 rounded-xl max-w-md shadow-lg relative overflow-hidden group">
                <div className="absolute top-0 left-0 w-1 h-full bg-blue-600"></div>
                <p className="text-zinc-500 text-xs uppercase tracking-widest font-semibold mb-1">
                    Valor de Mercado Actual
                </p>
                <p className="text-4xl font-bold text-white tracking-tight group-hover:text-blue-400 transition-colors">
                    {formattedValue}
                </p>
            </div>
            
            <div className="grid grid-cols-2 gap-4 max-w-md">
                <div className="p-4 rounded-lg bg-zinc-900 border border-zinc-800">
                    <p className="text-xs text-zinc-500 uppercase font-bold mb-1 text-blue-500">Nacionalidad</p>
                    <p className="font-medium text-zinc-200 truncate">{player.Nation || 'N/A'}</p>
                </div>
                <div className="p-4 rounded-lg bg-zinc-900 border border-zinc-800">
                    <p className="text-xs text-zinc-500 uppercase font-bold mb-1 text-green-500">Partidos (Temp.)</p>
                    <p className="font-medium text-zinc-200">{player.MatchesPlayed || 0}</p>
                </div>
            </div>
        </div>
    </div>
  );
};