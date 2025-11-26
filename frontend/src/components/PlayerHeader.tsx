// frontend/src/components/PlayerHeader.tsx
import React from 'react';
import { PlayerDetail } from '../lib/schemas';
import { Card, CardContent } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { formatMarketValue } from '../lib/utils'; // Asegúrate de que esto esté importado

interface PlayerHeaderProps {
  player: PlayerDetail;
}

export const PlayerHeader: React.FC<PlayerHeaderProps> = ({ player }) => {
    // CORRECCIÓN AQUI:
    // Priorizamos el número real (MarketValueEUR) para poder darle nuestro propio formato.
    // Solo usamos 'market_value_show' si no hay un número válido (por si el backend manda un texto tipo "Libre").
    const formattedValue = (player.MarketValueEUR !== null && player.MarketValueEUR !== undefined && player.MarketValueEUR > 0)
        ? formatMarketValue(player.MarketValueEUR)
        : (player.market_value_show || 'Sin Cotización');

  return (
    <div className="grid grid-cols-1 lg:grid-cols-[auto_1fr] gap-8 mb-8">
        
        {/* 1. BLOQUE FOTO */}
        <Card className="bg-zinc-900 border-zinc-800 shadow-xl overflow-hidden max-w-[220px] self-start">
            <CardContent className="p-0 relative">
                <div className="w-[220px] h-[260px] bg-zinc-800 flex flex-col items-center justify-center text-zinc-500">
                    <span className="text-4xl font-bold mb-2">{player.player_name.charAt(0)}</span>
                    <span className="text-xs font-mono">FOTO</span>
                </div>
                
                <div className="bg-zinc-950 p-3 flex items-center justify-center gap-3 border-t border-zinc-800 h-14">
                    <div className="w-8 h-8 rounded-full bg-zinc-800 flex items-center justify-center text-xs border border-zinc-700 text-zinc-400">
                        ESC
                    </div>
                    <span className="font-bold text-sm text-white truncate max-w-[150px] capitalize">
                        {player.Squad || 'Sin Club'}
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

            {/* VALOR DE MERCADO */}
            <div className="bg-zinc-900/80 border border-zinc-800 p-5 rounded-xl max-w-md shadow-lg relative overflow-hidden">
                <div className="absolute top-0 left-0 w-1 h-full bg-blue-600"></div>
                <p className="text-zinc-500 text-xs uppercase tracking-widest font-semibold mb-1">
                    Valor de Mercado Actual
                </p>
                <p className="text-4xl font-bold text-white tracking-tight">
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