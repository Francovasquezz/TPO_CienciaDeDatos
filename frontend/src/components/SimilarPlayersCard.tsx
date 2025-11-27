// frontend/src/components/SimilarPlayersCard.tsx
import React from 'react';
import { useSimilarPlayers } from '../api/hooks';
import { Card, CardHeader, CardTitle, CardContent } from './ui/card';
import { Link } from 'react-router-dom';

interface SimilarPlayersCardProps {
    playerUuid: string;
}

export const SimilarPlayersCard: React.FC<SimilarPlayersCardProps> = ({ playerUuid }) => {
    const { data: similarPlayers, isLoading, isError } = useSimilarPlayers(playerUuid, 5);

    if (isLoading) {
        return <div className="p-4 text-center text-blue-400 font-semibold animate-pulse">Calculando similitud...</div>;
    }
    
    if (isError) {
        return <div className="p-4 text-red-500 text-center">Error al cargar similares.</div>;
    }

    if (!similarPlayers || similarPlayers.length === 0) {
        return <div className="p-4 text-zinc-500 text-center">No se encontraron jugadores similares.</div>;
    }

    return (
        <Card className="bg-zinc-900 border-zinc-800 h-full">
            <CardHeader>
                <CardTitle className="text-xl text-white">Jugadores Similares</CardTitle>
            </CardHeader>
            <CardContent>
                <div className="space-y-3">
                    {similarPlayers.map((player) => (
                        <Link 
                            key={player.player_uuid} 
                            to={`/player/${player.player_uuid}`}
                            className="block p-3 border border-zinc-800 rounded-lg hover:bg-zinc-800 hover:border-blue-500/50 transition-all group"
                        >
                            {/* CAMBIO: Capitalize en nombre */}
                            <p className="font-semibold text-zinc-200 group-hover:text-blue-400 transition-colors capitalize">
                                {player.full_name.toLowerCase()}
                            </p>
                            <div className="flex justify-between text-sm text-zinc-500 mt-1">
                                {/* CAMBIO: Capitalize en equipo */}
                                <span className="capitalize">{player.team_name?.toLowerCase() || 'Sin Club'} | {player.primary_position}</span>
                                {player.market_value_eur && (
                                    <span className="font-medium text-green-400">
                                        €{(player.market_value_eur / 1000000).toFixed(1)}M
                                    </span>
                                )}
                            </div>
                        </Link>
                    ))}
                </div>
            </CardContent>
        </Card>
    );
};