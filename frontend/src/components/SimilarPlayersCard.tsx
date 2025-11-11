// frontend/src/components/SimilarPlayersCard.tsx
import React from 'react';
import { useSimilarPlayers, SearchResult } from '../api/hooks';
import { Card, CardHeader, CardTitle, CardContent } from './ui/card';
import { Link } from 'react-router-dom';

interface SimilarPlayersCardProps {
    playerUuid: string;
}

export const SimilarPlayersCard: React.FC<SimilarPlayersCardProps> = ({ playerUuid }) => {
    // Busca 5 jugadores similares
    const { data: similarPlayers, isLoading, isError } = useSimilarPlayers(playerUuid, 5);

    if (isLoading) {
        return <div className="p-4 text-center text-blue-600 font-semibold">Calculando similitud...</div>;
    }
    
    if (isError) {
        return <div className="p-4 text-red-500">Error al cargar jugadores similares.</div>;
    }

    if (!similarPlayers || similarPlayers.length === 0) {
        return <div className="p-4 text-gray-500">No se encontraron jugadores con un perfil similar.</div>;
    }

    return (
        <Card className="mt-6">
            <CardHeader>
                <CardTitle className="text-xl">Jugadores con Perfil Similar</CardTitle>
            </CardHeader>
            <CardContent>
                <div className="space-y-3">
                    {similarPlayers.map((player) => (
                        <Link 
                            key={player.player_uuid} 
                            // Navega al detalle del jugador similar
                            to={`/player/${player.player_uuid}`}
                            className="block p-3 border rounded-lg hover:bg-blue-50 transition-colors"
                        >
                            <p className="font-semibold text-gray-800">{player.full_name}</p>
                            <div className="flex justify-between text-sm text-gray-600">
                                <span>{player.team_name} | {player.primary_position}</span>
                                <span className="font-medium">Valor: {player.market_value_eur?.toLocaleString('es-AR') || 'N/A'}</span>
                            </div>
                        </Link>
                    ))}
                </div>
            </CardContent>
        </Card>
    );
};