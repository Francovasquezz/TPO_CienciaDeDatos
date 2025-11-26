// frontend/src/pages/ClubDetail.tsx
import React from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { usePlayersByClub } from '../api/hooks';
// ⬅️ ELIMINADO: import { getClubLogoUrl } ...
import { Card, CardContent } from '@/components/ui/card'; // Mantenemos Card por si acaso, aunque no se usa en el listado simple
import { ArrowLeft } from 'lucide-react';

export const ClubDetailPage: React.FC = () => {
    const { clubName } = useParams<{ clubName: string }>();
    const name = decodeURIComponent(clubName || '');
    const { data: players, isLoading } = usePlayersByClub(name);
    const navigate = useNavigate();

    return (
        <div className="container mx-auto max-w-5xl py-8 space-y-8">
             <button onClick={() => navigate(-1)} className="text-zinc-400 hover:text-white flex items-center gap-2 transition-colors">
                <ArrowLeft className="w-4 h-4" /> Volver
            </button>

            <div className="border-b border-zinc-800 pb-6 flex items-center gap-4">
                {/* Placeholder de Escudo Grande */}
                <div className="w-16 h-16 bg-zinc-800 rounded-full flex items-center justify-center text-3xl border border-zinc-700">
                    🛡️
                </div>
                <div>
                    <h1 className="text-4xl font-extrabold text-white mb-1">{name}</h1>
                    <p className="text-zinc-400">Plantel Completo • {players?.length || 0} Jugadores</p>
                </div>
            </div>

            {isLoading ? (
                <div className="text-center py-20 text-blue-500 animate-pulse">Cargando plantel...</div>
            ) : (
                <div className="space-y-2">
                    {players?.map((player) => (
                        <div 
                            key={player.player_uuid}
                            onClick={() => navigate(`/player/${player.player_uuid}`)}
                            className="flex items-center justify-between p-4 bg-zinc-900 border border-zinc-800 rounded-lg hover:bg-zinc-800 cursor-pointer group transition-colors"
                        >
                            <div className="flex items-center gap-4">
                                {/* Avatar de iniciales para el jugador en la lista */}
                                <div className="w-10 h-10 rounded-full bg-zinc-950 flex items-center justify-center text-zinc-500 font-bold border border-zinc-800 group-hover:border-blue-500/30 transition-colors">
                                    {player.full_name.charAt(0)}
                                </div>
                                <div>
                                    <p className="font-bold text-white group-hover:text-blue-400 transition-colors">{player.full_name}</p>
                                    <p className="text-xs text-zinc-500">{player.primary_position}</p>
                                </div>
                            </div>
                            <span className="text-xs font-medium text-blue-500 border border-blue-900/30 px-3 py-1 rounded bg-blue-900/10">
                                Ver Perfil
                            </span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};