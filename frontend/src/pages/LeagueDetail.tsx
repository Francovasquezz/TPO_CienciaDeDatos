// frontend/src/pages/LeagueDetail.tsx
import React from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
import { useClubsByLeague } from '../api/hooks';
import { getLeagueLogo } from '../lib/league-assets';
import { Card, CardContent } from '@/components/ui/card';
import { ArrowLeft } from 'lucide-react';

export const LeagueDetailPage: React.FC = () => {
    const { leagueName } = useParams<{ leagueName: string }>();
    const name = decodeURIComponent(leagueName || '');
    const { data: clubs, isLoading } = useClubsByLeague(name);
    const navigate = useNavigate();

    const leagueLogo = getLeagueLogo(name);

    return (
        <div className="container mx-auto max-w-6xl py-8 space-y-8">
            <Link to="/leagues" className="text-zinc-400 hover:text-white flex items-center gap-2 transition-colors">
                <ArrowLeft className="w-4 h-4" /> Volver a Ligas
            </Link>

            <div className="flex items-center gap-6 bg-zinc-900/50 p-8 rounded-2xl border border-zinc-800">
                {leagueLogo ? (
                    <img src={leagueLogo} alt={name} className="w-24 h-24 object-contain" />
                ) : (
                    <div className="w-24 h-24 flex items-center justify-center bg-zinc-800 rounded-full text-4xl">🏆</div>
                )}
                <div>
                    <h1 className="text-4xl font-extrabold text-white">{name}</h1>
                    <p className="text-zinc-400 mt-2">{clubs?.length || 0} Clubes Registrados</p>
                </div>
            </div>

            {isLoading ? (
                <div className="text-center py-20 text-blue-500 animate-pulse">Cargando clubes...</div>
            ) : (
                <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-5 gap-4">
                    {clubs?.map((club) => {
                        return (
                            <Card 
                                key={club}
                                className="bg-zinc-900 border-zinc-800 hover:border-blue-500 cursor-pointer group transition-all"
                                onClick={() => navigate(`/clubs/${encodeURIComponent(club)}`)}
                            >
                                <CardContent className="p-6 flex flex-col items-center justify-center text-center gap-4 h-full">
                                    <div className="w-20 h-20 flex items-center justify-center">
                                        <div className="w-14 h-14 rounded-full bg-zinc-800 flex items-center justify-center text-zinc-600 text-2xl group-hover:bg-zinc-700 group-hover:text-white transition-colors">
                                            🛡️
                                        </div>
                                    </div>
                                    {/* CAMBIO: Agregado 'capitalize' */}
                                    <span className="font-bold text-zinc-300 group-hover:text-white text-sm capitalize">
                                        {club.toLowerCase()}
                                    </span>
                                </CardContent>
                            </Card>
                        );
                    })}
                </div>
            )}
        </div>
    );
};