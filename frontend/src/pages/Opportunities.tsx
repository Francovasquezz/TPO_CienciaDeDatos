// frontend/src/pages/Opportunities.tsx
import React, { useState } from 'react';
import { useMarketOpportunities } from '../api/hooks';
import { Card, CardContent } from '@/components/ui/card';
import { ArrowLeft, TrendingUp, Info } from 'lucide-react';
import { useNavigate } from 'react-router-dom';

// --- Componente de Tarjeta de Posición (Menú Principal) ---
interface PositionCardProps {
    title: string;
    image: string;
    onClick: () => void;
}

const PositionCard: React.FC<PositionCardProps> = ({ title, image, onClick }) => {
    return (
        <div 
            onClick={onClick}
            className="relative group cursor-pointer overflow-hidden rounded-2xl border border-zinc-800 h-[400px] transition-all duration-500 hover:shadow-2xl hover:shadow-blue-900/20"
        >
            <div 
                className="absolute inset-0 bg-cover bg-center transition-transform duration-700 group-hover:scale-110"
                style={{ backgroundImage: `url(${image})` }}
            />
            
            <div className="absolute inset-0 bg-gradient-to-t from-black/90 via-black/50 to-transparent opacity-80 transition-opacity duration-300 group-hover:opacity-90" />

            <div className="absolute inset-0 flex flex-col items-center justify-center p-4 text-center">
                <h3 className="text-2xl md:text-3xl font-bold text-white uppercase tracking-wider transition-transform duration-500 group-hover:-translate-y-4 break-words w-full">
                    {title}
                </h3>
                
                <div className="absolute bottom-10 opacity-0 translate-y-4 transition-all duration-500 group-hover:opacity-100 group-hover:translate-y-0">
                    <span className="px-6 py-2 bg-white text-black font-bold rounded-full text-sm uppercase tracking-wider hover:bg-blue-500 hover:text-white transition-colors">
                        Ver Oportunidades
                    </span>
                </div>
            </div>
        </div>
    );
};


// --- Componente de Lista de Resultados ---
interface OpportunitiesListProps {
    position: string; // 'FW', 'MF', 'DF', 'GK'
    onBack: () => void;
}

const OpportunitiesList: React.FC<OpportunitiesListProps> = ({ position, onBack }) => {
    // Pedimos 100 oportunidades
    const { data: opportunities, isLoading, isError } = useMarketOpportunities(200); 
    const navigate = useNavigate();

    // Filtrado por Posición
    const filteredPlayers = opportunities?.filter(p => p.primary_position.includes(position));

    const getPositionLabel = (pos: string) => {
        switch(pos) {
            case 'FW': return 'Delanteros';
            case 'MF': return 'Mediocentros';
            case 'DF': return 'Defensores';
            case 'GK': return 'Arqueros';
            default: return 'Jugadores';
        }
    };

    if (isLoading) return <div className="p-20 text-center text-blue-500 animate-pulse text-xl">Analizando el mercado con IA...</div>;
    if (isError) return <div className="p-20 text-center text-red-500 text-xl">Error al conectar con el servidor de datos.</div>;

    return (
        <div className="animate-in fade-in slide-in-from-bottom-4 duration-500">
            <button 
                onClick={onBack}
                className="flex items-center gap-2 text-zinc-400 hover:text-white mb-8 transition-colors group"
            >
                <ArrowLeft className="w-4 h-4 group-hover:-translate-x-1 transition-transform" /> Volver a Categorías
            </button>

            <div className="flex items-center gap-3 mb-8">
                <h2 className="text-4xl font-bold text-white">
                    Oportunidades: <span className="text-blue-500">{getPositionLabel(position)}</span>
                </h2>
                <span className="px-3 py-1 rounded-full bg-blue-900/30 text-blue-400 text-xs font-mono border border-blue-800">
                    {filteredPlayers?.length || 0} Resultados
                </span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {filteredPlayers?.map((player) => (
                    <Card 
                        key={player.player_uuid}
                        className="bg-zinc-900 border-zinc-800 hover:border-blue-500 transition-all duration-300 cursor-pointer group hover:shadow-lg hover:shadow-blue-900/20"
                        // 👇 AQUÍ ESTÁ LA MAGIA PARA PASAR LOS DATOS AL DETALLE
                        onClick={() => navigate(`/player/${player.player_uuid}`, { 
                            state: { 
                                opportunityData: {
                                    predicted_value: player.predicted_value_eur, // Valor IA
                                    actual_value: player.actual_value_eur,       // Valor Real (Corregido)
                                    diff: player.value_diff_eur,                 // Ganancia (Corregido)
                                    is_opportunity: true
                                }
                            } 
                        })}
                    >
                        <CardContent className="p-4 flex items-center gap-4">
                            {/* Avatar / Inicial */}
                            <div className="w-14 h-14 rounded-full bg-zinc-800 flex items-center justify-center text-zinc-400 font-bold border border-zinc-700 group-hover:border-blue-500/50 transition-colors shrink-0">
                                {player.full_name.charAt(0)}
                            </div>
                            
                            {/* Info del Jugador */}
                            <div className="flex-grow min-w-0">
                                <p className="font-bold text-white text-lg truncate group-hover:text-blue-400 transition-colors">
                                    {player.full_name}
                                </p>
                                <div className="flex items-center gap-2">
                                    <span className="text-xs font-medium px-1.5 py-0.5 rounded bg-zinc-800 text-zinc-400 border border-zinc-700">
                                        {player.primary_position}
                                    </span>
                                    <p className="text-xs text-zinc-500 truncate max-w-[120px]">
                                        {player.team_name || 'Sin Club'}
                                    </p>
                                </div>
                            </div>
                            
                            {/* Valores (Corregido para mostrar Value Actual vs IA) */}
                            <div className="text-right flex flex-col items-end">
                                <p className="text-xs text-zinc-500 uppercase mb-0.5">Valor Actual</p>
                                <p className="text-md font-bold text-zinc-300">
                                    {player.actual_value_eur 
                                        ? `€${(player.actual_value_eur / 1000000).toFixed(1)}M`
                                        : 'N/A'}
                                </p>
                                
                                {/* Mostrar pequeño el valor de IA para tentar al clic */}
                                {player.predicted_value_eur && (
                                    <p className="text-xs text-green-500 font-bold mt-1">
                                        IA: €{(player.predicted_value_eur / 1000000).toFixed(1)}M
                                    </p>
                                )}
                            </div>
                        </CardContent>
                    </Card>
                ))}
                
                {filteredPlayers?.length === 0 && (
                    <div className="col-span-full py-20 text-center">
                        <div className="inline-block p-4 rounded-full bg-zinc-900 mb-4">
                            <TrendingUp className="w-8 h-8 text-zinc-600" />
                        </div>
                        <p className="text-zinc-500 text-lg">No se encontraron oportunidades destacadas en esta posición por el momento.</p>
                    </div>
                )}
            </div>
        </div>
    );
};


// --- Página Principal ---
export const OpportunitiesPage: React.FC = () => {
    const [selectedPosition, setSelectedPosition] = useState<string | null>(null);

    // IMÁGENES LOCALES
    const images = {
        FW: "/img/delantero.jpg", 
        MF: "/img/mediocampista.jpg",
        DF: "/img/defensor.jpg",
        GK: "/img/arquero.jpg"
    };

    return (
        <div className="container mx-auto max-w-7xl py-10 px-4">
            
            {!selectedPosition ? (
                // VISTA 1: Selección de Categoría
                <div className="space-y-12 animate-in fade-in duration-700">
                    
                    {/* Encabezado */}
                    <div className="text-center space-y-6 max-w-3xl mx-auto">
                        <h1 className="text-5xl md:text-6xl font-extrabold text-white tracking-tight">
                            Oportunidades de Mercado
                        </h1>
                        
                        {/* Explicación Profesional */}
                        <div className="bg-blue-900/10 border border-blue-900/30 rounded-xl p-6 backdrop-blur-sm">
                            <p className="text-lg md:text-xl text-blue-200 font-light flex items-center justify-center gap-3">
                                <Info className="w-6 h-6 text-blue-400 flex-shrink-0" />
                                <span>
                                    Descubre talentos ocultos cuyo <span className="font-bold text-white">rendimiento</span> supera significativamente su <span className="font-bold text-white">valoración actual de mercado</span>.
                                </span>
                            </p>
                        </div>
                        
                        <p className="text-zinc-500 uppercase tracking-widest text-sm pt-8">
                            Selecciona una posición para comenzar el scouting
                        </p>
                    </div>

                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                        <PositionCard 
                            title="DELANTEROS" 
                            image={images.FW}
                            onClick={() => setSelectedPosition('FW')}
                        />
                        <PositionCard 
                            title="MEDIOCENTROS" 
                            image={images.MF}
                            onClick={() => setSelectedPosition('MF')}
                        />
                        <PositionCard 
                            title="DEFENSORES" 
                            image={images.DF}
                            onClick={() => setSelectedPosition('DF')}
                        />
                        <PositionCard 
                            title="ARQUEROS" 
                            image={images.GK}
                            onClick={() => setSelectedPosition('GK')}
                        />
                    </div>
                </div>
            ) : (
                // VISTA 2: Lista de Jugadores Filtrada
                <OpportunitiesList 
                    position={selectedPosition} 
                    onBack={() => setSelectedPosition(null)} 
                />
            )}
        </div>
    );
};