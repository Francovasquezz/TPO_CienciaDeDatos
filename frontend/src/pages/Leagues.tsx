// frontend/src/pages/Leagues.tsx
import React, { useState, useRef } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useLeagues } from '../api/hooks';
import { getLeagueLogo } from '../lib/league-assets';
import { Card, CardContent } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Search, ChevronRight, ChevronLeft } from 'lucide-react';

export const LeaguesPage: React.FC = () => {
    const { data: leagues, isLoading } = useLeagues();
    const [searchTerm, setSearchTerm] = useState("");
    const navigate = useNavigate();
    
    // Referencia para el contenedor del carrusel
    const scrollContainerRef = useRef<HTMLDivElement>(null);

    const filteredLeagues = leagues?.filter(l => 
        l.toLowerCase().includes(searchTerm.toLowerCase())
    );

    const featuredLeagues = [
        "Premier League",
        "LaLiga",
        "Serie A",
        "Bundesliga",
        "Ligue 1",
        "Primera División Argentina",
        "Major League Soccer",
        "Liga Portugal",
        "Eredivisie"
    ];

    // Función para desplazar el carrusel
    const scroll = (direction: 'left' | 'right') => {
        if (scrollContainerRef.current) {
            const { current } = scrollContainerRef;
            const scrollAmount = 300; // Cantidad de píxeles a desplazar
            if (direction === 'left') {
                current.scrollBy({ left: -scrollAmount, behavior: 'smooth' });
            } else {
                current.scrollBy({ left: scrollAmount, behavior: 'smooth' });
            }
        }
    };

    return (
        <div className="container mx-auto max-w-6xl py-8 space-y-12">
            
            {/* Encabezado y Buscador */}
            <div className="text-center space-y-6">
                <h1 className="text-4xl md:text-5xl font-extrabold text-white tracking-tight">
                    Explorador de Ligas
                </h1>
                <div className="relative max-w-md mx-auto">
                    <Search className="absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-gray-400" />
                    <Input 
                        type="text" 
                        placeholder="Buscar competición..." 
                        className="w-full rounded-full pl-12 pr-4 h-12 bg-zinc-900 border-zinc-700 text-white focus:border-blue-500 transition-colors"
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                    />
                </div>
            </div>

            {/* Carrusel de Ligas Destacadas con Flechas */}
            {!searchTerm && (
                <div className="space-y-4 relative group">
                    <h2 className="text-xl font-bold text-zinc-400 uppercase tracking-wider">Destacadas</h2>
                    
                    <div className="relative flex items-center">
                        {/* Botón Izquierdo */}
                        <button 
                            onClick={() => scroll('left')}
                            className="absolute left-0 z-10 p-2 bg-black/50 hover:bg-blue-600 text-white rounded-full -translate-x-4 opacity-0 group-hover:opacity-100 transition-all duration-300 backdrop-blur-sm border border-zinc-700"
                        >
                            <ChevronLeft className="w-6 h-6" />
                        </button>

                        {/* Contenedor Scrollable (Sin barra visible) */}
                        <div 
                            ref={scrollContainerRef}
                            className="flex gap-6 overflow-x-auto pb-4 snap-x no-scrollbar scroll-smooth"
                        >
                            {featuredLeagues.map((league) => { 
                                const logo = getLeagueLogo(league);
                                return (
                                    <div 
                                        key={league} 
                                        onClick={() => navigate(`/leagues/${encodeURIComponent(league)}`)}
                                        className="snap-center flex-shrink-0 w-40 h-40 bg-zinc-900 border border-zinc-800 rounded-xl flex flex-col items-center justify-center p-4 cursor-pointer hover:border-blue-500 hover:bg-zinc-800 transition-all group/card"
                                    >
                                        {logo ? (
                                            <img src={logo} alt={league} className="w-20 h-20 object-contain mb-3 group-hover/card:scale-110 transition-transform" />
                                        ) : (
                                            <span className="text-4xl mb-2">🏆</span>
                                        )}
                                        <span className="text-xs text-center font-bold text-zinc-300 group-hover/card:text-white">{league}</span>
                                    </div>
                                );
                            })}
                        </div>

                        {/* Botón Derecho */}
                        <button 
                            onClick={() => scroll('right')}
                            className="absolute right-0 z-10 p-2 bg-black/50 hover:bg-blue-600 text-white rounded-full translate-x-4 opacity-0 group-hover:opacity-100 transition-all duration-300 backdrop-blur-sm border border-zinc-700"
                        >
                            <ChevronRight className="w-6 h-6" />
                        </button>
                    </div>
                </div>
            )}

            {/* Lista Completa de Resultados */}
            <div className="space-y-4">
                <h2 className="text-xl font-bold text-zinc-400 uppercase tracking-wider">
                    {searchTerm ? 'Resultados' : 'Todas las Competiciones'}
                </h2>
                
                {isLoading ? (
                    <div className="text-blue-500 animate-pulse text-center py-10">Cargando ligas...</div>
                ) : (
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                        {filteredLeagues?.map((league) => (
                            <Link 
                                key={league} 
                                to={`/leagues/${encodeURIComponent(league)}`}
                                className="block group"
                            >
                                <Card className="bg-zinc-900 border-zinc-800 hover:border-blue-500 transition-colors">
                                    <CardContent className="p-4 flex items-center justify-between">
                                        <div className="flex items-center gap-4">
                                            <div className="w-10 h-10 bg-zinc-950 rounded-full flex items-center justify-center p-1 border border-zinc-800">
                                                {getLeagueLogo(league) ? (
                                                    <img src={getLeagueLogo(league)!} className="w-full h-full object-contain" />
                                                ) : (
                                                    <span className="text-xs font-bold text-zinc-600">L</span>
                                                )}
                                            </div>
                                            <span className="font-bold text-zinc-200 group-hover:text-white">{league}</span>
                                        </div>
                                        <ChevronRight className="w-5 h-5 text-zinc-600 group-hover:text-blue-500" />
                                    </CardContent>
                                </Card>
                            </Link>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
};