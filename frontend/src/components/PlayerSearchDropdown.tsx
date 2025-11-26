// frontend/src/components/PlayerSearchDropdown.tsx
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom'; 
import { useSearchPlayers, SearchResult } from '../api/hooks';
import { Input } from './ui/input';
import { Search } from 'lucide-react';

export const PlayerSearchDropdown: React.FC = () => {
    const [searchTerm, setSearchTerm] = useState('');
    const navigate = useNavigate();
    const { data, isLoading } = useSearchPlayers(searchTerm);

    const handleSelectPlayer = (uuid: string) => {
        navigate(`/player/${uuid}`); 
        setSearchTerm(''); 
    };
    
    const handleBlur = () => {
        setTimeout(() => setSearchTerm(''), 150); 
    }

    const results = data as SearchResult[] | undefined;

    return (
        <div className="relative w-full max-w-lg mx-auto">
            <Search className="absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-gray-400" />
            
            {/* Input estilo Dark Futurista */}
            <Input
                type="text"
                placeholder="Busca un jugador (ej: Julián)..."
                className="w-full rounded-full pl-12 pr-4 h-14 text-lg shadow-lg shadow-blue-900/20 bg-zinc-900 border-zinc-700 text-white placeholder:text-gray-500 focus:border-blue-500 focus:ring-2 focus:ring-blue-900 transition-all"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onBlur={handleBlur}
                onFocus={() => searchTerm.length > 0 && setSearchTerm(searchTerm)} 
            />
            
            {/* Dropdown Oscuro */}
            {searchTerm.length > 2 && (
                <div className="absolute z-50 w-full mt-2 bg-zinc-900 border border-zinc-700 rounded-xl shadow-2xl max-h-80 overflow-y-auto text-left">
                    
                    {isLoading && (
                        <div className="p-4 text-center text-blue-400 font-medium animate-pulse">Analizando base de datos...</div>
                    )}
                    
                    {!isLoading && results && results.length === 0 && (
                        <div className="p-4 text-center text-gray-500">No se encontraron resultados.</div>
                    )}

                    {!isLoading && results && results.length > 0 && results.map((player) => (
                        <div
                            key={player.player_uuid}
                            className="p-4 cursor-pointer hover:bg-zinc-800 flex justify-between items-center transition-colors border-b border-zinc-800 last:border-b-0 group"
                            onMouseDown={() => handleSelectPlayer(player.player_uuid)} 
                        >
                            <div>
                                <p className="font-bold text-gray-200 group-hover:text-blue-400 transition-colors">{player.full_name}</p>
                                <p className="text-xs text-gray-500 uppercase tracking-wide">{player.team_name} • {player.primary_position}</p>
                            </div>
                            <span className="text-xs font-medium text-zinc-500 group-hover:text-white transition-colors border border-zinc-700 rounded px-2 py-1">Ver Ficha</span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};