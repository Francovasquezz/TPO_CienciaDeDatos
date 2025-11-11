// frontend/src/components/PlayerSearchDropdown.tsx
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom'; 
import { useSearchPlayers, SearchResult } from '../api/hooks';
import { Input } from './ui/input';
import { Search } from 'lucide-react';

export const PlayerSearchDropdown: React.FC = () => {
    const [searchTerm, setSearchTerm] = useState('');
    const navigate = useNavigate();
    
    // El hook llama a /players/search
    const { data, isLoading } = useSearchPlayers(searchTerm);

    const handleSelectPlayer = (uuid: string) => {
        // Navega a la ruta de detalle con el UUID
        navigate(`/player/${uuid}`); 
        setSearchTerm(''); 
    };
    
    // Función para manejar el clic fuera del dropdown
    const handleBlur = () => {
        // Pequeño timeout para permitir que el clic en el resultado se registre antes de cerrar
        setTimeout(() => setSearchTerm(''), 150); 
    }

    // Se asume que el backend devuelve un array de SearchResult
    const results = data as SearchResult[] | undefined;

    return (
        <div className="relative w-full max-w-lg mx-auto">
            <Search className="absolute left-3 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
            <Input
                type="text"
                placeholder="Busca un jugador (ej: Julián, Enzo Pérez)..."
                className="w-full rounded-full pl-12 pr-4 h-12 text-lg shadow-xl border-2 focus:border-blue-500"
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                onBlur={handleBlur}
                onFocus={() => searchTerm.length > 0 && setSearchTerm(searchTerm)} // Abre si ya hay texto
            />
            
            {/* Menú Desplegable de Resultados */}
            {searchTerm.length > 2 && (
                <div className="absolute z-50 w-full mt-2 bg-white border border-gray-200 rounded-xl shadow-2xl max-h-80 overflow-y-auto">
                    {isLoading && (
                        <div className="p-3 text-center text-blue-600 font-semibold">Buscando...</div>
                    )}
                    
                    {!isLoading && results && results.length === 0 && (
                        <div className="p-3 text-center text-gray-500">No se encontraron resultados para "{searchTerm}".</div>
                    )}

                    {!isLoading && results && results.length > 0 && results.map((player) => (
                        <div
                            key={player.player_uuid}
                            className="p-3 cursor-pointer hover:bg-gray-100 flex justify-between items-center transition-colors border-b last:border-b-0"
                            // Usamos onMouseDown en lugar de onClick para evitar que el blur cierre antes de navegar
                            onMouseDown={() => handleSelectPlayer(player.player_uuid)} 
                        >
                            <div>
                                <p className="font-semibold text-gray-800">{player.full_name}</p>
                                <p className="text-xs text-gray-500">{player.team_name} | {player.primary_position}</p>
                            </div>
                            <span className="text-sm font-medium text-blue-600">Valor: {player.market_value_eur?.toLocaleString('es-AR') || 'N/A'}</span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};