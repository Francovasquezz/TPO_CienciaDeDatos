// frontend/src/pages/Dashboard.tsx (HOME PAGE)
import React from 'react';
import { PlayerSearchDropdown } from '../components/PlayerSearchDropdown';

export const DashboardPage: React.FC = () => {
    return (
        <div className="flex flex-col items-center justify-center min-h-[70vh] text-center p-6">
            <h1 className="text-6xl font-extrabold mb-4 text-gray-800">Football AI Analytics</h1>
            <p className="text-xl text-gray-600 mb-10">
                Encuentra, analiza y compara jugadores usando el poder del Machine Learning.
            </p>
            
            {/* El componente central de búsqueda con autocompletado */}
            <PlayerSearchDropdown />
            
            {/* Opcional: Card de KPIs aquí */}
        </div>
    );
};