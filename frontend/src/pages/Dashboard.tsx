// frontend/src/pages/Dashboard.tsx
import React from 'react';
import { PlayerSearchDropdown } from '../components/PlayerSearchDropdown';

export const DashboardPage: React.FC = () => {
    return (
        <div className="flex flex-col items-center justify-center min-h-[70vh] text-center p-6">
            {/* CAMBIO: text-gray-800 -> text-white */}
            <h1 className="text-6xl font-extrabold mb-4 text-white">Football AI Analytics</h1>
            <p className="text-xl text-gray-400 mb-10">
                Encuentra, analiza y compara jugadores usando el poder del Machine Learning.
            </p>
            
            {/* Componente de búsqueda */}
            <PlayerSearchDropdown />
        </div>
    );
};