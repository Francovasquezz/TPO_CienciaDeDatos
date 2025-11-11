// frontend/src/components/PlayerHeader.tsx (DISEÑO FINAL Y CORREGIDO)
import React from 'react';
import { PlayerDetail } from '../lib/schemas';
import { Card, CardContent } from './ui/card';
import { Badge } from './ui/badge';

interface PlayerHeaderProps {
  player: PlayerDetail;
}

// Placeholder de Bandera: USAMOS CLASES DE W/H FIJO
const FlagImage: React.FC<{ nation: string }> = ({ nation }) => {
  const flag = nation === 'ARG' ? '🇦🇷' : '🏳️';
  // ⬅️ USAMOS text-2xl y w-8 h-8 para asegurar que se vea
  return <div className="text-2xl w-8 h-8 flex items-center justify-center">{flag}</div>; 
};

export const PlayerHeader: React.FC<PlayerHeaderProps> = ({ player }) => {
  const formattedValue = player.market_value_eur?.toLocaleString('es-AR', {
    style: 'currency',
    currency: 'EUR',
    minimumFractionDigits: 0,
  }) || 'Valor Ausente';

  return (
    <Card className="shadow-lg mb-8">
      <CardContent className="p-6">
        
        {/* Nombre del jugador */}
        <h1 className="text-4xl font-extrabold tracking-tight mb-4">{player.name}</h1>
        
        {/* Contenedor Principal: FOTO + DATOS */}
        <div className="flex gap-8 items-start">
            
            {/* 1. Bloque de Foto y Valor (Izquierda) */}
            <div className="flex-shrink-0 flex flex-col items-center">
                <img 
                    src={player.photo_url || 'https://via.placeholder.com/130x180?text=Foto'} 
                    alt={`Foto de ${player.name}`} 
                    className="w-[130px] h-[180px] object-cover rounded-md border" 
                />
                
                {/* VALOR DE MERCADO (Debajo de la foto) */}
                <p className="text-lg font-bold pt-4 flex flex-col items-center gap-1 w-full text-center">
                    Valor: {formattedValue}
                    {player.market_value_is_estimated && (
                        <Badge variant="destructive" className="text-sm">
                            ESTIMADO
                        </Badge>
                    )}
                </p>
                {/* ... (disclaimer de ML) */}
            </div>

            {/* 2. Bloque de Datos y Escudo/Bandera (Derecha de la foto) */}
            <div className="flex-grow space-y-4 pt-2">
                
                {/* 2a. Escudo y Bandera (Arriba y Juntos) */}
                <div className="flex items-center gap-4">
                    {/* Escudo - AHORA CON IMAGEN REAL Y CONTENCIÓN FIJA */}
                    {player.team_logo_url && (
                        <div className="w-8 h-8 flex-shrink-0"> {/* TAMAÑO PEQUEÑO FIJO (32x32px) */}
                            <img 
                                src={player.team_logo_url} 
                                alt="Escudo del club" 
                                className="w-full h-full object-contain" /> 
                        </div>
                    )}
                    {/* Bandera - MISMO TAMAÑO DEL ESCUDO */}
                    {player.nationality && <FlagImage nation={player.nationality} />}
                </div>

                {/* 2b. Datos Personales (Debajo) */}
                <div className="space-y-1 text-lg">
                    <p><strong>Edad:</strong> {player.age} años</p>
                    <p><strong>Posición:</strong> {player.position}</p>
                    <p><strong>Club:</strong> {player.team_name}</p>
                    <p><strong>Nacionalidad:</strong> {player.nationality}</p>
                </div>
            </div>
        </div>
        
      </CardContent>
    </Card>
  );
};