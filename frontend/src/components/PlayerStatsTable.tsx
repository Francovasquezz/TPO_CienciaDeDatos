// frontend/src/components/PlayerStatsTable.tsx
import React from 'react';
import { PlayerDetail } from '../lib/schemas';
import { Card, CardHeader, CardTitle, CardContent } from './ui/card';
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';

interface PlayerStatsTableProps {
  player: PlayerDetail;
}

export const PlayerStatsTable: React.FC<PlayerStatsTableProps> = ({ player }) => {
  const isGK = player.IsGK; 

  return (
    <Card className="bg-zinc-950 border-zinc-800 shadow-lg mt-8">
      <CardHeader className="border-b border-zinc-900">
        <CardTitle className="text-white flex items-center gap-3">
            <span>Estadísticas de la Temporada</span>
            <span className={`text-xs px-2 py-0.5 rounded border ${isGK ? 'bg-yellow-900/30 border-yellow-700 text-yellow-500' : 'bg-blue-900/30 border-blue-700 text-blue-400'}`}>
                {isGK ? 'ARQUERO' : 'JUGADOR DE CAMPO'}
            </span>
        </CardTitle>
      </CardHeader>
      <CardContent className="p-0 overflow-x-auto">
        <Table>
          <TableHeader className="bg-zinc-900">
            <TableRow className="border-zinc-800 hover:bg-zinc-900">
              {/* Encabezados Comunes */}
              <TableHead className="text-zinc-400 whitespace-nowrap">Partidos Jugados</TableHead>
              
              {isGK ? (
                /* Columnas Arquero */
                <>
                  <TableHead className="text-center text-zinc-400 whitespace-nowrap">Atajadas</TableHead>
                  <TableHead className="text-center text-zinc-400 whitespace-nowrap">Goles Rec.</TableHead>
                  <TableHead className="text-center text-zinc-400 whitespace-nowrap">% Atajadas</TableHead>
                  <TableHead className="text-center text-zinc-400 whitespace-nowrap">Vallas Inv.</TableHead>
                  <TableHead className="text-center text-zinc-400 whitespace-nowrap">PSxG</TableHead>
                </>
              ) : (
                /* Columnas Jugador de Campo (Lista Completa Solicitada) */
                <>
                  <TableHead className="text-center text-zinc-400 font-bold text-white">Goles</TableHead>
                  <TableHead className="text-center text-zinc-400 font-bold text-white">Asistencias</TableHead>
                  <TableHead className="text-center text-zinc-400" title="Goles Esperados">xG</TableHead>
                  <TableHead className="text-center text-zinc-400" title="Asistencias Esperadas">xAG</TableHead>
                  <TableHead className="text-center text-zinc-400">Tiros</TableHead>
                  <TableHead className="text-center text-zinc-400" title="Tiros al Arco">Tiros al Arco</TableHead>
                  <TableHead className="text-center text-zinc-400">Pases Comp.</TableHead>
                  <TableHead className="text-center text-zinc-400">Pases Int.</TableHead>
                  <TableHead className="text-center text-zinc-400">% Pases</TableHead>
                  <TableHead className="text-center text-zinc-400">Tackles</TableHead>
                  <TableHead className="text-center text-zinc-400">Tackles Gan.</TableHead>
                  <TableHead className="text-center text-zinc-400">Bloqueos</TableHead>
                  <TableHead className="text-center text-zinc-400">Intercep.</TableHead>
                </>
              )}
            </TableRow>
          </TableHeader>
          <TableBody>
            <TableRow className="hover:bg-zinc-900 border-zinc-800">
              {/* Valores Comunes */}
              <TableCell className="text-zinc-200 font-medium pl-4">{player.MatchesPlayed ?? '-'}</TableCell>

              {isGK ? (
                /* Valores Arquero */
                <>
                  <TableCell className="text-center text-blue-400 font-bold">{player.GK_Saves ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.GK_GA ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.GK_SavePct ? `${player.GK_SavePct}%` : '-'}</TableCell>
                  <TableCell className="text-center text-green-400">{player.GK_CS ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.GK_PSxG ?? '-'}</TableCell>
                </>
              ) : (
                /* Valores Jugador de Campo */
                <>
                  <TableCell className="text-center text-green-400 font-bold text-lg">{player.Gls ?? 0}</TableCell>
                  <TableCell className="text-center text-blue-400 font-bold text-lg">{player.Ast ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.xG ?? '-'}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.xAG ?? '-'}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.Shots ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.SoT ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.PassCmp ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.PassAtt ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.PassCmpPct ? `${player.PassCmpPct}%` : '-'}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.Tkl ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.TklW ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.Blocks ?? 0}</TableCell>
                  <TableCell className="text-center text-zinc-300">{player.Int ?? 0}</TableCell>
                </>
              )}
            </TableRow>
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
};