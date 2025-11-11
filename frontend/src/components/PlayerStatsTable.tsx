// frontend/src/components/PlayerStatsTable.tsx
import React from 'react';
import { PlayerDetail } from '../lib/schemas';
import { Card, CardHeader, CardTitle, CardContent } from './ui/card'; 
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from './ui/table';

// Definimos las columnas según el CSV y la lógica de portero
const FIELD_PLAYER_COLUMNS = ['PJ', 'Goles', 'Asist.', 'xG', 'xA', 'Minutos'];
const GOALKEEPER_COLUMNS = ['PJ', 'Goles Rec.', 'Vallas 0', '% Atajadas', 'Minutos'];

interface PlayerStatsTableProps {
  player: PlayerDetail;
}

export const PlayerStatsTable: React.FC<PlayerStatsTableProps> = ({ player }) => {
  const isGK = player.is_goalkeeper;
  const columns = isGK ? GOALKEEPER_COLUMNS : FIELD_PLAYER_COLUMNS;
  const stats = player.competition_stats || [];

  // Función para renderizar las celdas de datos por competición
  const renderPlayerRow = (stat: any) => {
    if (isGK) {
      return (
        <>
          <TableCell className="text-center">{stat.matches}</TableCell>
          <TableCell className="text-center">{stat.goals_received ?? 0}</TableCell>
          <TableCell className="text-center">{stat.clean_sheets ?? 0}</TableCell>
          <TableCell className="text-center">
            {stat.save_percentage !== undefined ? `${(stat.save_percentage * 100).toFixed(1)}%` : 'N/A'}
          </TableCell>
          <TableCell className="text-center">{stat.minutes}</TableCell>
        </>
      );
    }
    // Lógica para Jugador de Campo
    return (
      <>
        <TableCell className="text-center">{stat.matches}</TableCell>
        <TableCell className="text-center">{stat.goals ?? 0}</TableCell>
        <TableCell className="text-center">{stat.assists ?? 0}</TableCell>
        <TableCell className="text-center">{stat.xG ?? 'N/A'}</TableCell>
        <TableCell className="text-center">{stat.xA ?? 'N/A'}</TableCell>
        <TableCell className="text-center">{stat.minutes}</TableCell>
      </>
    );
  };

  // Función para calcular y renderizar la fila de total (parte inferior)
  const renderTotalRow = () => {
    const totalMinutes = stats.reduce((sum, s) => sum + s.minutes, 0);
    const totalMatches = stats.reduce((sum, s) => sum + s.matches, 0);

    if (isGK) {
      const totalGoalsReceived = stats.reduce((sum, s) => sum + (s.goals_received ?? 0), 0);
      const totalCleanSheets = stats.reduce((sum, s) => sum + (s.clean_sheets ?? 0), 0);
      return (
        <>
          <TableCell className="text-center">{totalMatches}</TableCell>
          <TableCell className="text-center">{totalGoalsReceived}</TableCell>
          <TableCell className="text-center">{totalCleanSheets}</TableCell>
          <TableCell className="text-center" colSpan={1}>N/A</TableCell>
          <TableCell className="text-center">{totalMinutes}</TableCell>
        </>
      );
    }
    
    const totalGoals = stats.reduce((sum, s) => sum + (s.goals ?? 0), 0);
    const totalAssists = stats.reduce((sum, s) => sum + (s.assists ?? 0), 0);
    return (
      <>
        <TableCell className="text-center">{totalMatches}</TableCell>
        <TableCell className="text-center">{totalGoals}</TableCell>
        <TableCell className="text-center">{totalAssists}</TableCell>
        <TableCell className="text-center" colSpan={2}>N/A</TableCell>
        <TableCell className="text-center">{totalMinutes}</TableCell>
      </>
    );
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle>Rendimiento por Competición</CardTitle>
      </CardHeader>
      <CardContent className="p-0">
        <div className="flex justify-between p-4 border-b">
            <span className="font-semibold text-sm">Compacto (Ampliado - TBD)</span>
        </div>
        <div className="overflow-x-auto">
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead className="w-[180px]">Competición</TableHead>
                {columns.map(col => (
                  <TableHead key={col} className="text-center">{col}</TableHead>
                ))}
              </TableRow>
            </TableHeader>
            <TableBody>
              {stats.map((stat, index) => (
                <TableRow key={index}>
                  <TableCell className="font-medium">{stat.competition}</TableCell>
                  {renderPlayerRow(stat)}
                </TableRow>
              ))}
              {/* Fila del Total */}
              <TableRow className="font-bold bg-gray-50 dark:bg-gray-800/50">
                <TableCell>Total</TableCell>
                {renderTotalRow()}
              </TableRow>
            </TableBody>
          </Table>
        </div>
      </CardContent>
    </Card>
  );
};