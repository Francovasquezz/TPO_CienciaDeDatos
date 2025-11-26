// frontend/src/components/PlayerHighlights.tsx
import React from 'react';
import { Card, CardHeader, CardTitle, CardContent } from './ui/card';
import { PlayCircle } from 'lucide-react';

export const PlayerHighlights: React.FC = () => {
  return (
    <Card className="h-full border-zinc-800 bg-zinc-900/50">
      <CardHeader className="pb-2">
        <CardTitle className="text-md font-medium text-blue-400 flex items-center gap-2">
          <PlayCircle className="w-4 h-4" /> Highlights & Goles
        </CardTitle>
      </CardHeader>
      <CardContent>
        {/* Placeholder de Video 1 */}
        <div className="aspect-video bg-zinc-950 rounded-md border border-zinc-800 flex items-center justify-center mb-3 group cursor-pointer hover:border-blue-500 transition-colors">
            <div className="text-center">
                <PlayCircle className="w-10 h-10 text-zinc-700 group-hover:text-blue-500 transition-colors mx-auto mb-2" />
                <span className="text-xs text-zinc-500">Ver Mejores Jugadas 2024</span>
            </div>
        </div>
        {/* Lista de otros videos */}
        <div className="space-y-2">
            <div className="h-8 bg-zinc-800/50 rounded w-full animate-pulse"></div>
            <div className="h-8 bg-zinc-800/50 rounded w-3/4 animate-pulse"></div>
        </div>
      </CardContent>
    </Card>
  );
};