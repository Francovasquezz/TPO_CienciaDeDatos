// frontend/src/pages/Players.tsx

import { usePlayers } from "../api/hooks";

// Definición temporal del tipo para facilitar la depuración
interface PagedPlayersDebug {
    items?: Array<any>;
    page?: number;
    page_size?: number;
    total?: number;
    // Capturará el mock completo si la estructura es incorrecta
    default?: any; 
}


export const PlayersPage = () => {
  // El hook ahora devuelve 'any' temporalmente, lo tratamos como PagedPlayersDebug
  const { data, isLoading, isError, error } = usePlayers(); 
  const pagedData = data as PagedPlayersDebug | undefined;

  // --- 1. Estado de Carga ---
  if (isLoading) {
    return (
      <div className="p-4 border rounded-lg bg-yellow-50 text-yellow-800">
        Cargando jugadores... (Datos desde MSW)
      </div>
    );
  }

  // --- 2. Estado de Error (de red o de JS) ---
  if (isError) {
    // Si hay un error de Zod, el error.message contiene el JSON del error, que es lo que queremos ver.
    const errorMessage = (error as any).message || "Error desconocido al contactar la API Mock.";
    return (
      <div className="p-4 border rounded-lg bg-red-100 text-red-800">
        <h1 className="text-xl font-bold">Error de Carga o Conexión</h1>
        <p>Hubo un problema al intentar obtener los datos mockeados.</p>
        <pre className="mt-2 text-xs overflow-auto bg-red-50 p-2 border border-red-300">
            {errorMessage}
        </pre>
      </div>
    );
  }

  // --- 3. Estado de Depuración de MOCK ---
  // Si los datos llegaron, pero NO tienen el campo 'items', asumimos que el mock está mal estructurado.
  if (pagedData && !pagedData.items) {
      // Intentamos mostrar el contenido crudo, que probablemente sea el error de Zod o el módulo JSON.
      return (
          <div className="p-4 border rounded-lg bg-orange-100 text-orange-800">
              <h1 className="text-xl font-bold">⚠️ Error de Estructura del Mock</h1>
              <p>El archivo <code className="font-mono">players.json</code> no está devolviendo la estructura esperada <code className="font-mono">{"{items: [], page: N, total: N}"}</code>.</p>
              <p className="mt-2 font-semibold">Contenido crudo recibido:</p>
              <pre className="mt-2 text-xs overflow-auto bg-orange-50 p-2 border border-orange-300">
                  {JSON.stringify(pagedData, null, 2)}
              </pre>
              <p className="mt-4 text-sm font-semibold">Revisa tu <code className="font-mono">src/mocks/fixtures/players.json</code> y cómo lo importas en <code className="font-mono">handlers.ts</code>.</p>
          </div>
      );
  }

  // --- 4. Estado Exitoso ---
  return (
    <div className="space-y-4">
      <h1 className="text-3xl font-bold">Lista de Jugadores (Mockeados)</h1>
      <p>Total de jugadores encontrados: {pagedData?.total || 0}</p>
      
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
        {pagedData?.items?.map((player) => (
          <div key={player.id} className="p-4 border rounded-lg shadow-sm bg-white">
            <h2 className="text-lg font-semibold">{player.name}</h2>
            <p className="text-sm text-gray-600">Posición: {player.position} | Edad: {player.age}</p>
            <p className="font-mono text-sm mt-1">
              Valor: ${player.market_value_eur?.toLocaleString() || 'N/A'} 
              {player.market_value_is_estimated && (
                <span className="ml-2 px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-800 rounded-full">
                  Estimado
                </span>
              )}
            </p>
          </div>
        ))}
      </div>
    </div>
  );
};