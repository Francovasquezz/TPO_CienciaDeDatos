// frontend/src/mocks/handlers.ts (CÓDIGO FINAL Y FUNCIONAL)
import { http, HttpResponse } from 'msw';
import playerDetailData from './fixtures/playerDetail.json'; // Mantenemos esta importación

// Importamos el archivo JSON de lista de jugadores de forma dinámica para evitar problemas de TS/Vite module
import playersData from './fixtures/players.json';

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000';

// Función auxiliar para obtener el objeto JSON correcto
const getPlayersData = () => {
    // Maneja la importación de JSON: usa .default si existe (problema común de Webpack/Vite)
    return (playersData as any).default || playersData;
};

// Función auxiliar para obtener el objeto JSON de detalle
const getPlayerDetailData = () => {
    return (playerDetailData as any).default || playerDetailData;
};


export const handlers = [
  // 1. Mock para GET /players (LISTA PAGINADA)
  http.get(`${API_BASE_URL}/players`, () => {
    console.log('MSW: Respondiendo a /players con LISTA DE JUGADORES mockeada.');
    
    // Devolvemos el JSON de la LISTA DE JUGADORES
    return HttpResponse.json(getPlayersData(), { status: 200 });
  }),

  // 2. Mock para GET /players/{id} (DETALLE)
  http.get(`${API_BASE_URL}/players/:id`, () => {
    console.log('MSW: Respondiendo a /players/:id con DETALLE DE JUGADOR mockeado.');
    
    // Devolvemos el JSON del DETALLE DE JUGADOR
    return HttpResponse.json(getPlayerDetailData(), { status: 200 });
  }),
];