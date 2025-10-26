// frontend/src/main.tsx (MODIFICAR)
import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query' // <-- NUEVO IMPORT
import { AppRouter } from './routes/router'
import { enableMocking } from './mocks/browser' 

// Importar los estilos 
import './styles/index.css'

// 1. Crear una instancia del cliente de consulta
const queryClient = new QueryClient();

// Se asegura que React inicie SÓLO después de que MSW esté listo
enableMocking().then(() => {
    ReactDOM.createRoot(document.getElementById('root')!).render(
        <React.StrictMode>
            {/* 2. ENVOLVEMOS TODA LA APLICACIÓN CON EL PROVEEDOR */}
            <QueryClientProvider client={queryClient}>
                <AppRouter />
            </QueryClientProvider>
        </React.StrictMode>,
    );
});