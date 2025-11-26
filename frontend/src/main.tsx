// frontend/src/main.tsx (CÓDIGO FINAL SIN MOCKS)
import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query' 
import { AppRouter } from './routes/router'

// 1. IMPORTANTE: NO importamos 'enableMocking'

import './styles/index.css'

// 2. Creamos el cliente de React Query
const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            staleTime: 5 * 60 * 1000, // Cache de 5 minutos
        }
    }
});

// 3. Renderizamos la aplicación directamente
ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
        <QueryClientProvider client={queryClient}>
            <AppRouter />
        </QueryClientProvider>
    </React.StrictMode>,
);