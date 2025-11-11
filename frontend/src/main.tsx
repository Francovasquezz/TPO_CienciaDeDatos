// frontend/src/main.tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query' 
import { AppRouter } from './routes/router'
// NOTA: Se asume que el código de MSW (mocks/browser.ts) fue eliminado o comentado

import './styles/index.css'

const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            // Configuración por defecto para evitar refetch innecesario
            staleTime: 5 * 60 * 1000, 
        }
    }
});

// RENDERIZADO FINAL (Llamará a FastAPI)
ReactDOM.createRoot(document.getElementById('root')!).render(
    <React.StrictMode>
        <QueryClientProvider client={queryClient}>
            <AppRouter />
        </QueryClientProvider>
    </React.StrictMode>,
);