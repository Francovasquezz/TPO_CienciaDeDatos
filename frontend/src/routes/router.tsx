// frontend/src/routes/Router.tsx
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { Layout } from "./layout";
import { DashboardPage } from "../pages/Dashboard"; // ⬅️ NUEVO IMPORT
import { PlayersPage } from "../pages/Players"; 
import { PlayerDetailPage } from "../pages/PlayerDetail"; // ⬅️ NUEVO IMPORT

// Páginas simples
const AboutPage = () => <h1>About / Metodología</h1>;

const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      // ⬅️ Home Page
      { index: true, element: <DashboardPage /> }, 
      
      // ⬅️ Ruta de Lista (Se mantiene, aunque el foco es la búsqueda)
      { path: "players", element: <PlayersPage /> }, 
      
      // ⬅️ Ruta de Detalle (Basada en el UUID del flujo)
      { path: "player/:uuid", element: <PlayerDetailPage /> }, 
      
      { path: "about", element: <AboutPage /> },
    ],
  },
]);

export const AppRouter = () => <RouterProvider router={router} />;