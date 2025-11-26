// frontend/src/routes/router.tsx (ACTUALIZAR)
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { Layout } from "./layout"; // Asegúrate que coincida mayúsculas/minúsculas con tu archivo
import { DashboardPage } from "../pages/Dashboard"; 
import { PlayerDetailPage } from "../pages/PlayerDetail"; 
import { OpportunitiesPage } from "../pages/Opportunities"; // ⬅️ IMPORTAR
import { LeaguesPage } from "../pages/Leagues";
import { LeagueDetailPage } from "../pages/LeagueDetail";
import { ClubDetailPage } from "../pages/ClubDetail";
// Páginas simples
const AboutPage = () => <h1 className="text-white text-center mt-10">About / Metodología</h1>;
const PlayersPagePlaceholder = () => <h1 className="text-white text-center mt-10">Listado de Jugadores (Use la búsqueda en Home)</h1>;

const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      { index: true, element: <DashboardPage /> },
      // ⬅️ NUEVAS RUTAS
      { path: "leagues", element: <LeaguesPage /> }, 
      { path: "leagues/:leagueName", element: <LeagueDetailPage /> },
      { path: "clubs/:clubName", element: <ClubDetailPage /> },
      { path: "players", element: <PlayersPagePlaceholder /> }, 
      { path: "player/:uuid", element: <PlayerDetailPage /> }, 
      { path: "opportunities", element: <OpportunitiesPage /> }, // ⬅️ NUEVA RUTA
      { path: "about", element: <AboutPage /> },
    ],
  },
]);

export const AppRouter = () => <RouterProvider router={router} />;