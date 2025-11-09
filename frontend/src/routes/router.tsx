// frontend/src/routes/Router.tsx
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { Layout } from "./layout";
import { PlayersPage } from "../pages/Players"; 
import { PlayerDetailPage } from "../pages/PlayerDetail"; // <-- Importamos la página real

// Páginas simples
const DashboardPage = () => <h1>Dashboard (Home)</h1>;
const AboutPage = () => <h1>About / Metodología</h1>;

const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      { index: true, element: <DashboardPage /> },
      { path: "players", element: <PlayersPage /> },
      { path: "players/:id", element: <PlayerDetailPage /> }, // <-- Ruta conectada
      { path: "about", element: <AboutPage /> },
    ],
  },
]);

export const AppRouter = () => <RouterProvider router={router} />;