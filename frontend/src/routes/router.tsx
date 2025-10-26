// frontend/src/routes/Router.tsx
import { createBrowserRouter, RouterProvider } from "react-router-dom";
import { Layout } from "./layout";
import { PlayersPage } from "../pages/Players"; // <-- Importamos la página

// Páginas simples (aún no creadas)
const DashboardPage = () => <h1>Dashboard (Home)</h1>;
const PlayerDetailPage = () => <h1>Player Detail</h1>;
const AboutPage = () => <h1>About / Metodología</h1>;

const router = createBrowserRouter([
  {
    path: "/",
    element: <Layout />,
    children: [
      { index: true, element: <DashboardPage /> },
      // Conectamos la ruta /players a la página real:
      { path: "players", element: <PlayersPage /> }, 
      { path: "players/:id", element: <PlayerDetailPage /> },
      { path: "about", element: <AboutPage /> },
    ],
  },
]);

export const AppRouter = () => <RouterProvider router={router} />;