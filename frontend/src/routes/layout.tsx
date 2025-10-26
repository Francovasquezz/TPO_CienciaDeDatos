// frontend/src/routes/Layout.tsx
import { Outlet } from "react-router-dom";
import { LogOut, Home, Settings, Users } from "lucide-react"; // Añadimos Users

const Sidebar = () => (
  // CLASES CLAVE: w-64 fija, bg-white, h-screen fija
  <div className="flex flex-col w-64 border-r bg-white h-screen fixed"> 
    <div className="flex items-center h-20 p-4 border-b">
      <div className="bg-gray-700 text-white p-2 rounded-lg font-bold">
        TPO_Fútbol
      </div>
    </div>
    <nav className="flex-grow p-4 space-y-2">
      <a href="/" className="flex items-center p-3 rounded-lg text-gray-700 hover:bg-gray-100 transition-colors">
        <Home className="w-5 h-5 mr-3" />
        Home (Dashboard)
      </a>
      <a href="/players" className="flex items-center p-3 rounded-lg text-gray-700 hover:bg-gray-100 transition-colors">
        <Users className="w-5 h-5 mr-3" />
        Players
      </a>
      <a href="/about" className="flex items-center p-3 rounded-lg text-gray-700 hover:bg-gray-100 transition-colors">
        <Settings className="w-5 h-5 mr-3" />
        About / Metodología
      </a>
    </nav>
  </div>
);

export const Layout = () => {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      {/* Margen izquierdo para el contenido principal igual al ancho de la Sidebar */}
      <main className="flex-grow ml-64 p-8"> 
        <Outlet />
      </main>
    </div>
  );
};