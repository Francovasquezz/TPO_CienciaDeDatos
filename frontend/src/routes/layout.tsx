// frontend/src/routes/Layout.tsx (DISEÑO AJUSTADO FINAL Y ROBUSTO)
import { Outlet, useLocation } from "react-router-dom";
import { Home, Settings, Users, Search } from "lucide-react";
import { Input } from "../components/ui/input"; 

// Array de navegación
const navItems = [
    { name: "Home", path: "/", icon: Home },
    { name: "Players", path: "/players", icon: Users },
    { name: "About", path: "/about", icon: Settings },
];

// Componente Navbar (Header estándar)
const Navbar = () => {
    const location = useLocation(); 

    return (
      <header className="sticky top-0 z-40 w-full border-b bg-white shadow-sm">
        {/* Contenedor principal: justify-between empuja a los extremos */}
        <div className="flex h-16 items-center px-4 md:px-8 justify-between">
          
          {/* 1. Logo (Izquierda) */}
          <div className="font-bold text-xl text-blue-600 flex-shrink-0 w-32">
            Logo
          </div>
          
          {/* 2. Botones de Navegación (Centro) */}
          {/* mx-auto centra este bloque en el espacio restante */}
          <nav className="flex space-x-6 text-sm font-medium mx-auto"> 
            {navItems.map((item) => {
                const isActive = location.pathname === item.path;
                return (
                    <a 
                        key={item.name} 
                        href={item.path} 
                        className={`hover:text-blue-600 transition-colors flex items-center gap-1 ${isActive ? 'text-blue-600 border-b-2 border-blue-600' : 'text-gray-600'}`}
                    >
                        <item.icon className="w-4 h-4" /> 
                        {item.name}
                    </a>
                );
            })}
          </nav>
          
          {/* 3. Búsqueda Estática (Derecha) */}
          <div className="relative max-w-64 flex-shrink-0"> {/* ANCHO MÁS PEQUEÑO: max-w-64 */}
            <Search className="absolute top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              type="search"
              placeholder="Buscar jugadores..."
              className="w-full rounded-full bg-gray-100 pl-10 pr-4 text-sm"
              disabled 
            />
          </div>
        </div>
      </header>
    );
};

export const Layout = () => {
  return (
    <div className="min-h-screen flex flex-col">
      <Navbar /> 
      <main className="flex-grow p-4 md:p-8 container mx-auto">
        <Outlet />
      </main>
    </div>
  );
};