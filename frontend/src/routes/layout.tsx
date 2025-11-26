// frontend/src/routes/layout.tsx
import { Outlet, useLocation, Link } from "react-router-dom"; // ⬅️ IMPORTANTE: Importar Link
import { Home, Settings, TrendingUp, Trophy } from "lucide-react";

const navItems = [
    { name: "Home", path: "/", icon: Home },
    { name: "Ligas y Clubes", path: "/leagues", icon: Trophy },
    { name: "Oportunidades", path: "/opportunities", icon: TrendingUp },
    { name: "About", path: "/about", icon: Settings },
];

const Navbar = () => {
    const location = useLocation(); 

    return (
      <header className="sticky top-0 z-40 w-full border-b border-white/10 bg-zinc-950/80 backdrop-blur-md shadow-sm">
        <div className="flex h-16 items-center px-4 md:px-8 justify-between">
          
          <div className="font-bold text-xl text-blue-500 flex-shrink-0 w-32 tracking-wider">
            FUTBOL<span className="text-white">AI</span>
          </div>
          
          <nav className="flex space-x-6 text-sm font-medium mx-auto"> 
            {navItems.map((item) => {
                const isActive = location.pathname === item.path;
                return (
                    // ⬇️ CAMBIO CRÍTICO: Usar Link y 'to' en lugar de 'a' y 'href'
                    <Link 
                        key={item.name} 
                        to={item.path} 
                        className={`hover:text-blue-400 transition-colors flex items-center gap-1 ${isActive ? 'text-blue-400 border-b-2 border-blue-400' : 'text-gray-400'}`}
                    >
                        <item.icon className="w-4 h-4" /> 
                        {item.name}
                    </Link>
                );
            })}
          </nav>
          
          <div className="w-32 flex-shrink-0 hidden md:block"></div> 
        </div>
      </header>
    );
};

export const Layout = () => {
  return (
    <div className="min-h-screen flex flex-col bg-black text-white">
      <Navbar /> 
      <main className="flex-grow p-4 md:p-8 container mx-auto">
        <Outlet />
      </main>
    </div>
  );
};