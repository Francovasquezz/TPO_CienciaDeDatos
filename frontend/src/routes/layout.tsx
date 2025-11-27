// frontend/src/routes/layout.tsx
import { Outlet, useLocation, Link } from "react-router-dom"; // Importamos Link
import { Home, Users, TrendingUp, Trophy } from "lucide-react"; // Quitamos Settings

// CAMBIO: Eliminamos "About" del array
const navItems = [
    { name: "Home", path: "/", icon: Home },
    { name: "Ligas y Clubes", path: "/leagues", icon: Trophy },
    { name: "Oportunidades", path: "/opportunities", icon: TrendingUp },
];

const Navbar = () => {
    const location = useLocation(); 

    return (
      <header className="sticky top-0 z-40 w-full border-b border-white/10 bg-zinc-950/80 backdrop-blur-md shadow-sm">
        <div className="flex h-16 items-center px-4 md:px-8 justify-between">
          
          {/* CAMBIO: Logo envuelto en Link para ir al Home */}
          <Link to="/" className="font-bold text-xl text-blue-500 flex-shrink-0 w-32 tracking-wider hover:opacity-80 transition-opacity cursor-pointer">
            FUTBOL<span className="text-white">AI</span>
          </Link>
          
          {/* Navegación */}
          <nav className="flex space-x-6 text-sm font-medium mx-auto"> 
            {navItems.map((item) => {
                const isActive = location.pathname === item.path;
                return (
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
          
          {/* Espaciador derecho para mantener simetría */}
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