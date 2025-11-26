// frontend/src/lib/league-assets.ts

// Mapa de logos usando imágenes locales en public/img/leagues/
export const LEAGUE_LOGOS: Record<string, string> = {
    "Liga Profesional": "/img/leagues/arg.png",
    "Premier League": "/img/leagues/eng.png",
    "La Liga": "/img/leagues/esp.png",
    "Serie A": "/img/leagues/ita.png",
    "Bundesliga": "/img/leagues/ger.png",
    "Ligue 1": "/img/leagues/fra.png",
    "Liga Portugal": "/img/leagues/por.png",
    "Eredivisie": "/img/leagues/ned.png",
    "Brasileirao": "/img/leagues/bra.png",
    "Major League Soccer": "/img/leagues/usa.png",
    // Agrega otros si tienes los archivos (ej: "Pro League": "/img/leagues/bel.png")
};

// Función fallback para ligas sin logo
export const getLeagueLogo = (name: string) => {
    // Busca coincidencia exacta o parcial
    if (LEAGUE_LOGOS[name]) return LEAGUE_LOGOS[name];
    
    const key = Object.keys(LEAGUE_LOGOS).find(k => name.includes(k));
    return key ? LEAGUE_LOGOS[key] : null;
};