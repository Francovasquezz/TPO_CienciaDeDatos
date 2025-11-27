// frontend/src/lib/league-assets.ts

// Mapa de logos con los nombres EXACTOS que devuelve el backend (get-leagues)
export const LEAGUE_LOGOS: Record<string, string> = {
    // --- Nombres Exactos del Backend ---
    "Primera División Argentina": "/img/arg.png",
    "Premier League": "/img/eng.png",
    "LaLiga": "/img/esp.png",
    "Serie A": "/img/ita.png",
    "Bundesliga": "/img/ger.png",
    "Ligue 1": "/img/fra.png",
    "Eredivisie": "/img/ned.png",
    "Primeira Liga": "/img/por.png",
    "Brasileirão Serie A": "/img/bra.png", // ⬅️ Al estar exacto aquí, ya no usará el de Italia
    "Major League Soccer": "/img/usa.png",
    "Belgian Pro League": "/img/bel.png",
    "Saudi Pro League": "/img/sau.png",

    // --- Alias adicionales (por seguridad o variantes históricas) ---
    "Liga Profesional": "/img/arg.png",
    "La Liga": "/img/esp.png",
    "Liga Portugal": "/img/por.png",
    "Brasileirao": "/img/bra.png",
    "MLS": "/img/usa.png",
    "Jupiler Pro League": "/img/bel.png"
};

export const getLeagueLogo = (name: string) => {
    if (!name) return null;
    
    // 1. Prioridad TOTAL a la coincidencia exacta
    // Esto soluciona el problema de "Brasileirão Serie A" vs "Serie A"
    if (LEAGUE_LOGOS[name]) return LEAGUE_LOGOS[name];
    
    // 2. Búsqueda parcial (Fallback)
    // Solo se ejecuta si el nombre exacto no existe en el mapa
    const key = Object.keys(LEAGUE_LOGOS).find(k => name.includes(k));
    return key ? LEAGUE_LOGOS[key] : null;
};