import { z } from 'zod';

export const toTitleCase = (str: string | null | undefined) => {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|\s)\w/g, (match) => match.toUpperCase());
};

// --- Función de Normalización ---
const normalizeBackendData = (data: unknown) => {
  if (typeof data !== 'object' || data === null) return data;
  
  const d = data as Record<string, unknown>;

  // CORRECCIÓN AQUÍ: Agregamos 'd.market_value_show' a la lista de prioridad.
  // Esto captura el "18000000" aunque venga en el campo de texto.
  const rawValue = d.actual_value_eur 
    ?? d.MarketValueEUR 
    ?? d.market_value_eur 
    ?? d.value 
    ?? d.market_value_show; // <--- CLAVE: Buscar aquí también

  // Intentamos convertir lo que encontremos a número
  let numericValue = rawValue ? Number(rawValue) : null;

  // Si la conversión falla (ej: viene texto "Sin cotización"), lo dejamos nulo
  if (Number.isNaN(numericValue)) {
    numericValue = null;
  }

  return {
    ...d,
    player_id: d.player_id ?? d.player_uuid,
    player_name: toTitleCase(String(d.player_name ?? d.Player ?? '')),
    
    Pos: d.Pos ?? d.pos ?? d.primary_position,
    Age: d.Age ?? d.age,
    Squad: toTitleCase(String(d.Squad ?? d.squad ?? d.club ?? d.Club ?? d.team ?? '')), 
    Nation: d.Nation ?? d.nation,
    
    // Asignamos el valor numérico limpio
    MarketValueEUR: numericValue,
    
    IsGK: d.IsGK ?? d.isgk ?? d.is_gk ?? false,
    
    MatchesPlayed: d.MatchesPlayed ?? d.matchesplayed ?? d.matches_played ?? d.mp,
    Gls: d.Gls ?? d.gls ?? d.goals,
    Ast: d.Ast ?? d.ast ?? d.assists,
    xG: d.xG ?? d.xg,
    xAG: d.xAG ?? d.xag,
    Shots: d.Shots ?? d.shots,
    SoT: d.SoT ?? d.sot,
    PassCmp: d.PassCmp ?? d.passcmp,
    PassAtt: d.PassAtt ?? d.passatt,
    PassCmpPct: d.PassCmpPct ?? d.passcmppct ?? d.pass_cmp_pct,
    Tkl: d.Tkl ?? d.tkl,
    TklW: d.TklW ?? d.tklw,
    Blocks: d.Blocks ?? d.blocks,
    Int: d.Int ?? d.int ?? d.interceptions,

    GK_Saves: d.GK_Saves ?? d.gk_saves,
    GK_GA: d.GK_GA ?? d.gk_ga,
    GK_SavePct: d.GK_SavePct ?? d.gk_savepct,
    GK_CS: d.GK_CS ?? d.gk_cs,
    GK_PSxG: d.GK_PSxG ?? d.gk_psxg,
    
    season_code: d.season_code ?? d.Season ?? 'Actual',
  };
};

// --- Schema Real ---
export const PlayerDetailSchema = z.preprocess(
  normalizeBackendData,
  z.object({
    player_id: z.union([z.string(), z.number()]).transform((val) => String(val)),
    player_name: z.string(),
    
    Age: z.union([z.number(), z.string()]).optional(),
    Pos: z.string().optional().default("N/A"),
    Squad: z.string().optional().nullable(),
    Nation: z.string().optional().nullable(),
    market_value_show: z.union([z.string(), z.number()]).optional().nullable(),
    
    // Zod recibirá el número limpio gracias a normalizeBackendData
    MarketValueEUR: z.number().nullable().optional(),
    
    IsGK: z.boolean().optional().default(false),

    MatchesPlayed: z.coerce.number().optional(),
    Gls: z.coerce.number().optional(),
    Ast: z.coerce.number().optional(),
    xG: z.coerce.number().optional(),
    xAG: z.coerce.number().optional(),
    Shots: z.coerce.number().optional(),
    SoT: z.coerce.number().optional(),
    PassCmp: z.coerce.number().optional(),
    PassAtt: z.coerce.number().optional(),
    PassCmpPct: z.coerce.number().optional(),
    Tkl: z.coerce.number().optional(),
    TklW: z.coerce.number().optional(),
    Blocks: z.coerce.number().optional(),
    Int: z.coerce.number().optional(),

    GK_GA: z.coerce.number().optional(),
    GK_Saves: z.coerce.number().optional(),
    GK_SavePct: z.coerce.number().optional(),
    GK_CS: z.coerce.number().optional(),
    GK_PSxG: z.coerce.number().optional(),
    
    season_code: z.string().optional(),
  }).passthrough()
);

export type PlayerDetail = z.infer<typeof PlayerDetailSchema>;

export const PlayerSchema = z.object({
    player_uuid: z.string(),
    full_name: z.string(),
    primary_position: z.string(),
});
export const PagedPlayersSchema = z.array(PlayerSchema);