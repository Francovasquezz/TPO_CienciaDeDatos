// frontend/src/lib/schemas.ts
import { z } from 'zod';

// Función para capitalizar texto (Ej: "river plate" -> "River Plate")
export const toTitleCase = (str: string | null | undefined) => {
  if (!str) return '';
  return str.toLowerCase().replace(/(?:^|\s)\w/g, (match) => match.toUpperCase());
};

// --- Función de Normalización ---
const normalizeBackendData = (data: unknown) => {
  if (typeof data !== 'object' || data === null) return data;
  
  const d = data as Record<string, unknown>;

  return {
    ...d,
    // Identificadores
    player_id: d.player_id ?? d.player_uuid,
    player_name: toTitleCase(String(d.player_name ?? d.Player ?? '')),
    
    // Mapeo de campos básicos
    Pos: d.Pos ?? d.pos ?? d.primary_position,
    Age: d.Age ?? d.age,
    Squad: toTitleCase(String(d.Squad ?? d.squad ?? d.club ?? d.Club ?? d.team ?? '')), 
    Nation: d.Nation ?? d.nation,
    
    // ⬅️ CORRECCIÓN: Aquí capturamos 'actual_value_eur'
    MarketValueEUR: d.actual_value_eur ?? d.MarketValueEUR ?? d.market_value_eur ?? d.value,
    
    // Lógica de Arquero
    IsGK: d.IsGK ?? d.isgk ?? d.is_gk ?? false,
    
    // Stats de Campo
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

    // Stats de Arquero
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
    
    // Valor de Mercado (puede ser nulo)
    MarketValueEUR: z.number().nullable().optional(),
    
    IsGK: z.boolean().optional().default(false),

    // Stats (Todas opcionales para evitar errores si faltan)
    MatchesPlayed: z.number().optional(),
    Gls: z.number().optional(),
    Ast: z.number().optional(),
    xG: z.number().optional(),
    xAG: z.number().optional(),
    Shots: z.number().optional(),
    SoT: z.number().optional(),
    PassCmp: z.number().optional(),
    PassAtt: z.number().optional(),
    PassCmpPct: z.number().optional(),
    Tkl: z.number().optional(),
    TklW: z.number().optional(),
    Blocks: z.number().optional(),
    Int: z.number().optional(),

    GK_GA: z.number().optional(),
    GK_Saves: z.number().optional(),
    GK_SavePct: z.number().optional(),
    GK_CS: z.number().optional(),
    GK_PSxG: z.number().optional(),
    
    season_code: z.string().optional(),
  }).passthrough()
);

export type PlayerDetail = z.infer<typeof PlayerDetailSchema>;

// Schemas auxiliares
export const PlayerSchema = z.object({
    player_uuid: z.string(),
    full_name: z.string(),
    primary_position: z.string(),
});
export const PagedPlayersSchema = z.array(PlayerSchema);