// frontend/src/lib/schemas.ts
import { z } from 'zod';

// --- Schemas Base ---
export const TeamSchema = z.object({
  id: z.string(),
  name: z.string(),
  league: z.string(),
  logo_url: z.string().url().optional().nullable(),
});

export const PlayerSchema = z.object({
  id: z.string(),
  name: z.string(),
  position: z.enum(['GK', 'DF', 'MF', 'FW', 'NA']),
  age: z.number().int().positive().nullable(),
  nationality: z.string().nullable(),
  team_id: z.string(),
  photo_url: z.string().url().optional().nullable(),
  market_value_eur: z.number().int().nullable(),
  market_value_is_estimated: z.boolean(),
});

export const PagedPlayersSchema = z.object({
  items: z.array(PlayerSchema),
  page: z.number().int().positive(),
  page_size: z.number().int().positive(),
  total: z.number().int(),
});

// --- Schema de Detalle (Actualizado para PR2) ---
const CompetitionStatSchema = z.object({
  competition: z.string(),
  matches: z.number(),
  goals: z.number().optional(),
  assists: z.number().optional(),
  xG: z.number().optional().nullable(),
  xA: z.number().optional().nullable(),
  goals_received: z.number().optional(),
  clean_sheets: z.number().optional(),
  save_percentage: z.number().optional().nullable(),
  minutes: z.number(),
});

const RecentMatchSchema = z.object({
  date: z.string(),
  competition: z.string(),
  home_team: z.string(),
  home_score: z.number(),
  away_team: z.string(),
  away_score: z.number(),
  result: z.enum(['W', 'D', 'L']),
});

export const PlayerDetailSchema = PlayerSchema.extend({
  team_name: z.string(),
  team_logo_url: z.string().url().optional().nullable(),
  is_goalkeeper: z.boolean(),
  
  stats: z.object({
    minutes: z.number().nullable(),
    goals: z.number().nullable(),
    assists: z.number().nullable(),
    xG: z.number().nullable(),
    xA: z.number().nullable(),
    rating: z.number().nullable(),
  }),
  
  competition_stats: z.array(CompetitionStatSchema).optional(),
  recent_matches: z.array(RecentMatchSchema).optional(),
  
  feature_attribution: z.array(z.object({
    feature: z.string(),
    contribution: z.number(),
  })).optional(),
  
  last_updated: z.string(),
  sources: z.array(z.string()),
});

// --- Exportar Tipos ---
export type Team = z.infer<typeof TeamSchema>;
export type Player = z.infer<typeof PlayerSchema>;
export type PagedPlayers = z.infer<typeof PagedPlayersSchema>;
export type PlayerDetail = z.infer<typeof PlayerDetailSchema>;