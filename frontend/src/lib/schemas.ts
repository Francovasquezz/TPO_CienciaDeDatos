// frontend/src/lib/schemas.ts
import { z } from 'zod';

// Tipos base
export const TeamSchema = z.object({
  id: z.string(),
  name: z.string(),
  league: z.string(),
  logo_url: z.string().url().optional().nullable(),
});

// Tipo clave: Jugador
export const PlayerSchema = z.object({
  id: z.string(),
  name: z.string(),
  position: z.enum(['GK', 'DF', 'MF', 'FW', 'NA']),
  age: z.number().int().positive().nullable(),
  nationality: z.string().nullable(),
  team_id: z.string(),
  photo_url: z.string().url().optional().nullable(),
  market_value_eur: z.number().int().nullable(),
  market_value_is_estimated: z.boolean(), // ¡Crucial para el proyecto!
});

// Tipo para resultados paginados (GET /players)
export const PagedPlayersSchema = z.object({
  items: z.array(PlayerSchema),
  page: z.number().int().positive(),
  page_size: z.number().int().positive(),
  total: z.number().int(),
});

// Tipo para el detalle (GET /players/{id})
export const PlayerDetailSchema = PlayerSchema.extend({
  team_name: z.string(),
  stats: z.object({
    minutes: z.number().nullable(),
    goals: z.number().nullable(),
    assists: z.number().nullable(),
    xG: z.number().nullable(),
    xA: z.number().nullable(),
    rating: z.number().nullable(),
  }),
  feature_attribution: z.array(z.object({
    feature: z.string(),
    contribution: z.number(),
  })).optional(),
  last_updated: z.string(),
  sources: z.array(z.string()),
});


// Exportar tipos
export type Team = z.infer<typeof TeamSchema>;
export type Player = z.infer<typeof PlayerSchema>;
export type PagedPlayers = z.infer<typeof PagedPlayersSchema>;
export type PlayerDetail = z.infer<typeof PlayerDetailSchema>;