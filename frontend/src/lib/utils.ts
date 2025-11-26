// frontend/src/lib/utils.ts
import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Formatea un número a formato de moneda con símbolo Euro fijo.
 * Ej: 18000000 -> "€ 18.000.000"
 */
export function formatMarketValue(value: number | undefined | null): string {
  // 1. Manejo de casos vacíos
  if (!value || value === 0) return 'Sin Cotización';
  
  // 2. Formateamos SOLO el número usando 'es-AR' para obtener los puntos de mil (18.000.000)
  // Nota: No usamos style: 'currency' aquí para evitar que el navegador decida poner "EUR".
  const numberPart = new Intl.NumberFormat('es-AR', {
    maximumFractionDigits: 0, // Sin decimales
  }).format(value);

  // 3. Agregamos el símbolo manualmente donde más nos guste
  return `€ ${numberPart}`;
}