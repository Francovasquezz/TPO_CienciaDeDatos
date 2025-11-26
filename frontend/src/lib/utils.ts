// frontend/src/lib/utils.ts
import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Opción alternativa: Número completo con puntos
export function formatMarketValue(value: number | undefined | null): string {
  if (!value || value === 0) return 'Sin Cotización';
  
  return new Intl.NumberFormat('es-AR', {
    style: 'currency',
    currency: 'EUR',
    maximumFractionDigits: 0 // Sin decimales
  }).format(value);
}

// Nota: Las librerías 'clsx' y 'tailwind-merge' deben estar instaladas. 
// Las instalamos previamente, pero si hay problemas, se pueden reinstalar.