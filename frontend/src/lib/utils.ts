// frontend/src/lib/utils.ts
import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

// Nota: Las librerías 'clsx' y 'tailwind-merge' deben estar instaladas. 
// Las instalamos previamente, pero si hay problemas, se pueden reinstalar.