// frontend/src/mocks/browser.ts
import { setupWorker } from 'msw/browser';
import { handlers } from './handlers';

export const worker = setupWorker(...handlers);

export async function enableMocking() {
  if (import.meta.env.MODE === 'production') {
    return;
  }
  
  console.log("Iniciando Mock Service Worker (MSW)...");
  await worker.start({ onUnhandledRequest: 'bypass' });
}