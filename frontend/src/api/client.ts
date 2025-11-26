// frontend/src/api/client.ts
import axios from 'axios';

const axiosClient = axios.create({
  // CORRECCIÓN: Cambiamos BASE_URL por URL para coincidir con Vercel
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8000',
});

export default axiosClient;