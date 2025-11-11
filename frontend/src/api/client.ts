// frontend/src/api/client.ts
import axios from 'axios';

const axiosClient = axios.create({
  // Asume VITE_API_BASE_URL=http://localhost:8000
  baseURL: import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000',
});

export default axiosClient;