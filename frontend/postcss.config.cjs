// frontend/postcss.config.cjs (¡Asegúrate de que el nombre del archivo sea .cjs!)
module.exports = {
  plugins: {
    // CAMBIAMOS 'tailwindcss' por su nombre de plugin correcto:
    '@tailwindcss/postcss': {}, 
    autoprefixer: {},
  },
}