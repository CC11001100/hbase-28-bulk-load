import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  base: '/hbase-28-bulk-load/',
  server: {
    port: 54328,
  },
})
