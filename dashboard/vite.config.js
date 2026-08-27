import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/ch": {
        target: "http://localhost:8123",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/ch/, "/"),
        configure: (proxy) => {
          proxy.on("proxyReq", (proxyReq) => {
            proxyReq.setHeader("X-ClickHouse-User", "default");
            proxyReq.setHeader("X-ClickHouse-Key", "clickhouse");
          });
        },
      },
    },
  },
});
