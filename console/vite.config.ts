import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// One page, built to dist/. Phase 6 has the api role serve dist/ and adds the read API.
export default defineConfig({
  plugins: [react()],
  server: { host: "127.0.0.1" },
  preview: { host: "127.0.0.1" },
});
