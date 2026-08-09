import { defineConfig } from "oxlint";
import { recommended } from "oxlint-plugin-effect/presets/recommended";

export default defineConfig({
  categories: {
    correctness: "error",
    suspicious: "error",
    pedantic: "error",
    perf: "error",
  },
  plugins: ["typescript", "import", "node"],
  jsPlugins: ["oxlint-plugin-effect/plugin"],
  rules: recommended,
  ignorePatterns: ["**/dist/**", "**/node_modules/**", "**/*.d.ts", "**/bin/**", "**/scripts/**"],
});
