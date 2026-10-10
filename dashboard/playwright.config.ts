import {defineConfig} from "@playwright/test";
export default defineConfig({
  testDir: "./tests",
  workers: 1,
  use: {baseURL: "http://127.0.0.1:8766", headless: true},
  webServer: {
    command: '..\\.venv\\Scripts\\python.exe ..\\scripts\\browser_fixture.py',
    url: "http://127.0.0.1:8766/.well-known/hivemind",
    reuseExistingServer: false,
    timeout: 30000,
  },
});
