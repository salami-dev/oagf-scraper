const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

test("loadConfig supports async http queue settings", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-config-"));
  const configPath = path.join(tempDir, "config.json");

  fs.writeFileSync(
    configPath,
    JSON.stringify({
      asyncQueueMode: "http",
      asyncQueueHttpBaseUrl: "http://127.0.0.1:8081",
      asyncQueueHttpTimeoutMs: 7000,
    }),
    "utf-8",
  );

  const { loadConfig } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "config", "loadConfig.js")));
  const config = loadConfig(configPath);

  assert.equal(config.asyncQueueMode, "http");
  assert.equal(config.asyncQueueHttpBaseUrl, "http://127.0.0.1:8081");
  assert.equal(config.asyncQueueHttpTimeoutMs, 7000);
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
