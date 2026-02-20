const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

test("loadConfig uses defaults", async () => {
  const { loadConfig } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "config", "loadConfig.js")));
  const config = loadConfig();

  assert.equal(config.baseUrl, "https://oagf.gov.ng/publications/faac-report/");
  assert.equal(config.outputDirs.raw, "data/raw");
  assert.equal(config.outputDirs.extracted, "data/extracted");
  assert.equal(config.outputDirs.manifests, "data/manifests");
});

test("loadConfig merges config file overrides", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-config-"));
  const configPath = path.join(tempDir, "config.json");

  fs.writeFileSync(
    configPath,
    JSON.stringify({
      maxPages: 50,
      outputDirs: {
        raw: "tmp/raw",
      },
    }),
    "utf-8",
  );

  const { loadConfig } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "config", "loadConfig.js")));
  const config = loadConfig(configPath);

  assert.equal(config.maxPages, 50);
  assert.equal(config.outputDirs.raw, "tmp/raw");
  assert.equal(config.outputDirs.extracted, "data/extracted");
  assert.equal(config.outputDirs.manifests, "data/manifests");
});

test("loadConfig accepts conditional_get change detection mode", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-config-"));
  const configPath = path.join(tempDir, "config.json");

  fs.writeFileSync(
    configPath,
    JSON.stringify({
      changeDetectionMode: "conditional_get",
    }),
    "utf-8",
  );

  const { loadConfig } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "config", "loadConfig.js")));
  const config = loadConfig(configPath);

  assert.equal(config.changeDetectionMode, "conditional_get");
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
