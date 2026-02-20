const test = require("node:test");
const assert = require("node:assert/strict");
const { spawnSync } = require("node:child_process");
const path = require("node:path");

test("cli help renders expected commands", () => {
  const entry = path.join(process.cwd(), "dist", "index.js");
  const result = spawnSync(process.execPath, [entry, "--help"], { encoding: "utf-8" });

  assert.equal(result.status, 0);
  assert.match(result.stdout, /crawl --dry-run/);
  assert.match(result.stdout, /download/);
  assert.match(result.stdout, /extract/);
  assert.match(result.stdout, /run/);
  assert.match(result.stdout, /poll/);
  assert.match(result.stdout, /status/);
});

test("cli parser wires crawl dry-run and run max-docs", async () => {
  const { parseCliArgs } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "cli", "index.js")));

  const crawlParsed = parseCliArgs(["crawl", "--dry-run", "--config", "config.json"]);
  assert.equal(crawlParsed.command, "crawl");
  assert.equal(crawlParsed.dryRun, true);
  assert.equal(crawlParsed.configPath, "config.json");

  const runParsed = parseCliArgs(["run", "--max-docs", "25"]);
  assert.equal(runParsed.command, "run");
  assert.equal(runParsed.maxDocs, 25);

  const pollParsed = parseCliArgs(["poll", "--iterations", "3", "--interval-minutes", "10"]);
  assert.equal(pollParsed.command, "poll");
  assert.equal(pollParsed.iterations, 3);
  assert.equal(pollParsed.intervalMinutes, 10);
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
