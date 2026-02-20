const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

test("local jsonl sink appends records with runId", async () => {
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-sink-"));
  const manifestsDir = path.join(tempDir, "manifests");

  const { LocalJsonlSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "localJsonlSink.js")));
  const sink = new LocalJsonlSink(
    {
      baseUrl: "https://oagf.gov.ng/publications/faac-report/",
      userAgent: "test",
      requestTimeoutMs: 1000,
      downloadTimeoutMs: 1000,
      crawlConcurrency: 1,
      downloadConcurrency: 1,
      extractConcurrency: 1,
      maxPages: 1,
      maxDownloadAttempts: 1,
      maxExtractAttempts: 1,
      outputDirs: {
        raw: path.join(tempDir, "raw"),
        extracted: path.join(tempDir, "extracted"),
        manifests: manifestsDir,
      },
      storePath: path.join(tempDir, "state.sqlite"),
    },
    "run_test_123",
  );

  await sink.publishDiscovered([
    {
      docId: "doc-1",
      url: "https://example.com/file.pdf",
      title: "File",
      sourcePageUrl: "https://example.com/list",
      discoveredAt: "2026-02-20T00:00:00.000Z",
    },
  ]);

  const file = path.join(manifestsDir, "discovered.jsonl");
  const lines = fs.readFileSync(file, "utf-8").trim().split("\n");
  assert.equal(lines.length, 1);
  const record = JSON.parse(lines[0]);
  assert.equal(record.runId, "run_test_123");
  assert.equal(record.docId, "doc-1");
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
