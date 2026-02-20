const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

test("extractor marks invalid pdf as extracted_failed after retries", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { runExtractor } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "extractor.js")));
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));
  const { MetricsRegistry } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "metrics.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-extract-"));
  const rawDir = path.join(tempDir, "raw");
  const extractedDir = path.join(tempDir, "extracted");
  fs.mkdirSync(rawDir, { recursive: true });
  fs.mkdirSync(extractedDir, { recursive: true });

  const rawPath = path.join(rawDir, "doc.pdf");
  fs.writeFileSync(rawPath, "not-a-real-pdf", "utf-8");

  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  await store.upsertDiscovered({
    docId: "doc",
    url: "https://example.com/doc.pdf",
    title: "Doc",
    sourcePageUrl: "https://example.com/list",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc",
    url: "https://example.com/doc.pdf",
    status: "downloaded_ok",
    rawLocation: rawPath,
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  const published = [];
  try {
    await runExtractor({
      config: {
        baseUrl: "https://oagf.gov.ng/publications/faac-report/",
        userAgent: "test",
        requestTimeoutMs: 1_000,
        downloadTimeoutMs: 1_000,
        crawlConcurrency: 1,
        downloadConcurrency: 1,
        extractConcurrency: 1,
        maxPages: 1,
        maxDownloadAttempts: 1,
        maxExtractAttempts: 2,
        outputDirs: { raw: rawDir, extracted: extractedDir, manifests: path.join(tempDir, "manifests") },
        storePath: path.join(tempDir, "state.sqlite"),
      },
      logger: new Logger({ component: "test", runId: "run_extract" }),
      metrics: new MetricsRegistry(),
      store,
      sink: {
        publishDiscovered: async () => {},
        publishDownloadResult: async () => {},
        publishExtractionResult: async (results) => {
          published.push(...results);
        },
      },
      force: false,
    });
  } finally {
    await store.close();
  }

  assert.equal(published.length, 1);
  assert.equal(published[0].status, "extracted_failed");
});

test("extractor falls back when table extractor throws", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { runExtractor } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "extractor.js")));
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));
  const { MetricsRegistry } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "metrics.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-extract-fallback-"));
  const rawDir = path.join(tempDir, "raw");
  const extractedDir = path.join(tempDir, "extracted");
  fs.mkdirSync(rawDir, { recursive: true });
  fs.mkdirSync(extractedDir, { recursive: true });

  const rawPath = path.join(rawDir, "doc.pdf");
  fs.writeFileSync(rawPath, "fake", "utf-8");

  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  await store.upsertDiscovered({
    docId: "doc",
    url: "https://example.com/doc.pdf",
    title: "Doc",
    sourcePageUrl: "https://example.com/list",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc",
    url: "https://example.com/doc.pdf",
    status: "downloaded_ok",
    rawLocation: rawPath,
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  const published = [];
  try {
    await runExtractor({
      config: {
        baseUrl: "https://oagf.gov.ng/publications/faac-report/",
        userAgent: "test",
        ignoreHttpsErrors: false,
        requestTimeoutMs: 1_000,
        downloadTimeoutMs: 1_000,
        crawlConcurrency: 1,
        downloadConcurrency: 1,
        extractConcurrency: 1,
        maxPages: 1,
        maxDownloadAttempts: 1,
        maxExtractAttempts: 1,
        outputDirs: { raw: rawDir, extracted: extractedDir, manifests: path.join(tempDir, "manifests") },
        storePath: path.join(tempDir, "state.sqlite"),
      },
      logger: new Logger({ component: "test", runId: "run_extract" }),
      metrics: new MetricsRegistry(),
      store,
      sink: {
        publishDiscovered: async () => {},
        publishDownloadResult: async () => {},
        publishExtractionResult: async (results) => {
          published.push(...results);
        },
      },
      force: false,
      tableExtractor: {
        extractTables: async () => {
          throw new Error("table parse failed");
        },
      },
      textExtractor: async () => ({
        text: "hello",
        pageCount: 1,
      }),
    });
  } finally {
    await store.close();
  }

  assert.equal(published.length, 1);
  assert.equal(published[0].status, "extracted_ok");
  assert.match(published[0].tablesLocation, /\.tables\.json$/);
  const fallbackPayload = JSON.parse(fs.readFileSync(published[0].tablesLocation, "utf-8"));
  assert.equal(fallbackPayload.status, "not_supported");
});

test("extractor force mode processes each doc once per run", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { runExtractor } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "extractor.js")));
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));
  const { MetricsRegistry } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "metrics.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-extract-force-once-"));
  const rawDir = path.join(tempDir, "raw");
  const extractedDir = path.join(tempDir, "extracted");
  fs.mkdirSync(rawDir, { recursive: true });
  fs.mkdirSync(extractedDir, { recursive: true });
  const rawPath = path.join(rawDir, "doc.pdf");
  fs.writeFileSync(rawPath, "fake", "utf-8");

  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  await store.upsertDiscovered({
    docId: "doc",
    url: "https://example.com/doc.pdf",
    title: "Doc",
    sourcePageUrl: "https://example.com/list",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc",
    url: "https://example.com/doc.pdf",
    status: "downloaded_ok",
    rawLocation: rawPath,
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  try {
    const summary = await runExtractor({
      config: {
        baseUrl: "https://oagf.gov.ng/publications/faac-report/",
        userAgent: "test",
        ignoreHttpsErrors: false,
        requestTimeoutMs: 1_000,
        downloadTimeoutMs: 1_000,
        crawlConcurrency: 1,
        downloadConcurrency: 1,
        extractConcurrency: 1,
        maxPages: 1,
        maxDownloadAttempts: 1,
        maxExtractAttempts: 3,
        outputDirs: { raw: rawDir, extracted: extractedDir, manifests: path.join(tempDir, "manifests") },
        storePath: path.join(tempDir, "state.sqlite"),
      },
      logger: new Logger({ component: "test", runId: "run_extract_force_once" }),
      metrics: new MetricsRegistry(),
      store,
      sink: {
        publishDiscovered: async () => {},
        publishDownloadResult: async () => {},
        publishExtractionResult: async () => {},
      },
      force: true,
      tableExtractor: {
        extractTables: async () => ({ location: path.join(extractedDir, "doc.tables.json") }),
      },
      textExtractor: async () => ({
        text: "hello",
        pageCount: 1,
      }),
    });

    assert.equal(summary.processed, 1);
    assert.equal(summary.ok, 1);
  } finally {
    await store.close();
  }
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
