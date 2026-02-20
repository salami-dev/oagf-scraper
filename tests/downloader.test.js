const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const http = require("node:http");

test("downloader handles success, retry, and 404 with atomic files", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { runDownloader } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "download", "downloader.js")));
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));
  const { MetricsRegistry } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "metrics.js")));

  let retryCount = 0;
  const sockets = new Set();
  const server = http.createServer((req, res) => {
    if (!req.url) {
      res.statusCode = 500;
      res.end("no url");
      return;
    }

    if (req.url === "/ok.pdf") {
      res.statusCode = 200;
      res.setHeader("content-type", "application/pdf");
      res.end(Buffer.from("%PDF-ok"));
      return;
    }

    if (req.url === "/retry.pdf") {
      retryCount += 1;
      if (retryCount === 1) {
        res.statusCode = 500;
        res.end("retry");
        return;
      }
      res.statusCode = 200;
      res.setHeader("content-type", "application/pdf");
      res.end(Buffer.from("%PDF-retry"));
      return;
    }

    if (req.url === "/missing.pdf") {
      res.statusCode = 404;
      res.end("missing");
      return;
    }

    res.statusCode = 500;
    res.end("unexpected");
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
  });

  await new Promise((resolve) => server.listen(0, resolve));
  const port = server.address().port;

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-downloader-"));
  const rawDir = path.join(tempDir, "raw");
  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  const metrics = new MetricsRegistry();
  const logger = new Logger({ component: "test", runId: "run_test" });
  const results = [];

  const now = "2026-02-20T00:00:00.000Z";
  await store.upsertDiscovered({
    docId: "ok",
    url: `http://127.0.0.1:${port}/ok.pdf`,
    title: "ok",
    sourcePageUrl: "x",
    discoveredAt: now,
  });
  await store.upsertDiscovered({
    docId: "retry",
    url: `http://127.0.0.1:${port}/retry.pdf`,
    title: "retry",
    sourcePageUrl: "x",
    discoveredAt: now,
  });
  await store.upsertDiscovered({
    docId: "missing",
    url: `http://127.0.0.1:${port}/missing.pdf`,
    title: "missing",
    sourcePageUrl: "x",
    discoveredAt: now,
  });

  try {
    await runDownloader({
      config: {
        baseUrl: "https://oagf.gov.ng/publications/faac-report/",
        userAgent: "test-agent",
        requestTimeoutMs: 5_000,
        downloadTimeoutMs: 5_000,
        crawlConcurrency: 1,
        downloadConcurrency: 2,
        extractConcurrency: 1,
        maxPages: 1,
        maxDownloadAttempts: 3,
        maxExtractAttempts: 1,
        outputDirs: { raw: rawDir, extracted: "", manifests: "" },
        storePath: path.join(tempDir, "state.sqlite"),
      },
      logger,
      metrics,
      store,
      sink: {
        publishDiscovered: async () => {},
        publishDownloadResult: async (batch) => {
          results.push(...batch);
        },
        publishExtractionResult: async () => {},
      },
      force: false,
    });

    assert.equal(fs.existsSync(path.join(rawDir, "ok.pdf")), true);
    assert.equal(fs.existsSync(path.join(rawDir, "retry.pdf")), true);
    assert.equal(fs.existsSync(path.join(rawDir, "missing.pdf")), false);
    assert.equal(fs.existsSync(path.join(rawDir, "ok.pdf.part")), false);
    assert.equal(fs.existsSync(path.join(rawDir, "retry.pdf.part")), false);
    assert.equal(results.length, 3);
    assert.equal(results.some((r) => r.docId === "missing" && r.status === "download_failed"), true);
    assert.equal(retryCount >= 2, true);

    const stats = await store.getStats();
    assert.equal(stats.downloadedOk, 2);
    assert.equal(stats.downloadedFailed, 1);
  } finally {
    await store.close();
    for (const socket of sockets) {
      socket.destroy();
    }
    await new Promise((resolve) => server.close(resolve));
  }
});

test("downloader reconciles missing local files and revalidates changed remote docs", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { runDownloader } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "download", "downloader.js")));
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));
  const { MetricsRegistry } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "metrics.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-downloader-reval-"));
  const rawDir = path.join(tempDir, "raw");
  fs.mkdirSync(rawDir, { recursive: true });
  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));

  const existingRaw = path.join(rawDir, "changed-doc.pdf");
  fs.writeFileSync(existingRaw, "old", "utf-8");
  const missingRaw = path.join(rawDir, "missing-doc.pdf");

  await store.upsertDiscovered({
    docId: "changed-doc",
    url: "https://example.com/changed.pdf",
    title: "changed",
    sourcePageUrl: "x",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "changed-doc",
    url: "https://example.com/changed.pdf",
    status: "downloaded_ok",
    rawLocation: existingRaw,
    sha256: "old-hash",
    etag: "\"v1\"",
    lastModified: "Wed, 01 Jan 2025 00:00:00 GMT",
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  await store.upsertDiscovered({
    docId: "missing-doc",
    url: "https://example.com/missing.pdf",
    title: "missing",
    sourcePageUrl: "x",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "missing-doc",
    url: "https://example.com/missing.pdf",
    status: "downloaded_ok",
    rawLocation: missingRaw,
    sha256: "old-hash",
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  const fetchCalls = [];
  const results = [];
  const fetchFn = async (url, init) => {
    const method = init?.method ?? "GET";
    fetchCalls.push({ url, method });

    if (method === "HEAD") {
      return new Response(null, {
        status: 200,
        headers: {
          etag: "\"v2\"",
          "last-modified": "Wed, 01 Feb 2025 00:00:00 GMT",
        },
      });
    }

    return new Response(Buffer.from("%PDF-updated"), {
      status: 200,
      headers: {
        "content-type": "application/pdf",
        etag: "\"v2\"",
        "last-modified": "Wed, 01 Feb 2025 00:00:00 GMT",
      },
    });
  };

  try {
    const summary = await runDownloader({
      config: {
        baseUrl: "https://oagf.gov.ng/publications/faac-report/",
        userAgent: "test-agent",
        ignoreHttpsErrors: false,
        pollIntervalMinutes: 30,
        verifyDownloadedFilesOnStartup: true,
        revalidateAfterDays: 0,
        changeDetectionMode: "head",
        requestTimeoutMs: 5_000,
        downloadTimeoutMs: 5_000,
        crawlConcurrency: 1,
        downloadConcurrency: 2,
        extractConcurrency: 1,
        maxPages: 1,
        maxDownloadAttempts: 3,
        maxExtractAttempts: 1,
        outputDirs: { raw: rawDir, extracted: "", manifests: "" },
        storePath: path.join(tempDir, "state.sqlite"),
      },
      logger: new Logger({ component: "test", runId: "run_test_reval" }),
      metrics: new MetricsRegistry(),
      store,
      sink: {
        publishDiscovered: async () => {},
        publishDownloadResult: async (batch) => {
          results.push(...batch);
        },
        publishExtractionResult: async () => {},
      },
      force: false,
      fetchFn,
    });

    assert.equal(summary.processed, 2);
    assert.equal(summary.ok, 2);
    assert.equal(fetchCalls.some((call) => call.method === "HEAD"), true);
    assert.equal(fs.existsSync(existingRaw), true);
    assert.equal(fs.existsSync(missingRaw), true);
    assert.equal(results.filter((r) => r.status === "downloaded_ok").length, 2);
  } finally {
    await store.close();
  }
});

test("downloader conditional_get sends validators and respects 304", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { runDownloader } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "download", "downloader.js")));
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));
  const { MetricsRegistry } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "metrics.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-downloader-condget-"));
  const rawDir = path.join(tempDir, "raw");
  fs.mkdirSync(rawDir, { recursive: true });
  const rawPath = path.join(rawDir, "doc.pdf");
  fs.writeFileSync(rawPath, "old", "utf-8");

  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  await store.upsertDiscovered({
    docId: "doc-304",
    url: "https://example.com/doc-304.pdf",
    title: "doc",
    sourcePageUrl: "x",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc-304",
    url: "https://example.com/doc-304.pdf",
    status: "downloaded_ok",
    rawLocation: rawPath,
    sha256: "hash",
    etag: "\"etag-v1\"",
    lastModified: "Wed, 01 Jan 2025 00:00:00 GMT",
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  const fetchCalls = [];
  const fetchFn = async (url, init) => {
    fetchCalls.push({ url, init });
    return new Response(null, { status: 304 });
  };

  try {
    const summary = await runDownloader({
      config: {
        baseUrl: "https://oagf.gov.ng/publications/faac-report/",
        userAgent: "test-agent",
        ignoreHttpsErrors: false,
        pollIntervalMinutes: 30,
        verifyDownloadedFilesOnStartup: false,
        revalidateAfterDays: 0,
        changeDetectionMode: "conditional_get",
        requestTimeoutMs: 5_000,
        downloadTimeoutMs: 5_000,
        crawlConcurrency: 1,
        downloadConcurrency: 1,
        extractConcurrency: 1,
        maxPages: 1,
        maxDownloadAttempts: 3,
        maxExtractAttempts: 1,
        outputDirs: { raw: rawDir, extracted: "", manifests: "" },
        storePath: path.join(tempDir, "state.sqlite"),
      },
      logger: new Logger({ component: "test", runId: "run_test_cond_get" }),
      metrics: new MetricsRegistry(),
      store,
      sink: {
        publishDiscovered: async () => {},
        publishDownloadResult: async () => {},
        publishExtractionResult: async () => {},
      },
      force: false,
      fetchFn,
    });

    assert.equal(summary.processed, 0);
    assert.equal(fetchCalls.length >= 1, true);
    assert.equal(fetchCalls[0].init.method, "GET");
    assert.equal(fetchCalls[0].init.headers["If-None-Match"], "\"etag-v1\"");
    assert.equal(fetchCalls[0].init.headers["If-Modified-Since"], "Wed, 01 Jan 2025 00:00:00 GMT");
  } finally {
    await store.close();
  }
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
