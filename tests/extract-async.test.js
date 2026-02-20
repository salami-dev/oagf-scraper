const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}

test("submitExtractJobs enqueues pending extracts into async queue", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { LocalSqliteQueueAdapter } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "localSqliteQueueAdapter.js"))
  );
  const { submitExtractJobs } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "asyncOrchestrator.js")));
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-extract-submit-"));
  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  const queue = new LocalSqliteQueueAdapter(path.join(tempDir, "queue.sqlite"));

  await store.upsertDiscovered({
    docId: "doc-1",
    url: "https://example.com/doc-1.pdf",
    title: "Doc",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc-1",
    url: "https://example.com/doc-1.pdf",
    status: "downloaded_ok",
    rawLocation: path.join(tempDir, "doc-1.pdf"),
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  const result = await submitExtractJobs({
    runId: "run-1",
    store,
    queue,
    logger: new Logger({ component: "test", runId: "run-1" }),
    limit: 10,
    maxExtractAttempts: 3,
  });
  assert.equal(result.submitted, 1);

  const queued = await queue.consumeRequests(10);
  assert.equal(queued.length, 1);
  assert.equal(queued[0].message.docId, "doc-1");

  await queue.close();
  await store.close();
});

test("collectExtractResults updates extraction status from async results", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { LocalSqliteQueueAdapter } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "localSqliteQueueAdapter.js"))
  );
  const { submitExtractJobs, collectExtractResults } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "asyncOrchestrator.js"))
  );
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-extract-collect-"));
  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  const queue = new LocalSqliteQueueAdapter(path.join(tempDir, "queue.sqlite"));

  await store.upsertDiscovered({
    docId: "doc-2",
    url: "https://example.com/doc-2.pdf",
    title: "Doc2",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc-2",
    url: "https://example.com/doc-2.pdf",
    status: "downloaded_ok",
    rawLocation: path.join(tempDir, "doc-2.pdf"),
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  await submitExtractJobs({
    runId: "run-2",
    store,
    queue,
    logger: new Logger({ component: "test", runId: "run-2" }),
    limit: 10,
    maxExtractAttempts: 3,
  });
  const requestBatch = await queue.consumeRequests(10);
  assert.equal(requestBatch.length, 1);

  await queue.publishResult({
    version: "v1",
    type: "tables.extract.result",
    jobId: requestBatch[0].message.jobId,
    docId: "doc-2",
    status: "ok",
    tablesLocation: path.join(tempDir, "doc-2.tables.json"),
    tableCount: 2,
    finishedAt: "2026-02-20T02:00:00.000Z",
  });

  const summary = await collectExtractResults({
    store,
    queue,
    logger: new Logger({ component: "test", runId: "run-2" }),
    limit: 10,
  });
  assert.equal(summary.processed, 1);
  assert.equal(summary.ok, 1);

  const stats = await store.getStats();
  assert.equal(stats.extractedOk, 1);

  await queue.close();
  await store.close();
});

test("collectExtractResults preserves error code in extraction failure", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const { LocalSqliteQueueAdapter } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "localSqliteQueueAdapter.js"))
  );
  const { submitExtractJobs, collectExtractResults } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "asyncOrchestrator.js"))
  );
  const { Logger } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "logger.js")));

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-extract-failed-"));
  const store = new SqliteStore(path.join(tempDir, "state.sqlite"));
  const queue = new LocalSqliteQueueAdapter(path.join(tempDir, "queue.sqlite"));

  await store.upsertDiscovered({
    docId: "doc-3",
    url: "https://example.com/doc-3.pdf",
    title: "Doc3",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc-3",
    url: "https://example.com/doc-3.pdf",
    status: "downloaded_ok",
    rawLocation: path.join(tempDir, "doc-3.pdf"),
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  await submitExtractJobs({
    runId: "run-3",
    store,
    queue,
    logger: new Logger({ component: "test", runId: "run-3" }),
    limit: 10,
    maxExtractAttempts: 3,
  });
  const requestBatch = await queue.consumeRequests(10);

  await queue.publishResult({
    version: "v1",
    type: "tables.extract.result",
    jobId: requestBatch[0].message.jobId,
    docId: "doc-3",
    status: "failed",
    errorCode: "file_not_found",
    error: "raw file missing",
    finishedAt: "2026-02-20T02:00:00.000Z",
  });

  const summary = await collectExtractResults({
    store,
    queue,
    logger: new Logger({ component: "test", runId: "run-3" }),
    limit: 10,
  });
  assert.equal(summary.failed, 1);

  const stats = await store.getStats();
  assert.equal(stats.extractedFailed, 1);

  await queue.close();
  await store.close();
});
