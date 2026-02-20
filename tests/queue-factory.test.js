const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}

test("queue factory returns local sqlite adapter by default", async () => {
  const { createAsyncQueue } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "queueFactory.js")));

  const queue = createAsyncQueue({
    baseUrl: "https://oagf.gov.ng/publications/faac-report/",
    userAgent: "test-agent",
    ignoreHttpsErrors: false,
    pollIntervalMinutes: 30,
    verifyDownloadedFilesOnStartup: false,
    revalidateAfterDays: 30,
    changeDetectionMode: "head",
    requestTimeoutMs: 1000,
    downloadTimeoutMs: 1000,
    crawlConcurrency: 1,
    downloadConcurrency: 1,
    extractConcurrency: 1,
    maxPages: 10,
    maxDownloadAttempts: 1,
    maxExtractAttempts: 1,
    asyncQueueMode: "local_sqlite",
    asyncQueuePath: "data/test-async.sqlite",
    asyncQueueHttpBaseUrl: "http://127.0.0.1:8081",
    asyncQueueHttpToken: undefined,
    asyncQueueHttpTimeoutMs: 1000,
    extractJobLeaseMinutes: 10,
    outputDirs: { raw: "data/raw", extracted: "data/extracted", manifests: "data/manifests" },
    storePath: "data/state.sqlite",
  });

  assert.equal(queue.constructor.name, "LocalSqliteQueueAdapter");
  await queue.close();
});

test("queue factory returns http adapter when configured", async () => {
  const { createAsyncQueue } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "queueFactory.js")));

  const queue = createAsyncQueue({
    baseUrl: "https://oagf.gov.ng/publications/faac-report/",
    userAgent: "test-agent",
    ignoreHttpsErrors: true,
    pollIntervalMinutes: 30,
    verifyDownloadedFilesOnStartup: false,
    revalidateAfterDays: 30,
    changeDetectionMode: "head",
    requestTimeoutMs: 1000,
    downloadTimeoutMs: 1000,
    crawlConcurrency: 1,
    downloadConcurrency: 1,
    extractConcurrency: 1,
    maxPages: 10,
    maxDownloadAttempts: 1,
    maxExtractAttempts: 1,
    asyncQueueMode: "http",
    asyncQueuePath: "data/test-async.sqlite",
    asyncQueueHttpBaseUrl: "http://127.0.0.1:8081",
    asyncQueueHttpToken: "secret",
    asyncQueueHttpTimeoutMs: 1000,
    extractJobLeaseMinutes: 10,
    outputDirs: { raw: "data/raw", extracted: "data/extracted", manifests: "data/manifests" },
    storePath: "data/state.sqlite",
  });

  assert.equal(queue.constructor.name, "HttpAsyncQueueAdapter");
  await queue.close();
});
