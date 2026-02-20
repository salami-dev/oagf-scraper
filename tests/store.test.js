const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

test("sqlite store upsert and pending download idempotency", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-store-"));
  const dbPath = path.join(tempDir, "state.sqlite");
  const store = new SqliteStore(dbPath);

  await store.upsertDiscovered({
    docId: "doc-1",
    url: "https://example.com/a.pdf",
    title: "A",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });

  let pending = await store.listPendingDownloads(10);
  assert.equal(pending.length, 1);
  assert.equal(pending[0].docId, "doc-1");
  assert.equal(pending[0].attempt, 1);

  await store.markDownloadResult({
    docId: "doc-1",
    url: "https://example.com/a.pdf",
    status: "downloaded_ok",
    sha256: "abc123",
    bytes: 1234,
    contentType: "application/pdf",
    rawLocation: "data/raw/doc-1.pdf",
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  pending = await store.listPendingDownloads(10);
  assert.equal(pending.length, 0);
  await store.close();
});

test("sqlite store pending extracts obey extraction idempotency", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-store-"));
  const dbPath = path.join(tempDir, "state.sqlite");
  const store = new SqliteStore(dbPath);

  await store.upsertDiscovered({
    docId: "doc-2",
    url: "https://example.com/b.pdf",
    title: "B",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });

  await store.markDownloadResult({
    docId: "doc-2",
    url: "https://example.com/b.pdf",
    status: "downloaded_ok",
    sha256: "def456",
    bytes: 2222,
    contentType: "application/pdf",
    rawLocation: "data/raw/doc-2.pdf",
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  let pendingExtracts = await store.listPendingExtracts(10);
  assert.equal(pendingExtracts.length, 1);
  assert.equal(pendingExtracts[0].docId, "doc-2");

  await store.markExtractionResult({
    docId: "doc-2",
    status: "extracted_ok",
    pageCount: 10,
    textLocation: "data/extracted/doc-2.txt",
    attempt: 1,
    extractedAt: "2026-02-20T02:00:00.000Z",
  });

  pendingExtracts = await store.listPendingExtracts(10);
  assert.equal(pendingExtracts.length, 0);
  await store.close();
});

test("sqlite store force listing bypasses attempt caps", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-store-"));
  const dbPath = path.join(tempDir, "state.sqlite");
  const store = new SqliteStore(dbPath);

  await store.upsertDiscovered({
    docId: "doc-force",
    url: "https://example.com/force.pdf",
    title: "Force",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc-force",
    url: "https://example.com/force.pdf",
    status: "downloaded_ok",
    rawLocation: "data/raw/doc-force.pdf",
    attempt: 5,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });
  await store.markExtractionResult({
    docId: "doc-force",
    status: "extracted_ok",
    textLocation: "data/extracted/doc-force.txt",
    attempt: 5,
    extractedAt: "2026-02-20T02:00:00.000Z",
  });

  const forceDownloads = await store.listPendingDownloads(10, true, 3);
  const forceExtracts = await store.listPendingExtracts(10, true, 3);

  assert.equal(forceDownloads.some((item) => item.docId === "doc-force"), true);
  assert.equal(forceExtracts.some((item) => item.docId === "doc-force"), true);
  await store.close();
});

test("sqlite store revalidation and missing-file reconciliation", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-store-"));
  const dbPath = path.join(tempDir, "state.sqlite");
  const store = new SqliteStore(dbPath);

  await store.upsertDiscovered({
    docId: "doc-revalidate",
    url: "https://example.com/revalidate.pdf",
    title: "Revalidate",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc-revalidate",
    url: "https://example.com/revalidate.pdf",
    status: "downloaded_ok",
    rawLocation: path.join(tempDir, "raw", "doc-revalidate.pdf"),
    sha256: "abc",
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
    etag: "\"v1\"",
    lastModified: "Wed, 01 Jan 2025 00:00:00 GMT",
  });

  const candidates = await store.listRevalidationCandidates(10, "2026-02-21T00:00:00.000Z");
  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].docId, "doc-revalidate");

  await store.markRemoteUnchanged("doc-revalidate", "2026-02-21T00:00:00.000Z", "\"v1\"", "Wed, 01 Jan 2025 00:00:00 GMT");
  const unchangedCandidates = await store.listRevalidationCandidates(10, "2026-02-21T00:00:00.000Z");
  assert.equal(unchangedCandidates.length, 0);

  await store.markRemoteChanged("doc-revalidate", "2026-02-22T00:00:00.000Z", "\"v2\"", "Wed, 01 Feb 2025 00:00:00 GMT");
  const pendingAfterChange = await store.listPendingDownloads(10, false, 3);
  assert.equal(pendingAfterChange.some((item) => item.docId === "doc-revalidate"), true);

  const missingRaw = path.join(tempDir, "missing.pdf");
  await store.upsertDiscovered({
    docId: "doc-missing-raw",
    url: "https://example.com/missing.pdf",
    title: "MissingRaw",
    sourcePageUrl: "https://example.com",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  });
  await store.markDownloadResult({
    docId: "doc-missing-raw",
    url: "https://example.com/missing.pdf",
    status: "downloaded_ok",
    rawLocation: missingRaw,
    sha256: "def",
    attempt: 1,
    downloadedAt: "2026-02-20T01:00:00.000Z",
  });

  await store.markRawMissing("doc-missing-raw", "2026-02-23T00:00:00.000Z");
  const pendingAfterMissing = await store.listPendingDownloads(10, false, 3);
  assert.equal(pendingAfterMissing.some((item) => item.docId === "doc-missing-raw"), true);

  await store.close();
});

test("sqlite store extract job lifecycle supports enqueue claim complete", async () => {
  const { SqliteStore } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "store", "sqliteStore.js")));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-store-"));
  const dbPath = path.join(tempDir, "state.sqlite");
  const store = new SqliteStore(dbPath);

  await store.enqueueExtractJob({
    jobId: "job-1",
    runId: "run-1",
    docId: "doc-1",
    rawPdfPath: "data/raw/doc-1.pdf",
    fileSha256: "abc",
    attempt: 1,
    submittedAt: "2026-02-20T00:00:00.000Z",
  });

  const claimed = await store.claimPendingExtractJobs(10, "2099-02-20T01:00:00.000Z");
  assert.equal(claimed.length, 1);
  assert.equal(claimed[0].jobId, "job-1");

  const claimedAgain = await store.claimPendingExtractJobs(10, "2099-02-20T01:00:00.000Z");
  assert.equal(claimedAgain.length, 0);

  await store.completeExtractJob({
    jobId: "job-1",
    status: "completed",
    finishedAt: "2026-02-20T02:00:00.000Z",
    resultRef: "data/extracted/doc-1.tables.json",
  });

  const claimedAfterComplete = await store.claimPendingExtractJobs(10, "2026-02-20T03:00:00.000Z");
  assert.equal(claimedAfterComplete.length, 0);
  await store.close();
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
