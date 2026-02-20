const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}

test("local sqlite queue publishes, consumes and acks requests/results", async () => {
  const { LocalSqliteQueueAdapter } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "localSqliteQueueAdapter.js"))
  );

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-queue-"));
  const queueDbPath = path.join(tempDir, "queue.sqlite");
  const queue = new LocalSqliteQueueAdapter(queueDbPath);

  const request = {
    version: "v1",
    type: "tables.extract.request",
    jobId: "job-1",
    runId: "run-1",
    docId: "doc-1",
    rawPdfPath: "data/raw/doc-1.pdf",
    attempt: 1,
    submittedAt: "2026-02-20T00:00:00.000Z",
  };
  await queue.publishRequest(request);

  const requestBatch = await queue.consumeRequests(10);
  assert.equal(requestBatch.length, 1);
  assert.equal(requestBatch[0].message.jobId, "job-1");

  const requestBatchAgain = await queue.consumeRequests(10);
  assert.equal(requestBatchAgain.length, 0);

  await queue.ackRequest(requestBatch[0].queueMessageId);

  const result = {
    version: "v1",
    type: "tables.extract.result",
    jobId: "job-1",
    docId: "doc-1",
    status: "ok",
    tableCount: 3,
    finishedAt: "2026-02-20T00:01:00.000Z",
  };
  await queue.publishResult(result);

  const resultBatch = await queue.consumeResults(10);
  assert.equal(resultBatch.length, 1);
  assert.equal(resultBatch[0].message.status, "ok");

  await queue.ackResult(resultBatch[0].queueMessageId);
  const resultBatchAgain = await queue.consumeResults(10);
  assert.equal(resultBatchAgain.length, 0);

  await queue.close();
});
