const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}

function response(status, jsonBody) {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => JSON.stringify(jsonBody ?? {}),
    json: async () => jsonBody ?? {},
  };
}

test("http async queue publishes and consumes with bearer token", async () => {
  const { HttpAsyncQueueAdapter } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "httpAsyncQueueAdapter.js"))
  );

  const calls = [];
  const queue = new HttpAsyncQueueAdapter({
    baseUrl: "http://127.0.0.1:8081",
    token: "abc",
    fetchFn: async (url, init) => {
      calls.push({ url, init });

      if (url.endsWith("/v1/queue/requests/lease")) {
        return response(200, {
          messages: [
            {
              queueMessageId: "q1",
              payload: {
                version: "v1",
                type: "tables.extract.request",
                jobId: "job-1",
                runId: "run-1",
                docId: "doc-1",
                rawPdfPath: "data/raw/doc-1.pdf",
                attempt: 1,
                submittedAt: "2026-02-20T00:00:00.000Z",
              },
            },
          ],
        });
      }

      if (url.endsWith("/v1/queue/results/lease")) {
        return response(200, {
          messages: [
            {
              queueMessageId: "r1",
              payload: {
                version: "v1",
                type: "tables.extract.result",
                jobId: "job-1",
                docId: "doc-1",
                status: "ok",
                tableCount: 2,
                finishedAt: "2026-02-20T00:01:00.000Z",
              },
            },
          ],
        });
      }

      return response(200, { ok: true });
    },
  });

  await queue.publishRequest({
    version: "v1",
    type: "tables.extract.request",
    jobId: "job-1",
    runId: "run-1",
    docId: "doc-1",
    rawPdfPath: "data/raw/doc-1.pdf",
    attempt: 1,
    submittedAt: "2026-02-20T00:00:00.000Z",
  });

  const reqMessages = await queue.consumeRequests(5);
  assert.equal(reqMessages.length, 1);
  assert.equal(reqMessages[0].message.docId, "doc-1");

  await queue.ackRequest("q1");

  await queue.publishResult({
    version: "v1",
    type: "tables.extract.result",
    jobId: "job-1",
    docId: "doc-1",
    status: "ok",
    tableCount: 2,
    finishedAt: "2026-02-20T00:01:00.000Z",
  });

  const resultMessages = await queue.consumeResults(5);
  assert.equal(resultMessages.length, 1);
  assert.equal(resultMessages[0].message.status, "ok");

  await queue.ackResult("r1");
  await queue.close();

  assert.ok(calls.length >= 6);
  for (const call of calls) {
    assert.equal(call.init.headers.Authorization, "Bearer abc");
  }
});

test("http async queue throws on non-2xx", async () => {
  const { HttpAsyncQueueAdapter } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "httpAsyncQueueAdapter.js"))
  );

  const queue = new HttpAsyncQueueAdapter({
    baseUrl: "http://127.0.0.1:8081",
    fetchFn: async () => response(500, { error: "boom" }),
  });

  await assert.rejects(
    () =>
      queue.publishRequest({
        version: "v1",
        type: "tables.extract.request",
        jobId: "job-1",
        runId: "run-1",
        docId: "doc-1",
        rawPdfPath: "data/raw/doc-1.pdf",
        attempt: 1,
        submittedAt: "2026-02-20T00:00:00.000Z",
      }),
    /HTTP async queue error 500/,
  );
});
