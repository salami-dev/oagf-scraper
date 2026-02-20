const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}

function makeDiscovered(docId) {
  return {
    docId,
    url: `https://example.com/${docId}.pdf`,
    title: `Title ${docId}`,
    sourcePageUrl: "https://example.com/list",
    discoveredAt: "2026-02-20T00:00:00.000Z",
  };
}

function response(status, body = "") {
  return {
    ok: status >= 200 && status < 300,
    status,
    text: async () => body,
  };
}

test("http sink posts discovered batch with auth and idempotency headers", async () => {
  const { HttpSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "httpSink.js")));
  const calls = [];
  const sink = new HttpSink({
    endpoint: "https://api.example.com/ingest",
    token: "secret",
    fetchFn: async (url, init) => {
      calls.push({ url, init });
      return response(200);
    },
  });

  await sink.publishDiscovered([makeDiscovered("doc-1"), makeDiscovered("doc-2")]);

  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, "https://api.example.com/ingest");
  assert.equal(calls[0].init.method, "POST");
  assert.equal(calls[0].init.headers.Authorization, "Bearer secret");
  assert.equal(calls[0].init.headers["Content-Type"], "application/json");
  assert.match(calls[0].init.headers["Idempotency-Key"], /discovered:doc-1,doc-2/);
});

test("http sink retries retriable statuses", async () => {
  const { HttpSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "httpSink.js")));
  let attempts = 0;
  const sink = new HttpSink({
    endpoint: "https://api.example.com/ingest",
    fetchFn: async () => {
      attempts += 1;
      if (attempts === 1) {
        return response(500, "temporary");
      }
      return response(200);
    },
    maxRetries: 2,
    retryDelayMs: 1,
  });

  await sink.publishDiscovered([makeDiscovered("doc-1")]);
  assert.equal(attempts, 2);
});

test("http sink fails fast on non-retriable 400", async () => {
  const { HttpSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "httpSink.js")));
  let attempts = 0;
  const sink = new HttpSink({
    endpoint: "https://api.example.com/ingest",
    fetchFn: async () => {
      attempts += 1;
      return response(400, "bad request");
    },
    maxRetries: 3,
    retryDelayMs: 1,
  });

  await assert.rejects(() => sink.publishDiscovered([makeDiscovered("doc-1")]), /HTTP sink permanent error 400/);
  assert.equal(attempts, 1);
});
