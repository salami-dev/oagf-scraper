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

class FakeSqsClient {
  constructor(responses = []) {
    this.responses = [...responses];
    this.calls = [];
  }

  async send(command) {
    this.calls.push(command);
    if (this.responses.length === 0) {
      return {};
    }
    const next = this.responses.shift();
    if (next instanceof Error) {
      throw next;
    }
    return next;
  }
}

test("sqs sink batches discovered messages in chunks of 10", async () => {
  const { SqsSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "sqsSink.js")));
  const client = new FakeSqsClient([{ Successful: [] }, { Successful: [] }]);
  const sink = new SqsSink({
    queueUrl: "https://sqs.us-east-1.amazonaws.com/1111/queue",
    client,
  });

  const docs = Array.from({ length: 12 }, (_, i) => makeDiscovered(`doc-${i + 1}`));
  await sink.publishDiscovered(docs);

  assert.equal(client.calls.length, 2);
  assert.equal(client.calls[0].input.Entries.length, 10);
  assert.equal(client.calls[1].input.Entries.length, 2);
});

test("sqs sink sets fifo fields when configured", async () => {
  const { SqsSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "sqsSink.js")));
  const client = new FakeSqsClient([{ Successful: [] }]);
  const sink = new SqsSink({
    queueUrl: "https://sqs.us-east-1.amazonaws.com/1111/queue.fifo",
    client,
    fifo: true,
    groupId: "faac-docs",
  });

  await sink.publishDiscovered([makeDiscovered("doc-1")]);
  const entry = client.calls[0].input.Entries[0];
  assert.equal(entry.MessageGroupId, "faac-docs");
  assert.equal(entry.MessageDeduplicationId, "doc-1:discovered");
});

test("sqs sink retries only failed entries from a batch response", async () => {
  const { SqsSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "sqsSink.js")));
  const client = new FakeSqsClient([
    {
      Successful: [{ Id: "0", MessageId: "ok" }],
      Failed: [{ Id: "1", Message: "temporary", Code: "500", SenderFault: false }],
    },
    {
      Successful: [{ Id: "1", MessageId: "retried-ok" }],
      Failed: [],
    },
  ]);

  const sink = new SqsSink({
    queueUrl: "https://sqs.us-east-1.amazonaws.com/1111/queue",
    client,
    maxRetries: 2,
    retryDelayMs: 1,
  });

  await sink.publishDiscovered([makeDiscovered("doc-1"), makeDiscovered("doc-2")]);

  assert.equal(client.calls.length, 2);
  assert.equal(client.calls[0].input.Entries.length, 2);
  assert.equal(client.calls[1].input.Entries.length, 1);
  assert.equal(client.calls[1].input.Entries[0].Id, "1");
});
