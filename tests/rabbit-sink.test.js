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

class FakeChannel {
  constructor({ failPublishCount = 0 } = {}) {
    this.failPublishCount = failPublishCount;
    this.publishCalls = [];
  }

  async assertExchange() {}

  publish(exchange, routingKey, content, options) {
    this.publishCalls.push({ exchange, routingKey, content, options });
    if (this.failPublishCount > 0) {
      this.failPublishCount -= 1;
      throw new Error("publish failed");
    }
    return true;
  }

  async waitForConfirms() {}
  async close() {}
}

class FakeConnection {
  constructor(channel) {
    this.channel = channel;
  }

  async createConfirmChannel() {
    return this.channel;
  }

  async close() {}
}

test("rabbit sink publishes discovered items as persistent messages", async () => {
  const { RabbitSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "rabbitSink.js")));
  const channel = new FakeChannel();
  const sink = new RabbitSink({
    connectionUrl: "amqp://localhost",
    exchange: "docs.exchange",
    routingKey: "docs.discovered",
    connectFn: async () => new FakeConnection(channel),
  });

  await sink.publishDiscovered([makeDiscovered("doc-1")]);
  assert.equal(channel.publishCalls.length, 1);
  const call = channel.publishCalls[0];
  assert.equal(call.exchange, "docs.exchange");
  assert.equal(call.routingKey, "docs.discovered");
  assert.equal(call.options.persistent, true);
  assert.equal(call.options.contentType, "application/json");
  assert.equal(call.options.headers["x-idempotency-key"], "discovered:doc-1");
});

test("rabbit sink retries transient publish failures", async () => {
  const { RabbitSink } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "sink", "rabbitSink.js")));
  const channel = new FakeChannel({ failPublishCount: 1 });
  let connectCalls = 0;
  const sink = new RabbitSink({
    connectionUrl: "amqp://localhost",
    connectFn: async () => {
      connectCalls += 1;
      return new FakeConnection(channel);
    },
    maxRetries: 2,
    retryDelayMs: 1,
  });

  await sink.publishDiscovered([makeDiscovered("doc-1")]);
  assert.equal(channel.publishCalls.length, 2);
  assert.equal(connectCalls >= 2, true);
});
