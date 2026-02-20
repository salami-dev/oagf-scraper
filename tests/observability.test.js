const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

test("metrics registry tracks counters and timers", async () => {
  const { MetricsRegistry } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "observability", "metrics.js")));
  const metrics = new MetricsRegistry();

  metrics.incrementCounter("docs_discovered", 2);
  const stop = metrics.startTimer("page_fetch_ms");
  stop();

  const counters = metrics.getCounters();
  const timers = metrics.getTimerSummaries();

  assert.equal(counters.docs_discovered, 2);
  assert.equal(timers.page_fetch_ms.count, 1);
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
