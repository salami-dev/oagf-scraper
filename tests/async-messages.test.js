const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}

test("extract request message validator accepts valid v1 envelope", async () => {
  const { isExtractRequestMessageV1 } = await import(
    pathToFileUrl(path.join(process.cwd(), "dist", "extract", "asyncMessages.js"))
  );

  const valid = {
    version: "v1",
    type: "tables.extract.request",
    jobId: "job-1",
    runId: "run-1",
    docId: "doc-1",
    rawPdfPath: "data/raw/doc-1.pdf",
    attempt: 1,
    submittedAt: "2026-02-20T00:00:00.000Z",
  };

  assert.equal(isExtractRequestMessageV1(valid), true);
  assert.equal(isExtractRequestMessageV1({ ...valid, type: "wrong" }), false);
});

test("extract result message validator accepts valid v1 envelope", async () => {
  const { isExtractResultMessageV1 } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "asyncMessages.js")));

  const valid = {
    version: "v1",
    type: "tables.extract.result",
    jobId: "job-1",
    docId: "doc-1",
    status: "ok",
    tableCount: 2,
    finishedAt: "2026-02-20T00:00:00.000Z",
  };

  assert.equal(isExtractResultMessageV1(valid), true);
  assert.equal(isExtractResultMessageV1({ ...valid, status: "unknown" }), false);
});
