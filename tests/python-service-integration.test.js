const test = require("node:test");
const assert = require("node:assert/strict");
const { spawn, spawnSync } = require("node:child_process");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForHealth(baseUrl, timeoutMs) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    try {
      const response = await fetch(`${baseUrl}/health`);
      if (response.ok) {
        const body = await response.json();
        if (body.status === "ok") {
          return;
        }
      }
    } catch {
      // service is not ready yet
    }
    await sleep(200);
  }
  throw new Error("python table service did not become healthy in time");
}

test("python table service processes request and returns categorized file_not_found error", async (t) => {
  const probe = spawnSync("python", ["-c", "import fastapi,uvicorn,pdfplumber"], { encoding: "utf-8" });
  if (probe.status !== 0) {
    t.skip("python dependencies for table service are not installed");
    return;
  }

  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-pyservice-"));
  const queueDbPath = path.join(tempDir, "queue.sqlite");
  const extractedDir = path.join(tempDir, "extracted");
  const port = String(18080 + Math.floor(Math.random() * 300));
  const baseUrl = `http://127.0.0.1:${port}`;

  const child = spawn(
    "python",
    ["-m", "uvicorn", "app:app", "--app-dir", "services/table-service", "--host", "127.0.0.1", "--port", port],
    {
      cwd: process.cwd(),
      env: {
        ...process.env,
        ASYNC_QUEUE_PATH: queueDbPath,
        OUTPUT_EXTRACTED_DIR: extractedDir,
        TABLE_SERVICE_ENABLE_WORKER: "1",
        TABLE_WORKER_POLL_INTERVAL_MS: "100",
      },
      stdio: ["ignore", "pipe", "pipe"],
    },
  );

  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += String(chunk);
  });

  try {
    await waitForHealth(baseUrl, 20_000);

    const requestBody = {
      messages: [
        {
          version: "v1",
          type: "tables.extract.request",
          jobId: "job-integ-1",
          runId: "run-integ-1",
          docId: "doc-integ-1",
          rawPdfPath: path.join(tempDir, "missing.pdf"),
          attempt: 1,
          submittedAt: "2026-02-20T00:00:00.000Z",
        },
      ],
    };

    const publishResponse = await fetch(`${baseUrl}/v1/queue/requests`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(requestBody),
    });
    assert.equal(publishResponse.status, 200);

    let resultMessage;
    for (let i = 0; i < 50; i += 1) {
      const leaseResponse = await fetch(`${baseUrl}/v1/queue/results/lease`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ limit: 10 }),
      });
      assert.equal(leaseResponse.status, 200);
      const leaseBody = await leaseResponse.json();
      const messages = leaseBody.messages ?? [];
      const match = messages.find((m) => m.payload?.jobId === "job-integ-1");
      if (match) {
        resultMessage = match;
        break;
      }
      await sleep(200);
    }

    assert.ok(resultMessage, "expected a result message for job-integ-1");
    assert.equal(resultMessage.payload.status, "failed");
    assert.equal(resultMessage.payload.errorCode, "file_not_found");
  } finally {
    if (child.exitCode === null) {
      child.kill();
      await new Promise((resolve) => child.once("exit", resolve));
    }
    if (stderr.includes("Traceback")) {
      throw new Error(`python service stderr:\n${stderr}`);
    }
  }
});
