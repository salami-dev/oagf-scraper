const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}

test("pdf table extractor writes normalized table json", async () => {
  const { PdfParseTableExtractor } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "extract", "pdfParseTableExtractor.js")));
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "osgf-table-"));
  const rawPath = path.join(tempDir, "doc.pdf");
  fs.writeFileSync(rawPath, "fake", "utf-8");

  const extractor = new PdfParseTableExtractor(
    {
      baseUrl: "https://oagf.gov.ng/publications/faac-report/",
      userAgent: "test",
      ignoreHttpsErrors: false,
      requestTimeoutMs: 1000,
      downloadTimeoutMs: 1000,
      crawlConcurrency: 1,
      downloadConcurrency: 1,
      extractConcurrency: 1,
      maxPages: 1,
      maxDownloadAttempts: 1,
      maxExtractAttempts: 1,
      outputDirs: { raw: tempDir, extracted: tempDir, manifests: tempDir },
      storePath: path.join(tempDir, "state.sqlite"),
    },
    {
      parserFactory: () => ({
        getTable: async () => ({
          total: 1,
          pages: [{ num: 1, tables: [[["A", "B"], ["1", "2"]]] }],
        }),
        destroy: async () => {},
      }),
      readFile: async () => Buffer.from("pdf"),
    },
  );

  const result = await extractor.extractTables("doc-1", rawPath);
  assert.ok(result.location);
  const payload = JSON.parse(fs.readFileSync(result.location, "utf-8"));
  assert.equal(payload.status, "ok");
  assert.equal(payload.tableCount, 1);
  assert.equal(payload.pages[0].tables[0].rows[0][0], "A");
});
