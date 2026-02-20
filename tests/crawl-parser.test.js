const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

test("extractPdfLinksFromHtml finds PDF URLs and derives title metadata", async () => {
  const html = fs.readFileSync(path.join(process.cwd(), "tests", "fixtures", "faac-page-1.html"), "utf-8");
  const { extractPdfLinksFromHtml } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "crawl", "htmlParser.js")));

  const links = extractPdfLinksFromHtml(html, "https://oagf.gov.ng/publications/faac-report/");
  assert.equal(links.length, 2);
  assert.equal(links[0].url.includes(".pdf"), true);
  assert.equal(links[0].year, 2024);
  assert.equal(links[0].month, "April");
});

test("extractNextPageUrl returns next link when present", async () => {
  const html = fs.readFileSync(path.join(process.cwd(), "tests", "fixtures", "faac-page-1.html"), "utf-8");
  const { extractNextPageUrl } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "crawl", "htmlParser.js")));

  const next = extractNextPageUrl(html, "https://oagf.gov.ng/publications/faac-report/");
  assert.equal(next, "https://oagf.gov.ng/publications/faac-report/page/2/");
});

test("extractNextPageUrl returns undefined without pagination link", async () => {
  const html = fs.readFileSync(path.join(process.cwd(), "tests", "fixtures", "faac-page-no-next.html"), "utf-8");
  const { extractNextPageUrl } = await import(pathToFileUrl(path.join(process.cwd(), "dist", "crawl", "htmlParser.js")));

  const next = extractNextPageUrl(html, "https://oagf.gov.ng/publications/faac-report/");
  assert.equal(next, undefined);
});

function pathToFileUrl(filePath) {
  const normalized = filePath.replace(/\\/g, "/");
  return `file:///${normalized}`;
}
