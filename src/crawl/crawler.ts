import crypto from "node:crypto";
import { AppConfig } from "../config";
import { getFetchDispatcher } from "../core/fetch";
import { Logger, MetricsRegistry } from "../observability";
import { Sink } from "../sink";
import { PipelineStore } from "../store";
import { DocumentDiscoveredItem } from "../types";
import { extractNextPageUrl, extractPdfLinksFromHtml } from "./htmlParser";

interface CrawlDependencies {
  config: AppConfig;
  logger: Logger;
  metrics: MetricsRegistry;
  store: PipelineStore;
  sink: Sink;
}

interface CrawlOptions {
  dryRun: boolean;
  maxDocs?: number;
}

function createDocId(url: string): string {
  return crypto.createHash("sha256").update(url).digest("hex").slice(0, 24);
}

function buildFallbackPageUrl(baseUrl: string, pageNumber: number): string {
  const trimmed = baseUrl.endsWith("/") ? baseUrl.slice(0, -1) : baseUrl;
  return `${trimmed}/page/${pageNumber}/`;
}

async function fetchHtml(url: string, config: AppConfig): Promise<string> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.requestTimeoutMs);

  try {
    const response = await fetch(url, {
      method: "GET",
      headers: {
        "user-agent": config.userAgent,
        accept: "text/html,application/xhtml+xml",
      },
      dispatcher: getFetchDispatcher(config.ignoreHttpsErrors),
      signal: controller.signal,
    } as any);

    if (!response.ok) {
      throw new Error(`HTTP ${response.status} while fetching ${url}`);
    }

    return await response.text();
  } finally {
    clearTimeout(timeout);
  }
}

async function extractPdfLinksWithPlaywrightFallback(pageUrl: string): Promise<string[]> {
  try {
    const dynamicImport = new Function("moduleName", "return import(moduleName)");
    const playwright = (await dynamicImport("playwright")) as any;
    const browser = await playwright.chromium.launch({ headless: true });
    const page = await browser.newPage();
    const foundUrls = new Set<string>();

    page.on("response", (response: any) => {
      const headers = response.headers();
      if (headers["content-type"]?.includes("application/pdf")) {
        foundUrls.add(response.url());
      }
    });

    await page.goto(pageUrl, { waitUntil: "networkidle" });
    const hrefs = await page.$$eval("a[href]", (anchors: Array<{ href: string }>) =>
      anchors.map((anchor) => anchor.href).filter((href) => href.toLowerCase().includes(".pdf")),
    );
    hrefs.forEach((href: string) => foundUrls.add(href));
    await browser.close();

    return [...foundUrls];
  } catch {
    return [];
  }
}

function buildDiscoveredItem(url: string, title: string, sourcePageUrl: string, year?: number, month?: string): DocumentDiscoveredItem {
  return {
    docId: createDocId(url),
    url,
    title,
    year,
    month,
    sourcePageUrl,
    discoveredAt: new Date().toISOString(),
  };
}

export async function crawlFaacPages(deps: CrawlDependencies, options: CrawlOptions): Promise<number> {
  const { config, logger, metrics, store, sink } = deps;
  const seenDocUrls = new Set<string>();
  let pageUrl: string | undefined = config.baseUrl;
  let pageIndex = 1;
  let discoveredCount = 0;

  while (pageUrl && pageIndex <= config.maxPages) {
    const remainingBeforePage = options.maxDocs !== undefined ? Math.max(options.maxDocs - discoveredCount, 0) : undefined;
    if (remainingBeforePage === 0) {
      break;
    }

    logger.info("crawl_page_start", { pageUrl, attempt: pageIndex });
    const stopTimer = metrics.startTimer("page_fetch_ms");

    let html: string;
    try {
      html = await fetchHtml(pageUrl, config);
    } catch (error) {
      logger.error("crawl_page_fetch_failed", {
        pageUrl,
        attempt: pageIndex,
        error: error instanceof Error ? error.message : String(error),
      });
      break;
    }

    metrics.incrementCounter("pages_crawled", 1);
    const parsedLinks = extractPdfLinksFromHtml(html, pageUrl);
    let discoveredItems = parsedLinks
      .filter((link) => !seenDocUrls.has(link.url))
      .map((link) => buildDiscoveredItem(link.url, link.title, pageUrl as string, link.year, link.month));

    for (const item of discoveredItems) {
      seenDocUrls.add(item.url);
    }

    if (discoveredItems.length === 0 && /iframe|embed|object/i.test(html)) {
      logger.warn("crawl_html_no_pdf_links_trying_playwright_fallback", { pageUrl });
      const fallbackLinks = await extractPdfLinksWithPlaywrightFallback(pageUrl);
      discoveredItems = fallbackLinks
        .filter((url) => !seenDocUrls.has(url))
        .map((url) => buildDiscoveredItem(url, url.split("/").pop() ?? "Rendered PDF", pageUrl as string));

      for (const item of discoveredItems) {
        seenDocUrls.add(item.url);
      }
    }

    if (discoveredItems.length > 0) {
      const remaining = options.maxDocs !== undefined ? Math.max(options.maxDocs - discoveredCount, 0) : discoveredItems.length;
      if (options.maxDocs !== undefined && discoveredItems.length > remaining) {
        discoveredItems = discoveredItems.slice(0, remaining);
      }

      metrics.incrementCounter("docs_discovered", discoveredItems.length);
      metrics.incrementCounter("docs_enqueued", discoveredItems.length);
      discoveredCount += discoveredItems.length;

      if (options.dryRun) {
        for (const item of discoveredItems) {
          logger.info("crawl_discovered_dry_run", { docId: item.docId, url: item.url, pageUrl });
        }
      } else {
        for (const item of discoveredItems) {
          await store.upsertDiscovered(item);
        }
        await sink.publishDiscovered(discoveredItems);
      }
    }

    const durationMs = stopTimer();
    logger.info("crawl_page_complete", {
      pageUrl,
      discoveredOnPage: discoveredItems.length,
      durationMs,
    });

    const explicitNext = extractNextPageUrl(html, pageUrl);
    pageIndex += 1;
    pageUrl = explicitNext ?? (pageIndex <= config.maxPages ? buildFallbackPageUrl(config.baseUrl, pageIndex) : undefined);
  }

  if (pageIndex > config.maxPages) {
    logger.warn("crawl_max_pages_reached", { maxPages: config.maxPages });
  }

  logger.info("crawl_finished", { discoveredCount, pagesVisited: pageIndex - 1 });
  return discoveredCount;
}
