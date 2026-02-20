import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pipeline } from "node:stream/promises";
import { Readable } from "node:stream";
import { AppConfig } from "../config";
import { getFetchDispatcher } from "../core/fetch";
import { Logger, MetricsRegistry } from "../observability";
import { Sink } from "../sink";
import { PipelineStore } from "../store";
import { DownloadResult } from "../types";

interface DownloaderDeps {
  config: AppConfig;
  logger: Logger;
  metrics: MetricsRegistry;
  store: PipelineStore;
  sink: Sink;
  force: boolean;
  maxDocs?: number;
  fetchFn?: typeof fetch;
}

interface DownloadSummary {
  processed: number;
  ok: number;
  failed: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetriableStatus(status: number): boolean {
  return status === 429 || status >= 500;
}

async function processWithConcurrency<T>(
  items: T[],
  concurrency: number,
  worker: (item: T) => Promise<void>,
): Promise<void> {
  let index = 0;
  const slots = new Array(Math.max(1, concurrency)).fill(null).map(async () => {
    while (true) {
      const current = index;
      index += 1;
      if (current >= items.length) {
        break;
      }
      await worker(items[current]);
    }
  });
  await Promise.all(slots);
}

async function downloadAttempt(
  url: string,
  outputPath: string,
  config: AppConfig,
  fetchFn: typeof fetch,
): Promise<{
  statusCode: number;
  resolvedUrl?: string;
  contentType?: string;
  sha256?: string;
  bytes?: number;
  etag?: string;
  lastModified?: string;
}> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.downloadTimeoutMs);
  let response: Response;
  try {
    response = await fetchFn(url, {
      method: "GET",
      headers: {
        "user-agent": config.userAgent,
        accept: "application/pdf,*/*",
      },
      dispatcher: getFetchDispatcher(config.ignoreHttpsErrors),
      signal: controller.signal,
      redirect: "follow",
    } as any);
  } finally {
    clearTimeout(timeout);
  }

  if (!response.ok) {
    return { statusCode: response.status, resolvedUrl: response.url };
  }

  if (!response.body) {
    return { statusCode: 500, resolvedUrl: response.url };
  }

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  const tempPath = `${outputPath}.part`;
  const hash = crypto.createHash("sha256");
  let bytes = 0;

  const writable = fs.createWriteStream(tempPath, { flags: "w" });
  const readable = Readable.fromWeb(response.body as any);
  readable.on("data", (chunk) => {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    hash.update(buffer);
    bytes += buffer.length;
  });

  try {
    await pipeline(readable, writable);
    fs.renameSync(tempPath, outputPath);
  } catch (error) {
    if (fs.existsSync(tempPath)) {
      fs.unlinkSync(tempPath);
    }
    throw error;
  }

  return {
    statusCode: response.status,
    resolvedUrl: response.url,
    contentType: response.headers.get("content-type") ?? undefined,
    sha256: hash.digest("hex"),
    bytes,
    etag: response.headers.get("etag") ?? undefined,
    lastModified: response.headers.get("last-modified") ?? undefined,
  };
}

export async function runDownloader(deps: DownloaderDeps): Promise<DownloadSummary> {
  const { config, logger, metrics, store, sink, force } = deps;
  const fetchFn = deps.fetchFn ?? fetch;
  const batchSize = Math.max(config.downloadConcurrency * 2, 1);
  const processedDocIds = new Set<string>();
  let processed = 0;
  let ok = 0;
  let failed = 0;

  if (config.verifyDownloadedFilesOnStartup) {
    const rawFiles = await store.listDownloadedRawFiles(5000);
    for (const item of rawFiles) {
      if (!fs.existsSync(item.rawLocation)) {
        await store.markRawMissing(item.docId, new Date().toISOString());
        logger.warn("download_raw_missing_requeued", { docId: item.docId });
      }
    }
  }

  if (config.changeDetectionMode === "head") {
    const checkedBeforeIso = new Date(Date.now() - config.revalidateAfterDays * 24 * 60 * 60 * 1000).toISOString();
    const candidates = await store.listRevalidationCandidates(2000, checkedBeforeIso);
    for (const item of candidates) {
      try {
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), config.requestTimeoutMs);
        let response: Response;
        try {
          response = await fetchFn(item.url, {
            method: "HEAD",
            headers: { "user-agent": config.userAgent },
            dispatcher: getFetchDispatcher(config.ignoreHttpsErrors),
            signal: controller.signal,
            redirect: "follow",
          } as any);
        } finally {
          clearTimeout(timeout);
        }

        const remoteEtag = response.headers.get("etag") ?? undefined;
        const remoteLastModified = response.headers.get("last-modified") ?? undefined;
        const changed =
          (Boolean(item.etag) && Boolean(remoteEtag) && item.etag !== remoteEtag) ||
          (Boolean(item.lastModified) && Boolean(remoteLastModified) && item.lastModified !== remoteLastModified) ||
          response.status === 404;

        if (changed) {
          await store.markRemoteChanged(item.docId, new Date().toISOString(), remoteEtag, remoteLastModified);
          logger.info("download_revalidation_changed", { docId: item.docId, url: item.url });
        } else {
          await store.markRemoteUnchanged(item.docId, new Date().toISOString(), remoteEtag, remoteLastModified);
        }
      } catch (error) {
        logger.warn("download_revalidation_error", {
          docId: item.docId,
          url: item.url,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  while (true) {
    if (deps.maxDocs !== undefined && processed >= deps.maxDocs) {
      break;
    }

    const remaining = deps.maxDocs !== undefined ? Math.max(deps.maxDocs - processed, 0) : batchSize;
    const requestSize = Math.min(batchSize, remaining || batchSize);
    const workItems = (await store.listPendingDownloads(requestSize, force, config.maxDownloadAttempts)).filter(
      (item) => !processedDocIds.has(item.docId),
    );
    if (workItems.length === 0) {
      break;
    }

    await processWithConcurrency(workItems, config.downloadConcurrency, async (item) => {
      const finalPath = path.resolve(config.outputDirs.raw, `${item.docId}.pdf`);
      let result: DownloadResult | undefined;
      let attempt = item.attempt;

      for (; attempt <= config.maxDownloadAttempts; attempt += 1) {
        const stopTimer = metrics.startTimer("download_ms");
        logger.info("download_item_attempt_start", { docId: item.docId, url: item.url, attempt });

        try {
          const attemptResult = await downloadAttempt(item.url, finalPath, config, fetchFn);
          const durationMs = stopTimer();

          if (attemptResult.statusCode === 404) {
            result = {
              docId: item.docId,
              url: item.url,
              resolvedUrl: attemptResult.resolvedUrl,
              status: "download_failed",
              error: "HTTP 404",
              attempt,
              downloadedAt: new Date().toISOString(),
            };
            logger.warn("download_item_404", { docId: item.docId, url: item.url, attempt, durationMs });
            break;
          }

          if (attemptResult.statusCode >= 400) {
            const retriable = isRetriableStatus(attemptResult.statusCode);
            if (!retriable || attempt >= config.maxDownloadAttempts) {
              result = {
                docId: item.docId,
                url: item.url,
                resolvedUrl: attemptResult.resolvedUrl,
                status: "download_failed",
                error: `HTTP ${attemptResult.statusCode}`,
                attempt,
                downloadedAt: new Date().toISOString(),
              };
              logger.warn("download_item_failed_http", {
                docId: item.docId,
                url: item.url,
                attempt,
                durationMs,
                statusCode: attemptResult.statusCode,
              });
              break;
            }

            logger.warn("download_item_retry_http", {
              docId: item.docId,
              url: item.url,
              attempt,
              durationMs,
              statusCode: attemptResult.statusCode,
            });
            await sleep(Math.min(1000 * 2 ** (attempt - 1), 10_000));
            continue;
          }

          result = {
            docId: item.docId,
            url: item.url,
            resolvedUrl: attemptResult.resolvedUrl,
            status: "downloaded_ok",
            sha256: attemptResult.sha256,
            bytes: attemptResult.bytes,
            contentType: attemptResult.contentType,
            rawLocation: finalPath,
            etag: attemptResult.etag,
            lastModified: attemptResult.lastModified,
            attempt,
            downloadedAt: new Date().toISOString(),
          };
          logger.info("download_item_ok", { docId: item.docId, url: item.url, attempt, durationMs });
          break;
        } catch (error) {
          const durationMs = stopTimer();
          const message = error instanceof Error ? error.message : String(error);
          const isLastAttempt = attempt >= config.maxDownloadAttempts;
          logger.warn("download_item_error", {
            docId: item.docId,
            url: item.url,
            attempt,
            durationMs,
            error: message,
          });
          if (isLastAttempt) {
            result = {
              docId: item.docId,
              url: item.url,
              status: "download_failed",
              error: message,
              attempt,
              downloadedAt: new Date().toISOString(),
            };
            break;
          }
          await sleep(Math.min(1000 * 2 ** (attempt - 1), 10_000));
        }
      }

      if (!result) {
        result = {
          docId: item.docId,
          url: item.url,
          status: "download_failed",
          error: "Unknown download failure",
          attempt: config.maxDownloadAttempts,
          downloadedAt: new Date().toISOString(),
        };
      }

      await store.markDownloadResult(result);
      await sink.publishDownloadResult([result]);
      processedDocIds.add(item.docId);
      processed += 1;

      if (result.status === "downloaded_ok") {
        metrics.incrementCounter("downloads_ok", 1);
        ok += 1;
      } else {
        metrics.incrementCounter("downloads_failed", 1);
        failed += 1;
      }
    });
  }

  return { processed, ok, failed };
}
