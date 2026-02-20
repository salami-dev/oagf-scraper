import fs from "node:fs";
import path from "node:path";
import { AppConfig, ConfigOverrides } from "./types";

const DEFAULT_CONFIG: AppConfig = {
  baseUrl: "https://oagf.gov.ng/publications/faac-report/",
  userAgent: "osgf-extractor/1.0 (+https://oagf.gov.ng)",
  ignoreHttpsErrors: false,
  pollIntervalMinutes: 30,
  verifyDownloadedFilesOnStartup: true,
  revalidateAfterDays: 30,
  changeDetectionMode: "head",
  requestTimeoutMs: 20_000,
  downloadTimeoutMs: 120_000,
  crawlConcurrency: 2,
  downloadConcurrency: 6,
  extractConcurrency: 4,
  maxPages: 200,
  maxDownloadAttempts: 3,
  maxExtractAttempts: 3,
  asyncQueueMode: "http",
  asyncQueuePath: "data/async-queue.sqlite",
  asyncQueueHttpBaseUrl: "http://127.0.0.1:8081",
  asyncQueueHttpToken: undefined,
  asyncQueueHttpTimeoutMs: 15_000,
  extractJobLeaseMinutes: 10,
  outputDirs: {
    raw: "data/raw",
    extracted: "data/extracted",
    manifests: "data/manifests",
  },
  storePath: "data/state.sqlite",
};

function readConfigFile(configPath?: string): ConfigOverrides {
  if (!configPath) {
    return {};
  }

  const absolutePath = path.resolve(configPath);
  if (!fs.existsSync(absolutePath)) {
    throw new Error(`Config file not found: ${absolutePath}`);
  }

  const raw = fs.readFileSync(absolutePath, "utf-8");
  const parsed = JSON.parse(raw) as ConfigOverrides;
  return parsed ?? {};
}

function toInt(value: string | undefined, fallback: number): number {
  if (!value) {
    return fallback;
  }

  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function toBool(value: string | undefined, fallback: boolean): boolean {
  if (!value) {
    return fallback;
  }
  const normalized = value.trim().toLowerCase();
  if (normalized === "1" || normalized === "true" || normalized === "yes") {
    return true;
  }
  if (normalized === "0" || normalized === "false" || normalized === "no") {
    return false;
  }
  return fallback;
}

export function loadConfig(configPath?: string): AppConfig {
  const fileConfig = readConfigFile(configPath);

  const merged: AppConfig = {
    ...DEFAULT_CONFIG,
    ...fileConfig,
    outputDirs: {
      ...DEFAULT_CONFIG.outputDirs,
      ...(fileConfig.outputDirs ?? {}),
    },
  };

  return {
    ...merged,
    baseUrl: process.env.BASE_URL ?? merged.baseUrl,
    userAgent: process.env.USER_AGENT ?? merged.userAgent,
    ignoreHttpsErrors: toBool(process.env.IGNORE_HTTPS_ERRORS, merged.ignoreHttpsErrors),
    pollIntervalMinutes: toInt(process.env.POLL_INTERVAL_MINUTES, merged.pollIntervalMinutes),
    verifyDownloadedFilesOnStartup: toBool(
      process.env.VERIFY_DOWNLOADED_FILES_ON_STARTUP,
      merged.verifyDownloadedFilesOnStartup,
    ),
    revalidateAfterDays: toInt(process.env.REVALIDATE_AFTER_DAYS, merged.revalidateAfterDays),
    changeDetectionMode:
      process.env.CHANGE_DETECTION_MODE === "none" ||
      process.env.CHANGE_DETECTION_MODE === "head" ||
      process.env.CHANGE_DETECTION_MODE === "conditional_get"
        ? process.env.CHANGE_DETECTION_MODE
        : merged.changeDetectionMode,
    requestTimeoutMs: toInt(process.env.REQUEST_TIMEOUT_MS, merged.requestTimeoutMs),
    downloadTimeoutMs: toInt(process.env.DOWNLOAD_TIMEOUT_MS, merged.downloadTimeoutMs),
    crawlConcurrency: toInt(process.env.CRAWL_CONCURRENCY, merged.crawlConcurrency),
    downloadConcurrency: toInt(process.env.DOWNLOAD_CONCURRENCY, merged.downloadConcurrency),
    extractConcurrency: toInt(process.env.EXTRACT_CONCURRENCY, merged.extractConcurrency),
    maxPages: toInt(process.env.MAX_PAGES, merged.maxPages),
    maxDownloadAttempts: toInt(process.env.MAX_DOWNLOAD_ATTEMPTS, merged.maxDownloadAttempts),
    maxExtractAttempts: toInt(process.env.MAX_EXTRACT_ATTEMPTS, merged.maxExtractAttempts),
    asyncQueueMode:
      process.env.ASYNC_QUEUE_MODE === "local_sqlite" || process.env.ASYNC_QUEUE_MODE === "http"
        ? process.env.ASYNC_QUEUE_MODE
        : merged.asyncQueueMode,
    asyncQueuePath: process.env.ASYNC_QUEUE_PATH ?? merged.asyncQueuePath,
    asyncQueueHttpBaseUrl: process.env.ASYNC_QUEUE_HTTP_BASE_URL ?? merged.asyncQueueHttpBaseUrl,
    asyncQueueHttpToken: process.env.ASYNC_QUEUE_HTTP_TOKEN ?? merged.asyncQueueHttpToken,
    asyncQueueHttpTimeoutMs: toInt(process.env.ASYNC_QUEUE_HTTP_TIMEOUT_MS, merged.asyncQueueHttpTimeoutMs),
    extractJobLeaseMinutes: toInt(process.env.EXTRACT_JOB_LEASE_MINUTES, merged.extractJobLeaseMinutes),
    storePath: process.env.STORE_PATH ?? merged.storePath,
    outputDirs: {
      raw: process.env.OUTPUT_RAW_DIR ?? merged.outputDirs.raw,
      extracted: process.env.OUTPUT_EXTRACTED_DIR ?? merged.outputDirs.extracted,
      manifests: process.env.OUTPUT_MANIFESTS_DIR ?? merged.outputDirs.manifests,
    },
  };
}

export { DEFAULT_CONFIG };
