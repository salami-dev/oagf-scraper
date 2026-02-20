export type LogLevel = "debug" | "info" | "warn" | "error";

export interface LogFields {
  docId?: string;
  url?: string;
  pageUrl?: string;
  attempt?: number;
  [key: string]: unknown;
}

export type MetricCounterName =
  | "pages_crawled"
  | "docs_discovered"
  | "docs_enqueued"
  | "downloads_ok"
  | "downloads_failed"
  | "extracts_ok"
  | "extracts_failed";

export type MetricTimerName = "page_fetch_ms" | "download_ms" | "extract_ms";
