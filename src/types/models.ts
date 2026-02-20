export interface DocumentDiscoveredItem {
  docId: string;
  url: string;
  title: string;
  year?: number;
  month?: string;
  sourcePageUrl: string;
  discoveredAt: string;
}

export interface DownloadResult {
  docId: string;
  url: string;
  resolvedUrl?: string;
  status: "downloaded_ok" | "download_failed" | "skipped";
  sha256?: string;
  bytes?: number;
  contentType?: string;
  rawLocation?: string;
  etag?: string;
  lastModified?: string;
  error?: string;
  attempt: number;
  downloadedAt: string;
}

export interface ExtractionResult {
  docId: string;
  status: "extracted_ok" | "extracted_failed" | "skipped";
  pageCount?: number;
  textLocation?: string;
  tablesLocation?: string;
  error?: string;
  attempt: number;
  extractedAt: string;
}
