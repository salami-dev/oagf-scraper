import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";

export interface StoreStats {
  totalDocuments: number;
  discovered: number;
  downloadedOk: number;
  downloadedFailed: number;
  extractedOk: number;
  extractedFailed: number;
}

export interface DownloadWorkItem {
  docId: string;
  url: string;
  title: string;
  attempt: number;
}

export interface RevalidationItem {
  docId: string;
  url: string;
  etag?: string;
  lastModified?: string;
  lastCheckedAt?: string;
}

export interface RawFileItem {
  docId: string;
  rawLocation: string;
}

export interface ExtractJobRecord {
  jobId: string;
  runId: string;
  docId: string;
  rawPdfPath: string;
  fileSha256?: string;
  attempt: number;
  submittedAt: string;
}

export interface ClaimedExtractJob {
  jobId: string;
  runId: string;
  docId: string;
  rawPdfPath: string;
  fileSha256?: string;
  attempt: number;
  submittedAt: string;
  leaseUntil: string;
}

export interface ExtractJobStatusRecord {
  jobId: string;
  runId: string;
  docId: string;
  rawPdfPath: string;
  fileSha256?: string;
  attempt: number;
  submittedAt: string;
  status: "queued" | "leased" | "completed" | "failed";
  leaseUntil?: string;
  finishedAt?: string;
  error?: string;
  resultRef?: string;
}

export interface ExtractWorkItem {
  docId: string;
  rawLocation: string;
  attempt: number;
}

export interface PipelineStore {
  startRun(runId: string, startedAt: string): Promise<void>;
  finishRun(runId: string, status: "completed" | "failed", finishedAt: string): Promise<void>;
  upsertDiscovered(item: DocumentDiscoveredItem): Promise<void>;
  listDownloadedRawFiles(limit: number): Promise<RawFileItem[]>;
  listRevalidationCandidates(limit: number, checkedBeforeIso: string): Promise<RevalidationItem[]>;
  markRemoteUnchanged(docId: string, checkedAt: string, etag?: string, lastModified?: string): Promise<void>;
  markRemoteChanged(docId: string, checkedAt: string, etag?: string, lastModified?: string): Promise<void>;
  markRawMissing(docId: string, checkedAt: string): Promise<void>;
  enqueueExtractJob(job: ExtractJobRecord): Promise<void>;
  claimPendingExtractJobs(limit: number, leaseUntil: string): Promise<ClaimedExtractJob[]>;
  getExtractJob(jobId: string): Promise<ExtractJobStatusRecord | undefined>;
  completeExtractJob(result: {
    jobId: string;
    status: "completed" | "failed";
    finishedAt: string;
    error?: string;
    resultRef?: string;
  }): Promise<void>;
  listPendingDownloads(limit: number, force?: boolean, maxAttempts?: number): Promise<DownloadWorkItem[]>;
  markDownloadResult(result: DownloadResult): Promise<void>;
  listPendingExtracts(limit: number, force?: boolean, maxAttempts?: number): Promise<ExtractWorkItem[]>;
  markExtractionResult(result: ExtractionResult): Promise<void>;
  getStats(): Promise<StoreStats>;
  close(): Promise<void>;
}
