import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";
import { PipelineStore, RawFileItem, RevalidationItem, StoreStats } from "./types";

const ZERO_STATS: StoreStats = {
  totalDocuments: 0,
  discovered: 0,
  downloadedOk: 0,
  downloadedFailed: 0,
  extractedOk: 0,
  extractedFailed: 0,
};

export class InMemoryStore implements PipelineStore {
  async startRun(_runId: string, _startedAt: string): Promise<void> {
    return;
  }

  async finishRun(_runId: string, _status: "completed" | "failed", _finishedAt: string): Promise<void> {
    return;
  }

  async upsertDiscovered(_item: DocumentDiscoveredItem): Promise<void> {
    return;
  }

  async listRevalidationCandidates(_limit: number, _checkedBeforeIso: string): Promise<RevalidationItem[]> {
    return [];
  }

  async listDownloadedRawFiles(_limit: number): Promise<RawFileItem[]> {
    return [];
  }

  async markRemoteUnchanged(_docId: string, _checkedAt: string, _etag?: string, _lastModified?: string): Promise<void> {
    return;
  }

  async markRemoteChanged(_docId: string, _checkedAt: string, _etag?: string, _lastModified?: string): Promise<void> {
    return;
  }

  async markRawMissing(_docId: string, _checkedAt: string): Promise<void> {
    return;
  }

  async listPendingDownloads(_limit: number, _force = false, _maxAttempts = 3): Promise<[]> {
    return [];
  }

  async markDownloadResult(_result: DownloadResult): Promise<void> {
    return;
  }

  async listPendingExtracts(_limit: number, _force = false, _maxAttempts = 3): Promise<[]> {
    return [];
  }

  async markExtractionResult(_result: ExtractionResult): Promise<void> {
    return;
  }

  async getStats(): Promise<StoreStats> {
    return { ...ZERO_STATS };
  }

  async close(): Promise<void> {
    return;
  }
}
