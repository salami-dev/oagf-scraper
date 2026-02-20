import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";

export interface Sink {
  publishDiscovered(items: DocumentDiscoveredItem[]): Promise<void>;
  publishDownloadResult(results: DownloadResult[]): Promise<void>;
  publishExtractionResult(results: ExtractionResult[]): Promise<void>;
}
