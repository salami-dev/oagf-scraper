import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";
import { Sink } from "./types";

export abstract class BaseSink implements Sink {
  abstract publishDiscovered(items: DocumentDiscoveredItem[]): Promise<void>;
  abstract publishDownloadResult(results: DownloadResult[]): Promise<void>;
  abstract publishExtractionResult(results: ExtractionResult[]): Promise<void>;

  protected ensureConfigured(name: string, ready: boolean): void {
    if (!ready) {
      throw new Error(`${name} sink is not configured`);
    }
  }
}
