import fs from "node:fs";
import path from "node:path";
import { AppConfig } from "../config";
import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";
import { BaseSink } from "./baseSink";

export class LocalJsonlSink extends BaseSink {
  private readonly discoveredPath: string;
  private readonly downloadsPath: string;
  private readonly extractsPath: string;
  private readonly runId: string;

  constructor(config: AppConfig, runId: string) {
    super();
    const manifestsDir = path.resolve(config.outputDirs.manifests);
    fs.mkdirSync(manifestsDir, { recursive: true });
    this.discoveredPath = path.join(manifestsDir, "discovered.jsonl");
    this.downloadsPath = path.join(manifestsDir, "downloads.jsonl");
    this.extractsPath = path.join(manifestsDir, "extracts.jsonl");
    this.runId = runId;
  }

  async publishDiscovered(items: DocumentDiscoveredItem[]): Promise<void> {
    await this.appendLines(
      this.discoveredPath,
      items.map((item) => ({
        runId: this.runId,
        ...item,
      })),
    );
  }

  async publishDownloadResult(results: DownloadResult[]): Promise<void> {
    await this.appendLines(
      this.downloadsPath,
      results.map((result) => ({
        runId: this.runId,
        ...result,
      })),
    );
  }

  async publishExtractionResult(results: ExtractionResult[]): Promise<void> {
    await this.appendLines(
      this.extractsPath,
      results.map((result) => ({
        runId: this.runId,
        ...result,
      })),
    );
  }

  private async appendLines(filePath: string, records: unknown[]): Promise<void> {
    if (records.length === 0) {
      return;
    }

    const content = records.map((record) => JSON.stringify(record)).join("\n") + "\n";
    await fs.promises.appendFile(filePath, content, "utf-8");
  }
}
