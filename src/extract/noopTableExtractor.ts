import fs from "node:fs";
import path from "node:path";
import { AppConfig } from "../config";
import { TableExtractor, TableExtractionOutput } from "./tableExtractor";

export class NoopTableExtractor implements TableExtractor {
  private readonly outputDir: string;

  constructor(config: AppConfig) {
    this.outputDir = path.resolve(config.outputDirs.extracted);
    fs.mkdirSync(this.outputDir, { recursive: true });
  }

  async extractTables(docId: string, _rawPdfPath: string): Promise<TableExtractionOutput> {
    const location = path.join(this.outputDir, `${docId}.tables.json`);
    const payload = {
      docId,
      status: "not_supported",
      note: "Table extraction plugin not configured",
      extractedAt: new Date().toISOString(),
    };
    await fs.promises.writeFile(location, JSON.stringify(payload, null, 2), "utf-8");
    return { location, note: payload.note };
  }
}
