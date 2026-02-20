import fs from "node:fs";
import path from "node:path";
import { PDFParse } from "pdf-parse";
import { AppConfig } from "../config";
import { TableExtractor, TableExtractionOutput } from "./tableExtractor";

interface ParserLike {
  getTable(): Promise<{
    total: number;
    pages: Array<{
      num: number;
      tables: string[][][];
    }>;
  }>;
  destroy(): Promise<void>;
}

interface PdfParseTableExtractorDeps {
  parserFactory?: (data: Buffer) => ParserLike;
  readFile?: (filePath: string) => Promise<Buffer>;
}

export class PdfParseTableExtractor implements TableExtractor {
  private readonly outputDir: string;
  private readonly parserFactory: (data: Buffer) => ParserLike;
  private readonly readFile: (filePath: string) => Promise<Buffer>;

  constructor(config: AppConfig, deps?: PdfParseTableExtractorDeps) {
    this.outputDir = path.resolve(config.outputDirs.extracted);
    fs.mkdirSync(this.outputDir, { recursive: true });
    this.parserFactory =
      deps?.parserFactory ??
      ((data) =>
        new PDFParse({
          data,
        }));
    this.readFile = deps?.readFile ?? fs.promises.readFile;
  }

  async extractTables(docId: string, rawPdfPath: string): Promise<TableExtractionOutput> {
    const location = path.join(this.outputDir, `${docId}.tables.json`);
    const pdfBuffer = await this.readFile(rawPdfPath);
    const parser = this.parserFactory(pdfBuffer);

    let tableResult;
    try {
      tableResult = await parser.getTable();
    } finally {
      await parser.destroy().catch(() => undefined);
    }

    const pages = tableResult.pages.map((page) => ({
      pageNumber: page.num,
      tableCount: page.tables.length,
      tables: page.tables.map((rows, index) => ({
        index,
        rowCount: rows.length,
        rows,
      })),
    }));

    const payload = {
      docId,
      status: "ok",
      tableCount: pages.reduce((acc, p) => acc + p.tableCount, 0),
      pageCount: tableResult.total,
      pages,
      extractedAt: new Date().toISOString(),
    };

    await fs.promises.writeFile(location, JSON.stringify(payload, null, 2), "utf-8");
    return { location };
  }
}
