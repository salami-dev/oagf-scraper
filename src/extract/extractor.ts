import fs from "node:fs";
import path from "node:path";
import { PDFParse } from "pdf-parse";
import { AppConfig } from "../config";
import { Logger, MetricsRegistry } from "../observability";
import { Sink } from "../sink";
import { PipelineStore } from "../store";
import { ExtractionResult } from "../types";
import { NoopTableExtractor } from "./noopTableExtractor";
import { PdfParseTableExtractor } from "./pdfParseTableExtractor";
import { TableExtractor } from "./tableExtractor";

interface ExtractorDeps {
  config: AppConfig;
  logger: Logger;
  metrics: MetricsRegistry;
  store: PipelineStore;
  sink: Sink;
  force: boolean;
  maxDocs?: number;
  tableExtractor?: TableExtractor;
  textExtractor?: (rawPdfPath: string) => Promise<{ text: string; pageCount: number }>;
}

interface ExtractionSummary {
  processed: number;
  ok: number;
  failed: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function processWithConcurrency<T>(
  items: T[],
  concurrency: number,
  worker: (item: T) => Promise<void>,
): Promise<void> {
  let index = 0;
  const workers = new Array(Math.max(1, concurrency)).fill(null).map(async () => {
    while (true) {
      const current = index;
      index += 1;
      if (current >= items.length) {
        break;
      }
      await worker(items[current]);
    }
  });
  await Promise.all(workers);
}

export async function runExtractor(deps: ExtractorDeps): Promise<ExtractionSummary> {
  const { config, logger, metrics, store, sink, force } = deps;
  const tableExtractor = deps.tableExtractor ?? new PdfParseTableExtractor(config);
  const fallbackTableExtractor = new NoopTableExtractor(config);
  const textExtractor = deps.textExtractor ?? extractTextWithPdfParse;
  const processedDocIds = new Set<string>();
  const outputDir = path.resolve(config.outputDirs.extracted);
  fs.mkdirSync(outputDir, { recursive: true });

  let processed = 0;
  let ok = 0;
  let failed = 0;
  const batchSize = Math.max(config.extractConcurrency * 2, 1);

  while (true) {
    if (deps.maxDocs !== undefined && processed >= deps.maxDocs) {
      break;
    }

    const remaining = deps.maxDocs !== undefined ? Math.max(deps.maxDocs - processed, 0) : batchSize;
    const requestSize = Math.min(batchSize, remaining || batchSize);
    const workItems = (await store.listPendingExtracts(requestSize, force, config.maxExtractAttempts)).filter(
      (item) => !processedDocIds.has(item.docId),
    );
    if (workItems.length === 0) {
      break;
    }

    await processWithConcurrency(workItems, config.extractConcurrency, async (item) => {
      let result: ExtractionResult | undefined;
      for (let attempt = item.attempt; attempt <= config.maxExtractAttempts; attempt += 1) {
        const stopTimer = metrics.startTimer("extract_ms");
        logger.info("extract_item_attempt_start", { docId: item.docId, attempt });

        try {
          const parsed = await textExtractor(item.rawLocation);
          const textLocation = path.join(outputDir, `${item.docId}.txt`);
          await fs.promises.writeFile(textLocation, parsed.text ?? "", "utf-8");

          let tables;
          try {
            tables = await tableExtractor.extractTables(item.docId, item.rawLocation);
          } catch (tableError) {
            logger.warn("extract_table_error_fallback", {
              docId: item.docId,
              attempt,
              error: tableError instanceof Error ? tableError.message : String(tableError),
            });
            tables = await fallbackTableExtractor.extractTables(item.docId, item.rawLocation);
          }
          const durationMs = stopTimer();
          result = {
            docId: item.docId,
            status: "extracted_ok",
            pageCount: parsed.pageCount,
            textLocation,
            tablesLocation: tables.location,
            attempt,
            extractedAt: new Date().toISOString(),
          };
          logger.info("extract_item_ok", { docId: item.docId, attempt, durationMs, pageCount: parsed.pageCount });
          break;
        } catch (error) {
          const durationMs = stopTimer();
          const message = error instanceof Error ? error.message : String(error);
          logger.warn("extract_item_error", { docId: item.docId, attempt, durationMs, error: message });

          if (attempt >= config.maxExtractAttempts) {
            result = {
              docId: item.docId,
              status: "extracted_failed",
              error: message,
              attempt,
              extractedAt: new Date().toISOString(),
            };
            break;
          }

          await sleep(Math.min(1000 * 2 ** (attempt - 1), 10_000));
        }
      }

      if (!result) {
        result = {
          docId: item.docId,
          status: "extracted_failed",
          error: "Unknown extraction failure",
          attempt: config.maxExtractAttempts,
          extractedAt: new Date().toISOString(),
        };
      }

      await store.markExtractionResult(result);
      await sink.publishExtractionResult([result]);
      processedDocIds.add(item.docId);
      processed += 1;

      if (result.status === "extracted_ok") {
        metrics.incrementCounter("extracts_ok", 1);
        ok += 1;
      } else {
        metrics.incrementCounter("extracts_failed", 1);
        failed += 1;
      }
    });
  }

  return { processed, ok, failed };
}

async function extractTextWithPdfParse(rawPdfPath: string): Promise<{ text: string; pageCount: number }> {
  const pdfBuffer = await fs.promises.readFile(rawPdfPath);
  const parser = new PDFParse({ data: pdfBuffer });
  try {
    const parsed = await parser.getText();
    return {
      text: parsed.text ?? "",
      pageCount: parsed.total,
    };
  } finally {
    await parser.destroy().catch(() => undefined);
  }
}
