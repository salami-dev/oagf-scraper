import crypto from "node:crypto";
import { Logger } from "../observability";
import { PipelineStore } from "../store";
import { ExtractionResult } from "../types";
import { ExtractResultMessageV1 } from "./asyncMessages";
import { AsyncTableQueue } from "./asyncQueue";

export interface SubmitExtractJobsDeps {
  runId: string;
  store: PipelineStore;
  queue: AsyncTableQueue;
  logger: Logger;
  force?: boolean;
  limit: number;
  maxExtractAttempts: number;
}

export interface CollectExtractResultsDeps {
  store: PipelineStore;
  queue: AsyncTableQueue;
  logger: Logger;
  limit: number;
}

export function createExtractJobId(docId: string, rawPdfPath: string): string {
  return crypto.createHash("sha256").update(`${docId}|${rawPdfPath}`).digest("hex").slice(0, 24);
}

export async function submitExtractJobs(deps: SubmitExtractJobsDeps): Promise<{ submitted: number }> {
  const pending = await deps.store.listPendingExtracts(deps.limit, deps.force ?? false, deps.maxExtractAttempts);
  let submitted = 0;

  for (const item of pending) {
    const jobId = createExtractJobId(item.docId, item.rawLocation);
    const submittedAt = new Date().toISOString();
    await deps.store.enqueueExtractJob({
      jobId,
      runId: deps.runId,
      docId: item.docId,
      rawPdfPath: item.rawLocation,
      attempt: item.attempt,
      submittedAt,
    });

    await deps.queue.publishRequest({
      version: "v1",
      type: "tables.extract.request",
      jobId,
      runId: deps.runId,
      docId: item.docId,
      rawPdfPath: item.rawLocation,
      attempt: item.attempt,
      submittedAt,
    });
    submitted += 1;
  }

  deps.logger.info("extract_submit_complete", { submitted });
  return { submitted };
}

export async function collectExtractResults(deps: CollectExtractResultsDeps): Promise<{ processed: number; ok: number; failed: number }> {
  const messages = await deps.queue.consumeResults(deps.limit);
  let processed = 0;
  let ok = 0;
  let failed = 0;

  for (const queued of messages) {
    const message = queued.message;
    const job = await deps.store.getExtractJob(message.jobId);
    if (!job) {
      deps.logger.warn("extract_collect_missing_job", { jobId: message.jobId, docId: message.docId });
      await deps.queue.ackResult(queued.queueMessageId);
      continue;
    }

    const extractionResult = mapResultMessageToExtractionResult(message, job.attempt);
    await deps.store.markExtractionResult(extractionResult);
    await deps.store.completeExtractJob({
      jobId: message.jobId,
      status: message.status === "failed" ? "failed" : "completed",
      finishedAt: message.finishedAt,
      error: message.error,
      resultRef: message.tablesLocation,
    });
    await deps.queue.ackResult(queued.queueMessageId);

    processed += 1;
    if (message.status === "failed") {
      failed += 1;
    } else {
      ok += 1;
    }
  }

  deps.logger.info("extract_collect_complete", { processed, ok, failed });
  return { processed, ok, failed };
}

function mapResultMessageToExtractionResult(message: ExtractResultMessageV1, attempt: number): ExtractionResult {
  if (message.status === "failed") {
    const error = message.errorCode
      ? `${message.errorCode}${message.error ? `: ${message.error}` : ""}`
      : (message.error ?? "table_extraction_failed");
    return {
      docId: message.docId,
      status: "extracted_failed",
      error,
      attempt,
      extractedAt: message.finishedAt,
    };
  }

  return {
    docId: message.docId,
    status: "extracted_ok",
    tablesLocation: message.tablesLocation,
    attempt,
    extractedAt: message.finishedAt,
  };
}
