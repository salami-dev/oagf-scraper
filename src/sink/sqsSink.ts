import { SendMessageBatchCommand, SQSClient } from "@aws-sdk/client-sqs";
import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";
import { BaseSink } from "./baseSink";

type StageName = "discovered" | "download" | "extract";

type SinkPayload = DocumentDiscoveredItem | DownloadResult | ExtractionResult;

interface SqsClientLike {
  send(command: SendMessageBatchCommand): Promise<{ Failed?: Array<{ Id?: string; SenderFault?: boolean }> }>;
}

export interface SqsSinkOptions {
  queueUrl?: string;
  client?: SqsClientLike;
  fifo?: boolean;
  groupId?: string;
  maxRetries?: number;
  retryDelayMs?: number;
}

interface BatchEntry {
  Id: string;
  MessageBody: string;
  MessageDeduplicationId?: string;
  MessageGroupId?: string;
}

const BATCH_LIMIT = 10;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function chunk<T>(items: T[], size: number): T[][] {
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

export class SqsSink extends BaseSink {
  private readonly queueUrl?: string;
  private readonly client: SqsClientLike;
  private readonly fifo: boolean;
  private readonly groupId: string;
  private readonly maxRetries: number;
  private readonly retryDelayMs: number;

  constructor(optionsOrQueueUrl?: SqsSinkOptions | string) {
    super();
    const options: SqsSinkOptions =
      typeof optionsOrQueueUrl === "string" ? { queueUrl: optionsOrQueueUrl } : (optionsOrQueueUrl ?? {});

    this.queueUrl = options.queueUrl;
    this.client = options.client ?? new SQSClient({});
    this.fifo = options.fifo ?? Boolean(this.queueUrl?.endsWith(".fifo"));
    this.groupId = options.groupId ?? "osgf-extractor";
    this.maxRetries = options.maxRetries ?? 3;
    this.retryDelayMs = options.retryDelayMs ?? 200;
  }

  async publishDiscovered(items: DocumentDiscoveredItem[]): Promise<void> {
    await this.publishStage("discovered", items, (item) => item.docId);
  }

  async publishDownloadResult(results: DownloadResult[]): Promise<void> {
    await this.publishStage("download", results, (item) => item.docId);
  }

  async publishExtractionResult(results: ExtractionResult[]): Promise<void> {
    await this.publishStage("extract", results, (item) => item.docId);
  }

  private async publishStage<T extends SinkPayload>(stage: StageName, payloads: T[], getDocId: (item: T) => string): Promise<void> {
    if (payloads.length === 0) {
      return;
    }

    this.ensureConfigured("SQS", Boolean(this.queueUrl));
    const queueUrl = this.queueUrl as string;

    const entries = payloads.map((payload, index) => {
      const docId = getDocId(payload);
      const entry: BatchEntry = {
        Id: String(index),
        MessageBody: JSON.stringify({
          stage,
          docId,
          sentAt: new Date().toISOString(),
          payload,
        }),
      };

      if (this.fifo) {
        entry.MessageGroupId = this.groupId;
        entry.MessageDeduplicationId = `${docId}:${stage}`;
      }

      return entry;
    });

    for (const entryBatch of chunk(entries, BATCH_LIMIT)) {
      await this.sendBatchWithRetries(queueUrl, entryBatch);
    }
  }

  private async sendBatchWithRetries(queueUrl: string, originalEntries: BatchEntry[]): Promise<void> {
    let pendingEntries = [...originalEntries];
    let attempt = 0;

    while (pendingEntries.length > 0) {
      attempt += 1;
      const command = new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: pendingEntries,
      });
      const response = await this.client.send(command);

      const failedIds = new Set((response.Failed ?? []).map((f) => f.Id).filter((id): id is string => Boolean(id)));
      if (failedIds.size === 0) {
        return;
      }

      if (attempt > this.maxRetries) {
        throw new Error(`SQS publish failed after retries (${failedIds.size} entries still failed)`);
      }

      pendingEntries = pendingEntries.filter((entry) => failedIds.has(entry.Id));
      await sleep(this.retryDelayMs * attempt);
    }
  }
}
