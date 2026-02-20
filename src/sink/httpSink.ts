import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";
import { BaseSink } from "./baseSink";

type StageName = "discovered" | "download" | "extract";
type StagePayload = DocumentDiscoveredItem | DownloadResult | ExtractionResult;

interface HttpResponseLike {
  ok: boolean;
  status: number;
  text(): Promise<string>;
}

type FetchLike = (url: string, init: { method: string; headers: Record<string, string>; body: string; signal: AbortSignal }) => Promise<HttpResponseLike>;

export interface HttpSinkOptions {
  endpoint?: string;
  token?: string;
  fetchFn?: FetchLike;
  timeoutMs?: number;
  maxRetries?: number;
  retryDelayMs?: number;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRetriableStatus(status: number): boolean {
  return status === 408 || status === 429 || status >= 500;
}

class PermanentHttpSinkError extends Error {}

export class HttpSink extends BaseSink {
  private readonly endpoint?: string;
  private readonly token?: string;
  private readonly fetchFn: FetchLike;
  private readonly timeoutMs: number;
  private readonly maxRetries: number;
  private readonly retryDelayMs: number;

  constructor(optionsOrEndpoint?: HttpSinkOptions | string, token?: string) {
    super();
    const options: HttpSinkOptions =
      typeof optionsOrEndpoint === "string" ? { endpoint: optionsOrEndpoint, token } : (optionsOrEndpoint ?? {});

    this.endpoint = options.endpoint;
    this.token = options.token;
    this.fetchFn = options.fetchFn ?? ((url, init) => fetch(url, init) as Promise<HttpResponseLike>);
    this.timeoutMs = options.timeoutMs ?? 15_000;
    this.maxRetries = options.maxRetries ?? 3;
    this.retryDelayMs = options.retryDelayMs ?? 250;
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

  private async publishStage<T extends StagePayload>(stage: StageName, payloads: T[], getDocId: (item: T) => string): Promise<void> {
    if (payloads.length === 0) {
      return;
    }

    this.ensureConfigured("HTTP", Boolean(this.endpoint));
    const endpoint = this.endpoint as string;
    const docIds = payloads.map((item) => getDocId(item));

    const body = JSON.stringify({
      stage,
      sentAt: new Date().toISOString(),
      items: payloads,
    });

    const headers: Record<string, string> = {
      "Content-Type": "application/json",
      "Idempotency-Key": `${stage}:${docIds.join(",")}`,
    };

    if (this.token) {
      headers.Authorization = `Bearer ${this.token}`;
    }

    let attempt = 0;
    while (true) {
      attempt += 1;
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
      try {
        const response = await this.fetchFn(endpoint, {
          method: "POST",
          headers,
          body,
          signal: controller.signal,
        });

        if (response.ok) {
          return;
        }

        const responseText = await response.text();
        if (!isRetriableStatus(response.status)) {
          throw new PermanentHttpSinkError(`HTTP sink permanent error ${response.status}: ${responseText}`);
        }

        if (attempt > this.maxRetries) {
          throw new Error(`HTTP sink exhausted retries on status ${response.status}: ${responseText}`);
        }
      } catch (error) {
        if (error instanceof PermanentHttpSinkError) {
          throw error;
        }
        if (attempt > this.maxRetries) {
          throw error;
        }
      } finally {
        clearTimeout(timeout);
      }

      await sleep(this.retryDelayMs * attempt);
    }
  }
}
