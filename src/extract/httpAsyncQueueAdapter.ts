import { getFetchDispatcher } from "../core/fetch";
import { ExtractRequestMessageV1, ExtractResultMessageV1, isExtractRequestMessageV1, isExtractResultMessageV1 } from "./asyncMessages";
import { AsyncTableQueue, QueuedExtractRequest, QueuedExtractResult } from "./asyncQueue";

interface HttpResponseLike {
  ok: boolean;
  status: number;
  text(): Promise<string>;
}

type FetchLike = (
  url: string,
  init: {
    method: string;
    headers: Record<string, string>;
    body?: string;
    signal: AbortSignal;
    dispatcher?: unknown;
  },
) => Promise<HttpResponseLike>;

export interface HttpAsyncQueueAdapterOptions {
  baseUrl: string;
  token?: string;
  timeoutMs?: number;
  ignoreHttpsErrors?: boolean;
  fetchFn?: FetchLike;
}

interface LeaseResponse<T> {
  messages?: Array<{
    queueMessageId: string;
    payload: T;
  }>;
}

type UnknownLeasedMessage = {
  queueMessageId: string;
  payload: unknown;
};

export class HttpAsyncQueueAdapter implements AsyncTableQueue {
  private readonly baseUrl: string;
  private readonly token?: string;
  private readonly timeoutMs: number;
  private readonly dispatcher: unknown;
  private readonly fetchFn: FetchLike;

  constructor(options: HttpAsyncQueueAdapterOptions) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.token = options.token;
    this.timeoutMs = options.timeoutMs ?? 15_000;
    this.dispatcher = getFetchDispatcher(options.ignoreHttpsErrors ?? false);
    this.fetchFn = options.fetchFn ?? ((url, init) => fetch(url, init) as Promise<HttpResponseLike>);
  }

  async publishRequest(message: ExtractRequestMessageV1): Promise<void> {
    await this.post("/v1/queue/requests", { messages: [message] });
  }

  async consumeRequests(limit: number): Promise<QueuedExtractRequest[]> {
    const payload = await this.postAndRead<LeaseResponse<unknown>>("/v1/queue/requests/lease", { limit });
    const messages = (payload.messages ?? []) as UnknownLeasedMessage[];
    return messages
      .filter(isLeasedRequest)
      .map((item) => ({ queueMessageId: item.queueMessageId, message: item.payload }));
  }

  async ackRequest(queueMessageId: string): Promise<void> {
    await this.post("/v1/queue/requests/ack", { ids: [queueMessageId] });
  }

  async publishResult(message: ExtractResultMessageV1): Promise<void> {
    await this.post("/v1/queue/results", { messages: [message] });
  }

  async consumeResults(limit: number): Promise<QueuedExtractResult[]> {
    const payload = await this.postAndRead<LeaseResponse<unknown>>("/v1/queue/results/lease", { limit });
    const messages = (payload.messages ?? []) as UnknownLeasedMessage[];
    return messages
      .filter(isLeasedResult)
      .map((item) => ({ queueMessageId: item.queueMessageId, message: item.payload }));
  }

  async ackResult(queueMessageId: string): Promise<void> {
    await this.post("/v1/queue/results/ack", { ids: [queueMessageId] });
  }

  async close(): Promise<void> {
    return Promise.resolve();
  }

  private async post(pathname: string, body: unknown): Promise<void> {
    await this.request(pathname, body);
  }

  private async postAndRead<T>(pathname: string, body: unknown): Promise<T> {
    const response = await this.request(pathname, body);
    const text = await response.text();
    return text.length > 0 ? (JSON.parse(text) as T) : ({} as T);
  }

  private async request(pathname: string, body: unknown): Promise<HttpResponseLike> {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    if (this.token) {
      headers.Authorization = `Bearer ${this.token}`;
    }

    try {
      const response = await this.fetchFn(`${this.baseUrl}${pathname}`, {
        method: "POST",
        headers,
        body: JSON.stringify(body),
        signal: controller.signal,
        dispatcher: this.dispatcher,
      });

      if (!response.ok) {
        const responseText = await response.text();
        throw new Error(`HTTP async queue error ${response.status}: ${responseText}`);
      }

      return response;
    } finally {
      clearTimeout(timeout);
    }
  }
}

function isLeasedRequest(value: UnknownLeasedMessage): value is { queueMessageId: string; payload: ExtractRequestMessageV1 } {
  return isExtractRequestMessageV1(value.payload);
}

function isLeasedResult(value: UnknownLeasedMessage): value is { queueMessageId: string; payload: ExtractResultMessageV1 } {
  return isExtractResultMessageV1(value.payload);
}
