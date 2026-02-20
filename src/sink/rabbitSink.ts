import type { ChannelModel, Options } from "amqplib";
import { connect as amqpConnect } from "amqplib";
import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";
import { BaseSink } from "./baseSink";

type StageName = "discovered" | "download" | "extract";
type StagePayload = DocumentDiscoveredItem | DownloadResult | ExtractionResult;

type ConnectFn = (url: string) => Promise<ConnectionLike>;

interface ConnectionLike {
  createConfirmChannel(): Promise<ChannelLike>;
  close(): Promise<void>;
}

interface ChannelLike {
  assertExchange(exchange: string, type: string, options?: Options.AssertExchange): Promise<unknown>;
  publish(exchange: string, routingKey: string, content: Buffer, options?: Options.Publish): boolean;
  waitForConfirms?(): Promise<void>;
  close(): Promise<void>;
}

export interface RabbitSinkOptions {
  connectionUrl?: string;
  exchange?: string;
  routingKey?: string;
  exchangeType?: string;
  maxRetries?: number;
  retryDelayMs?: number;
  connectFn?: ConnectFn;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class RabbitSink extends BaseSink {
  private readonly connectionUrl?: string;
  private readonly exchange: string;
  private readonly routingKey: string;
  private readonly exchangeType: string;
  private readonly maxRetries: number;
  private readonly retryDelayMs: number;
  private readonly connectFn: ConnectFn;

  constructor(optionsOrConnectionUrl?: RabbitSinkOptions | string) {
    super();
    const options: RabbitSinkOptions =
      typeof optionsOrConnectionUrl === "string" ? { connectionUrl: optionsOrConnectionUrl } : (optionsOrConnectionUrl ?? {});

    this.connectionUrl = options.connectionUrl;
    this.exchange = options.exchange ?? "osgf.extractor";
    this.routingKey = options.routingKey ?? "faac.events";
    this.exchangeType = options.exchangeType ?? "topic";
    this.maxRetries = options.maxRetries ?? 3;
    this.retryDelayMs = options.retryDelayMs ?? 250;
    this.connectFn = options.connectFn ?? (async (url: string) => (amqpConnect(url) as Promise<ChannelModel>) as Promise<ConnectionLike>);
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

    this.ensureConfigured("RabbitMQ", Boolean(this.connectionUrl));
    const url = this.connectionUrl as string;

    let attempt = 0;
    while (true) {
      attempt += 1;
      let connection: ConnectionLike | undefined;
      let channel: ChannelLike | undefined;

      try {
        connection = await this.connectFn(url);
        channel = await connection.createConfirmChannel();
        await channel.assertExchange(this.exchange, this.exchangeType, { durable: true });

        for (const payload of payloads) {
          const docId = getDocId(payload);
          const body = Buffer.from(
            JSON.stringify({
              stage,
              docId,
              sentAt: new Date().toISOString(),
              payload,
            }),
          );

          channel.publish(this.exchange, this.routingKey, body, {
            persistent: true,
            contentType: "application/json",
            headers: {
              "x-stage": stage,
              "x-doc-id": docId,
              "x-idempotency-key": `${stage}:${docId}`,
            },
          });
        }

        if (typeof channel.waitForConfirms === "function") {
          await channel.waitForConfirms();
        }

        await channel.close();
        await connection.close();
        return;
      } catch (error) {
        if (channel) {
          await channel.close().catch(() => undefined);
        }
        if (connection) {
          await connection.close().catch(() => undefined);
        }

        if (attempt > this.maxRetries) {
          throw error;
        }
      }

      await sleep(this.retryDelayMs * attempt);
    }
  }
}
