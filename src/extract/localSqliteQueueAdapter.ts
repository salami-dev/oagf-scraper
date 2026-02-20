import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { ExtractRequestMessageV1, ExtractResultMessageV1, isExtractRequestMessageV1, isExtractResultMessageV1 } from "./asyncMessages";
import { AsyncTableQueue, QueuedExtractRequest, QueuedExtractResult } from "./asyncQueue";

type QueueRow = {
  queueMessageId: string;
  payload: string;
};

export class LocalSqliteQueueAdapter implements AsyncTableQueue {
  private readonly db: Database.Database;

  constructor(dbPath: string) {
    const absolutePath = path.resolve(dbPath);
    fs.mkdirSync(path.dirname(absolutePath), { recursive: true });
    this.db = new Database(absolutePath);
    this.db.pragma("journal_mode = WAL");
    this.initializeSchema();
  }

  async publishRequest(message: ExtractRequestMessageV1): Promise<void> {
    this.db
      .prepare(
        `
        INSERT INTO queue_requests (queueMessageId, payload, status, createdAt)
        VALUES (@queueMessageId, @payload, 'pending', @createdAt)
      `,
      )
      .run({
        queueMessageId: crypto.randomUUID(),
        payload: JSON.stringify(message),
        createdAt: new Date().toISOString(),
      });
  }

  async consumeRequests(limit: number): Promise<QueuedExtractRequest[]> {
    return this.consumeAndLease<ExtractRequestMessageV1>(
      "queue_requests",
      limit,
      isExtractRequestMessageV1,
      (queueMessageId, message) => ({ queueMessageId, message }),
    );
  }

  async ackRequest(queueMessageId: string): Promise<void> {
    this.ack("queue_requests", queueMessageId);
  }

  async publishResult(message: ExtractResultMessageV1): Promise<void> {
    this.db
      .prepare(
        `
        INSERT INTO queue_results (queueMessageId, payload, status, createdAt)
        VALUES (@queueMessageId, @payload, 'pending', @createdAt)
      `,
      )
      .run({
        queueMessageId: crypto.randomUUID(),
        payload: JSON.stringify(message),
        createdAt: new Date().toISOString(),
      });
  }

  async consumeResults(limit: number): Promise<QueuedExtractResult[]> {
    return this.consumeAndLease<ExtractResultMessageV1>(
      "queue_results",
      limit,
      isExtractResultMessageV1,
      (queueMessageId, message) => ({ queueMessageId, message }),
    );
  }

  async ackResult(queueMessageId: string): Promise<void> {
    this.ack("queue_results", queueMessageId);
  }

  async close(): Promise<void> {
    this.db.close();
  }

  private async consumeAndLease<T>(
    tableName: "queue_requests" | "queue_results",
    limit: number,
    guard: (value: unknown) => value is T,
    projector: (queueMessageId: string, message: T) => QueuedExtractRequest,
  ): Promise<QueuedExtractRequest[]>;
  private async consumeAndLease<T>(
    tableName: "queue_requests" | "queue_results",
    limit: number,
    guard: (value: unknown) => value is T,
    projector: (queueMessageId: string, message: T) => QueuedExtractResult,
  ): Promise<QueuedExtractResult[]>;
  private async consumeAndLease<T>(
    tableName: "queue_requests" | "queue_results",
    limit: number,
    guard: (value: unknown) => value is T,
    projector: (queueMessageId: string, message: T) => QueuedExtractRequest | QueuedExtractResult,
  ): Promise<Array<QueuedExtractRequest | QueuedExtractResult>> {
    const rows = this.db
      .prepare(
        `
        SELECT queueMessageId, payload
        FROM ${tableName}
        WHERE status = 'pending'
        ORDER BY createdAt ASC
        LIMIT ?
      `,
      )
      .all(limit) as QueueRow[];

    if (rows.length === 0) {
      return [];
    }

    const leaseStmt = this.db.prepare(`UPDATE ${tableName} SET status = 'leased' WHERE queueMessageId = ?`);
    const tx = this.db.transaction((leaseRows: QueueRow[]) => {
      for (const row of leaseRows) {
        leaseStmt.run(row.queueMessageId);
      }
    });
    tx(rows);

    const messages: Array<QueuedExtractRequest | QueuedExtractResult> = [];
    for (const row of rows) {
      const parsed = JSON.parse(row.payload) as unknown;
      if (!guard(parsed)) {
        this.db.prepare(`UPDATE ${tableName} SET status = 'failed' WHERE queueMessageId = ?`).run(row.queueMessageId);
        continue;
      }
      messages.push(projector(row.queueMessageId, parsed));
    }

    return messages;
  }

  private ack(tableName: "queue_requests" | "queue_results", queueMessageId: string): void {
    this.db.prepare(`DELETE FROM ${tableName} WHERE queueMessageId = ?`).run(queueMessageId);
  }

  private initializeSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS queue_requests (
        queueMessageId TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        createdAt TEXT NOT NULL,
        leaseUntil TEXT NULL
      );

      CREATE TABLE IF NOT EXISTS queue_results (
        queueMessageId TEXT PRIMARY KEY,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        createdAt TEXT NOT NULL,
        leaseUntil TEXT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_queue_requests_status ON queue_requests(status);
      CREATE INDEX IF NOT EXISTS idx_queue_results_status ON queue_results(status);
    `);

    this.ensureColumn("queue_requests", "leaseUntil", "TEXT NULL");
    this.ensureColumn("queue_results", "leaseUntil", "TEXT NULL");
  }

  private ensureColumn(tableName: "queue_requests" | "queue_results", columnName: string, columnType: string): void {
    const columns = this.db.prepare(`PRAGMA table_info(${tableName})`).all() as Array<{ name: string }>;
    if (!columns.some((column) => column.name === columnName)) {
      this.db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnType}`);
    }
  }
}
