import fs from "node:fs";
import path from "node:path";
import Database from "better-sqlite3";
import { DownloadResult, DocumentDiscoveredItem, ExtractionResult } from "../types";
import {
  ClaimedExtractJob,
  DownloadWorkItem,
  ExtractJobRecord,
  ExtractJobStatusRecord,
  ExtractWorkItem,
  PipelineStore,
  RawFileItem,
  RevalidationItem,
  StoreStats,
} from "./types";

type DocumentRow = {
  docId: string;
  url: string;
  title: string;
  attemptsDownload: number;
  rawLocation: string | null;
};

type ExtractRow = {
  docId: string;
  rawLocation: string;
  attemptsExtract: number;
};

type RevalidationRow = {
  docId: string;
  url: string;
  etag: string | null;
  lastModified: string | null;
  lastCheckedAt: string | null;
};

type RawFileRow = {
  docId: string;
  rawLocation: string;
};

type ExtractJobRow = {
  jobId: string;
  runId: string;
  docId: string;
  rawPdfPath: string;
  fileSha256: string | null;
  attempt: number;
  submittedAt: string;
  leaseUntil: string;
  status?: "queued" | "leased" | "completed" | "failed";
  finishedAt?: string | null;
  error?: string | null;
  resultRef?: string | null;
};

export class SqliteStore implements PipelineStore {
  private readonly db: Database.Database;

  constructor(dbPath: string) {
    const absolutePath = path.resolve(dbPath);
    fs.mkdirSync(path.dirname(absolutePath), { recursive: true });
    this.db = new Database(absolutePath);
    this.db.pragma("journal_mode = WAL");
    this.initializeSchema();
  }

  async upsertDiscovered(item: DocumentDiscoveredItem): Promise<void> {
    const now = new Date().toISOString();
    const statement = this.db.prepare(`
      INSERT INTO documents (
        docId, url, title, year, month, sourcePageUrl,
        firstSeenAt, lastSeenAt, lastStatus, updatedAt
      )
      VALUES (
        @docId, @url, @title, @year, @month, @sourcePageUrl,
        @firstSeenAt, @lastSeenAt, 'discovered', @updatedAt
      )
      ON CONFLICT(docId) DO UPDATE SET
        url = excluded.url,
        title = excluded.title,
        year = excluded.year,
        month = excluded.month,
        sourcePageUrl = excluded.sourcePageUrl,
        lastSeenAt = excluded.lastSeenAt,
        updatedAt = excluded.updatedAt
    `);

    statement.run({
      docId: item.docId,
      url: item.url,
      title: item.title,
      year: item.year ?? null,
      month: item.month ?? null,
      sourcePageUrl: item.sourcePageUrl,
      firstSeenAt: item.discoveredAt,
      lastSeenAt: item.discoveredAt || now,
      updatedAt: now,
    });
  }

  async startRun(runId: string, startedAt: string): Promise<void> {
    this.db
      .prepare(
        `
        INSERT INTO runs (runId, startedAt, finishedAt, status)
        VALUES (@runId, @startedAt, NULL, 'running')
        ON CONFLICT(runId) DO UPDATE SET
          startedAt = excluded.startedAt,
          finishedAt = NULL,
          status = 'running'
      `,
      )
      .run({
        runId,
        startedAt,
      });
  }

  async finishRun(runId: string, status: "completed" | "failed", finishedAt: string): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE runs
        SET
          status = @status,
          finishedAt = @finishedAt
        WHERE runId = @runId
      `,
      )
      .run({
        runId,
        status,
        finishedAt,
      });
  }

  async listPendingDownloads(limit: number, force = false, maxAttempts = 3): Promise<DownloadWorkItem[]> {
    if (force) {
      const rows = this.db
        .prepare(
          `
          SELECT docId, url, title, attemptsDownload, rawLocation
          FROM documents
          ORDER BY updatedAt ASC
          LIMIT ?
        `,
        )
        .all(limit) as DocumentRow[];

      return rows.map((row) => ({
        docId: row.docId,
        url: row.url,
        title: row.title,
        attempt: row.attemptsDownload + 1,
      }));
    }

    const rows = this.db
      .prepare(
        `
        SELECT docId, url, title, attemptsDownload, rawLocation
        FROM documents
        WHERE
          (
            lastStatus = 'discovered'
            OR (lastStatus = 'download_failed' AND (error IS NULL OR error NOT LIKE 'HTTP 404%'))
            OR (lastStatus = 'downloaded_ok' AND (rawLocation IS NULL OR sha256 IS NULL))
            OR lastStatus IS NULL
          )
          AND attemptsDownload < ?
        ORDER BY updatedAt ASC
        LIMIT ?
      `,
      )
      .all(maxAttempts, limit) as DocumentRow[];

    return rows.map((row) => ({
      docId: row.docId,
      url: row.url,
      title: row.title,
      attempt: row.attemptsDownload + 1,
    }));
  }

  async listRevalidationCandidates(limit: number, checkedBeforeIso: string): Promise<RevalidationItem[]> {
    const rows = this.db
      .prepare(
        `
        SELECT docId, url, etag, lastModified, lastCheckedAt
        FROM documents
        WHERE
          rawLocation IS NOT NULL
          AND lastStatus IN ('downloaded_ok', 'extracted_ok')
          AND (lastCheckedAt IS NULL OR lastCheckedAt < ?)
        ORDER BY COALESCE(lastCheckedAt, '1970-01-01T00:00:00.000Z') ASC, updatedAt ASC
        LIMIT ?
      `,
      )
      .all(checkedBeforeIso, limit) as RevalidationRow[];

    return rows.map((row) => ({
      docId: row.docId,
      url: row.url,
      etag: row.etag ?? undefined,
      lastModified: row.lastModified ?? undefined,
      lastCheckedAt: row.lastCheckedAt ?? undefined,
    }));
  }

  async listDownloadedRawFiles(limit: number): Promise<RawFileItem[]> {
    const rows = this.db
      .prepare(
        `
        SELECT docId, rawLocation
        FROM documents
        WHERE rawLocation IS NOT NULL
          AND lastStatus IN ('downloaded_ok', 'extracted_ok')
        ORDER BY updatedAt ASC
        LIMIT ?
      `,
      )
      .all(limit) as RawFileRow[];

    return rows.map((row) => ({
      docId: row.docId,
      rawLocation: row.rawLocation,
    }));
  }

  async markRemoteUnchanged(docId: string, checkedAt: string, etag?: string, lastModified?: string): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE documents
        SET
          etag = COALESCE(@etag, etag),
          lastModified = COALESCE(@lastModified, lastModified),
          lastCheckedAt = @checkedAt,
          updatedAt = @checkedAt
        WHERE docId = @docId
      `,
      )
      .run({
        docId,
        checkedAt,
        etag: etag ?? null,
        lastModified: lastModified ?? null,
      });
  }

  async markRemoteChanged(docId: string, checkedAt: string, etag?: string, lastModified?: string): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE documents
        SET
          etag = COALESCE(@etag, etag),
          lastModified = COALESCE(@lastModified, lastModified),
          lastCheckedAt = @checkedAt,
          lastStatus = 'discovered',
          error = 'remote_changed',
          updatedAt = @checkedAt
        WHERE docId = @docId
      `,
      )
      .run({
        docId,
        checkedAt,
        etag: etag ?? null,
        lastModified: lastModified ?? null,
      });
  }

  async markRawMissing(docId: string, checkedAt: string): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE documents
        SET
          lastStatus = 'discovered',
          rawLocation = NULL,
          sha256 = NULL,
          bytes = NULL,
          contentType = NULL,
          error = 'missing_local_file',
          updatedAt = @checkedAt
        WHERE docId = @docId
      `,
      )
      .run({
        docId,
        checkedAt,
      });
  }

  async enqueueExtractJob(job: ExtractJobRecord): Promise<void> {
    this.db
      .prepare(
        `
        INSERT INTO extract_jobs (
          jobId, runId, docId, rawPdfPath, fileSha256, attempt, submittedAt, status, leaseUntil
        )
        VALUES (
          @jobId, @runId, @docId, @rawPdfPath, @fileSha256, @attempt, @submittedAt, 'queued', NULL
        )
        ON CONFLICT(jobId) DO NOTHING
      `,
      )
      .run({
        jobId: job.jobId,
        runId: job.runId,
        docId: job.docId,
        rawPdfPath: job.rawPdfPath,
        fileSha256: job.fileSha256 ?? null,
        attempt: job.attempt,
        submittedAt: job.submittedAt,
      });
  }

  async claimPendingExtractJobs(limit: number, leaseUntil: string): Promise<ClaimedExtractJob[]> {
    const rows = this.db
      .prepare(
        `
        SELECT jobId, runId, docId, rawPdfPath, fileSha256, attempt, submittedAt
        FROM extract_jobs
        WHERE status = 'queued'
          OR (status = 'leased' AND leaseUntil IS NOT NULL AND leaseUntil < @now)
        ORDER BY submittedAt ASC
        LIMIT @limit
      `,
      )
      .all({ limit, now: new Date().toISOString() }) as Array<Omit<ExtractJobRow, "leaseUntil">>;

    if (rows.length === 0) {
      return [];
    }

    const updateStmt = this.db.prepare(
      `
      UPDATE extract_jobs
      SET status = 'leased', leaseUntil = @leaseUntil
      WHERE jobId = @jobId
    `,
    );
    const tx = this.db.transaction((jobRows: Array<Omit<ExtractJobRow, "leaseUntil">>) => {
      for (const row of jobRows) {
        updateStmt.run({ jobId: row.jobId, leaseUntil });
      }
    });
    tx(rows);

    return rows.map((row) => ({
      jobId: row.jobId,
      runId: row.runId,
      docId: row.docId,
      rawPdfPath: row.rawPdfPath,
      fileSha256: row.fileSha256 ?? undefined,
      attempt: row.attempt,
      submittedAt: row.submittedAt,
      leaseUntil,
    }));
  }

  async getExtractJob(jobId: string): Promise<ExtractJobStatusRecord | undefined> {
    const row = this.db
      .prepare(
        `
        SELECT jobId, runId, docId, rawPdfPath, fileSha256, attempt, submittedAt, status, leaseUntil, finishedAt, error, resultRef
        FROM extract_jobs
        WHERE jobId = ?
      `,
      )
      .get(jobId) as ExtractJobRow | undefined;

    if (!row) {
      return undefined;
    }

    return {
      jobId: row.jobId,
      runId: row.runId,
      docId: row.docId,
      rawPdfPath: row.rawPdfPath,
      fileSha256: row.fileSha256 ?? undefined,
      attempt: row.attempt,
      submittedAt: row.submittedAt,
      status: row.status as ExtractJobStatusRecord["status"],
      leaseUntil: row.leaseUntil ?? undefined,
      finishedAt: row.finishedAt ?? undefined,
      error: row.error ?? undefined,
      resultRef: row.resultRef ?? undefined,
    };
  }

  async completeExtractJob(result: {
    jobId: string;
    status: "completed" | "failed";
    finishedAt: string;
    error?: string;
    resultRef?: string;
  }): Promise<void> {
    this.db
      .prepare(
        `
        UPDATE extract_jobs
        SET
          status = @status,
          finishedAt = @finishedAt,
          error = @error,
          resultRef = @resultRef
        WHERE jobId = @jobId
      `,
      )
      .run({
        jobId: result.jobId,
        status: result.status,
        finishedAt: result.finishedAt,
        error: result.error ?? null,
        resultRef: result.resultRef ?? null,
      });
  }

  async markDownloadResult(result: DownloadResult): Promise<void> {
    const statement = this.db.prepare(`
      UPDATE documents
      SET
        url = @url,
        sha256 = COALESCE(@sha256, sha256),
        bytes = COALESCE(@bytes, bytes),
        contentType = COALESCE(@contentType, contentType),
        rawLocation = COALESCE(@rawLocation, rawLocation),
        etag = COALESCE(@etag, etag),
        lastModified = COALESCE(@lastModified, lastModified),
        lastCheckedAt = @downloadedAt,
        lastDownloadAt = CASE
          WHEN @status = 'downloaded_ok' THEN @downloadedAt
          ELSE lastDownloadAt
        END,
        error = @error,
        attemptsDownload = CASE
          WHEN @attempt > attemptsDownload THEN @attempt
          ELSE attemptsDownload + 1
        END,
        lastStatus = @status,
        updatedAt = @updatedAt
      WHERE docId = @docId
    `);

    statement.run({
      docId: result.docId,
      url: result.url,
      sha256: result.sha256 ?? null,
      bytes: result.bytes ?? null,
      contentType: result.contentType ?? null,
      rawLocation: result.rawLocation ?? null,
      etag: result.etag ?? null,
      lastModified: result.lastModified ?? null,
      error: result.error ?? null,
      attempt: result.attempt,
      status: result.status === "downloaded_ok" ? "downloaded_ok" : result.status === "skipped" ? "downloaded_ok" : "download_failed",
      downloadedAt: result.downloadedAt,
      updatedAt: result.downloadedAt,
    });
  }

  async listPendingExtracts(limit: number, force = false, maxAttempts = 3): Promise<ExtractWorkItem[]> {
    if (force) {
      const forceRows = this.db
        .prepare(
          `
          SELECT docId, rawLocation, attemptsExtract
          FROM documents
          WHERE rawLocation IS NOT NULL
          ORDER BY updatedAt ASC
          LIMIT ?
        `,
        )
        .all(limit) as ExtractRow[];

      return forceRows.map((row) => ({
        docId: row.docId,
        rawLocation: row.rawLocation,
        attempt: row.attemptsExtract + 1,
      }));
    }

    const rows = this.db
      .prepare(
        `
        SELECT docId, rawLocation, attemptsExtract
        FROM documents
        WHERE
          rawLocation IS NOT NULL
          AND attemptsExtract < ?
          AND lastStatus IN ('downloaded_ok', 'extracted_failed')
          AND lastStatus != 'extracted_ok'
        ORDER BY updatedAt ASC
        LIMIT ?
      `,
      )
      .all(maxAttempts, limit) as ExtractRow[];

    return rows.map((row) => ({
      docId: row.docId,
      rawLocation: row.rawLocation,
      attempt: row.attemptsExtract + 1,
    }));
  }

  async markExtractionResult(result: ExtractionResult): Promise<void> {
    const statement = this.db.prepare(`
      UPDATE documents
      SET
        extractedTextLocation = COALESCE(@textLocation, extractedTextLocation),
        extractedTablesLocation = COALESCE(@tablesLocation, extractedTablesLocation),
        error = @error,
        attemptsExtract = CASE
          WHEN @attempt > attemptsExtract THEN @attempt
          ELSE attemptsExtract + 1
        END,
        lastStatus = @status,
        updatedAt = @updatedAt
      WHERE docId = @docId
    `);

    statement.run({
      docId: result.docId,
      textLocation: result.textLocation ?? null,
      tablesLocation: result.tablesLocation ?? null,
      error: result.error ?? null,
      attempt: result.attempt,
      status: result.status === "extracted_ok" ? "extracted_ok" : result.status === "skipped" ? "extracted_ok" : "extracted_failed",
      updatedAt: result.extractedAt,
    });
  }

  async getStats(): Promise<StoreStats> {
    const totalDocuments = this.countWhere("1 = 1");
    const discovered = this.countWhere("lastStatus = 'discovered'");
    const downloadedOk = this.countWhere("lastStatus = 'downloaded_ok'");
    const downloadedFailed = this.countWhere("lastStatus = 'download_failed'");
    const extractedOk = this.countWhere("lastStatus = 'extracted_ok'");
    const extractedFailed = this.countWhere("lastStatus = 'extracted_failed'");

    return {
      totalDocuments,
      discovered,
      downloadedOk,
      downloadedFailed,
      extractedOk,
      extractedFailed,
    };
  }

  async close(): Promise<void> {
    this.db.close();
  }

  private countWhere(whereClause: string): number {
    const row = this.db.prepare(`SELECT COUNT(*) as count FROM documents WHERE ${whereClause}`).get() as { count: number };
    return row.count;
  }

  private initializeSchema(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS documents (
        docId TEXT PRIMARY KEY,
        url TEXT NOT NULL,
        title TEXT NOT NULL,
        year INTEGER NULL,
        month TEXT NULL,
        sourcePageUrl TEXT NOT NULL,
        firstSeenAt TEXT NOT NULL,
        lastSeenAt TEXT NOT NULL,
        lastStatus TEXT,
        sha256 TEXT NULL,
        bytes INTEGER NULL,
        contentType TEXT NULL,
        rawLocation TEXT NULL,
        extractedTextLocation TEXT NULL,
        extractedTablesLocation TEXT NULL,
        error TEXT NULL,
        etag TEXT NULL,
        lastModified TEXT NULL,
        lastCheckedAt TEXT NULL,
        lastDownloadAt TEXT NULL,
        attemptsDownload INTEGER NOT NULL DEFAULT 0,
        attemptsExtract INTEGER NOT NULL DEFAULT 0,
        updatedAt TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS runs (
        runId TEXT PRIMARY KEY,
        startedAt TEXT NOT NULL,
        finishedAt TEXT NULL,
        status TEXT NOT NULL
      );

      CREATE TABLE IF NOT EXISTS extract_jobs (
        jobId TEXT PRIMARY KEY,
        runId TEXT NOT NULL,
        docId TEXT NOT NULL,
        rawPdfPath TEXT NOT NULL,
        fileSha256 TEXT NULL,
        attempt INTEGER NOT NULL,
        submittedAt TEXT NOT NULL,
        status TEXT NOT NULL,
        leaseUntil TEXT NULL,
        finishedAt TEXT NULL,
        error TEXT NULL,
        resultRef TEXT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_documents_status ON documents(lastStatus);
      CREATE INDEX IF NOT EXISTS idx_documents_updated_at ON documents(updatedAt);
      CREATE INDEX IF NOT EXISTS idx_extract_jobs_status ON extract_jobs(status);
      CREATE INDEX IF NOT EXISTS idx_extract_jobs_doc ON extract_jobs(docId);
    `);

    this.ensureColumn("documents", "etag", "TEXT NULL");
    this.ensureColumn("documents", "lastModified", "TEXT NULL");
    this.ensureColumn("documents", "lastCheckedAt", "TEXT NULL");
    this.ensureColumn("documents", "lastDownloadAt", "TEXT NULL");
  }

  private ensureColumn(tableName: string, columnName: string, definition: string): void {
    const columns = this.db.prepare(`PRAGMA table_info(${tableName})`).all() as Array<{ name: string }>;
    if (columns.some((column) => column.name === columnName)) {
      return;
    }

    this.db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }
}
