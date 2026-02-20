#!/usr/bin/env python3
import asyncio
import hashlib
import json
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field


QUEUE_DB_PATH = os.getenv("ASYNC_QUEUE_PATH", "data/async-queue.sqlite")
OUTPUT_DIR = os.getenv("OUTPUT_EXTRACTED_DIR", "data/extracted")
SERVICE_TOKEN = os.getenv("TABLE_SERVICE_TOKEN")
WORKER_ENABLED = os.getenv("TABLE_SERVICE_ENABLE_WORKER", "1").lower() in {"1", "true", "yes"}
WORKER_CONCURRENCY = max(1, int(os.getenv("TABLE_WORKER_CONCURRENCY", "2")))
WORKER_BATCH = max(1, int(os.getenv("TABLE_WORKER_MAX_BATCH", "5")))
WORKER_POLL_MS = max(100, int(os.getenv("TABLE_WORKER_POLL_INTERVAL_MS", "1000")))
LEASE_SECONDS = max(30, int(os.getenv("TABLE_WORKER_LEASE_SECONDS", "120")))
ENABLE_OCR_FALLBACK = os.getenv("TABLE_WORKER_ENABLE_OCR_FALLBACK", "0").lower() in {"1", "true", "yes"}


class QueueEnvelope(BaseModel):
  queueMessageId: str
  payload: Dict[str, Any]


class PublishMessagesRequest(BaseModel):
  messages: List[Dict[str, Any]] = Field(default_factory=list)


class LeaseRequest(BaseModel):
  limit: int = 10


class AckRequest(BaseModel):
  ids: List[str] = Field(default_factory=list)


class LeaseResponse(BaseModel):
  messages: List[QueueEnvelope] = Field(default_factory=list)


def utc_now_iso() -> str:
  return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_pdfplumber_available() -> None:
  try:
    import pdfplumber  # noqa: F401
  except Exception as exc:  # pragma: no cover - startup guard
    raise RuntimeError(
      "pdfplumber is required for table worker. Install with: pip install -r services/table-service/requirements.txt"
    ) from exc


def compute_queue_message_id(payload: Dict[str, Any]) -> str:
  material = f"{payload.get('jobId', '')}|{payload.get('docId', '')}|{utc_now_iso()}".encode("utf-8")
  return hashlib.sha256(material).hexdigest()[:24]


def require_token(authorization: Optional[str]) -> None:
  if not SERVICE_TOKEN:
    return
  expected = f"Bearer {SERVICE_TOKEN}"
  if authorization != expected:
    raise HTTPException(status_code=401, detail="unauthorized")


class SqliteQueueStore:
  def __init__(self, db_path: str):
    self.db_path = db_path
    self.lock = threading.Lock()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    self._initialize_schema()

  def _connect(self) -> sqlite3.Connection:
    conn = sqlite3.connect(self.db_path)
    conn.row_factory = sqlite3.Row
    return conn

  def _initialize_schema(self) -> None:
    with self.lock:
      conn = self._connect()
      try:
        conn.executescript(
          """
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

          CREATE INDEX IF NOT EXISTS idx_queue_requests_status ON queue_requests(status, createdAt);
          CREATE INDEX IF NOT EXISTS idx_queue_results_status ON queue_results(status, createdAt);
          """
        )
        self._ensure_column(conn, "queue_requests", "leaseUntil", "TEXT NULL")
        self._ensure_column(conn, "queue_results", "leaseUntil", "TEXT NULL")
        conn.commit()
      finally:
        conn.close()

  def _ensure_column(self, conn: sqlite3.Connection, table_name: str, column_name: str, column_ddl: str) -> None:
    columns = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    column_names = {row["name"] for row in columns}
    if column_name not in column_names:
      conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_ddl}")

  def publish(self, table_name: str, payloads: List[Dict[str, Any]]) -> int:
    if not payloads:
      return 0

    now = utc_now_iso()
    with self.lock:
      conn = self._connect()
      try:
        for payload in payloads:
          conn.execute(
            f"""
            INSERT INTO {table_name}(queueMessageId, payload, status, createdAt, leaseUntil)
            VALUES (?, ?, 'pending', ?, NULL)
            """,
            (compute_queue_message_id(payload), json.dumps(payload), now),
          )
        conn.commit()
      finally:
        conn.close()
    return len(payloads)

  def lease(self, table_name: str, limit: int, lease_seconds: int) -> List[Dict[str, Any]]:
    if limit <= 0:
      return []

    now = utc_now_iso()
    lease_until_ts = datetime.now(timezone.utc).timestamp() + lease_seconds
    lease_until = datetime.fromtimestamp(lease_until_ts, timezone.utc).isoformat().replace("+00:00", "Z")

    with self.lock:
      conn = self._connect()
      try:
        conn.execute(
          f"""
          UPDATE {table_name}
          SET status='pending', leaseUntil=NULL
          WHERE status='leased' AND leaseUntil IS NOT NULL AND leaseUntil < ?
          """,
          (now,),
        )
        rows = conn.execute(
          f"""
          SELECT queueMessageId, payload
          FROM {table_name}
          WHERE status='pending'
          ORDER BY createdAt ASC
          LIMIT ?
          """,
          (limit,),
        ).fetchall()

        if not rows:
          conn.commit()
          return []

        queue_ids = [row["queueMessageId"] for row in rows]
        conn.executemany(
          f"UPDATE {table_name} SET status='leased', leaseUntil=? WHERE queueMessageId=?",
          [(lease_until, queue_id) for queue_id in queue_ids],
        )
        conn.commit()

        envelopes = []
        for row in rows:
          envelopes.append(
            {
              "queueMessageId": row["queueMessageId"],
              "payload": json.loads(row["payload"]),
            }
          )
        return envelopes
      finally:
        conn.close()

  def ack(self, table_name: str, queue_ids: List[str]) -> int:
    if not queue_ids:
      return 0

    with self.lock:
      conn = self._connect()
      try:
        conn.executemany(
          f"DELETE FROM {table_name} WHERE queueMessageId=?",
          [(queue_id,) for queue_id in queue_ids],
        )
        conn.commit()
      finally:
        conn.close()
    return len(queue_ids)


def run_table_extraction(doc_id: str, raw_pdf_path: str) -> Dict[str, Any]:
  if not raw_pdf_path or not Path(raw_pdf_path).exists():
    return {
      "status": "failed",
      "errorCode": "file_not_found",
      "error": f"raw pdf not found: {raw_pdf_path}",
      "tableCount": 0,
      "tablesLocation": None,
      "engine": "pdfplumber",
    }

  try:
    import pdfplumber  # type: ignore
  except Exception:
    return {
      "status": "failed",
      "errorCode": "dependency_missing",
      "error": "pdfplumber_not_installed",
      "tableCount": 0,
      "tablesLocation": None,
      "engine": "pdfplumber",
    }

  output_path = str(Path(OUTPUT_DIR) / f"{doc_id}.tables.json")
  pages_payload: List[Dict[str, Any]] = []
  table_count = 0

  try:
    with pdfplumber.open(raw_pdf_path) as pdf:
      for page_index, page in enumerate(pdf.pages, start=1):
        tables = extract_tables_with_strategies(page)
        normalized = []
        for table_index, rows in enumerate(tables):
          normalized.append(
            {
              "index": table_index,
              "rowCount": len(rows),
              "rows": rows,
            }
          )
        table_count += len(tables)
        pages_payload.append(
          {
            "pageNumber": page_index,
            "tableCount": len(tables),
                "tables": normalized,
              }
            )

      if table_count == 0 and ENABLE_OCR_FALLBACK:
        # Placeholder for future OCR table extraction path.
        # Keep explicit marker for observability when OCR fallback is requested.
        return {
          "status": "no_tables",
          "errorCode": "unknown",
          "error": "ocr_fallback_requested_but_not_implemented",
          "tableCount": 0,
          "tablesLocation": None,
          "engine": "pdfplumber",
        }

    payload = {
      "docId": doc_id,
      "status": "ok",
      "tableCount": table_count,
      "pageCount": len(pages_payload),
      "pages": pages_payload,
      "engine": "pdfplumber",
      "extractedAt": utc_now_iso(),
    }
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    Path(output_path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return {
      "status": "ok" if table_count > 0 else "no_tables",
      "errorCode": None,
      "error": None,
      "tableCount": table_count,
      "tablesLocation": output_path,
      "engine": "pdfplumber",
    }
  except PermissionError as exc:
    return {
      "status": "failed",
      "errorCode": "file_access_denied",
      "error": str(exc),
      "tableCount": 0,
      "tablesLocation": None,
      "engine": "pdfplumber",
    }
  except Exception as exc:
    return {
      "status": "failed",
      "errorCode": "parse_error",
      "error": str(exc),
      "tableCount": 0,
      "tablesLocation": None,
      "engine": "pdfplumber",
    }


def extract_tables_with_strategies(page: Any) -> List[Any]:
  strategy_candidates = [
    None,
    {"vertical_strategy": "lines", "horizontal_strategy": "lines"},
    {"vertical_strategy": "text", "horizontal_strategy": "text", "snap_tolerance": 3, "join_tolerance": 3, "intersection_tolerance": 3},
  ]
  merged: List[Any] = []
  seen = set()
  for strategy in strategy_candidates:
    tables = page.extract_tables(strategy) if strategy is not None else page.extract_tables()
    for table in tables or []:
      signature = json.dumps(table, ensure_ascii=False)
      if signature in seen:
        continue
      seen.add(signature)
      merged.append(table)
  return merged


def build_result_message(request_payload: Dict[str, Any], extracted: Dict[str, Any], duration_ms: int) -> Dict[str, Any]:
  return {
    "version": "v1",
    "type": "tables.extract.result",
    "jobId": request_payload.get("jobId", "unknown"),
    "docId": request_payload.get("docId", "unknown"),
    "status": extracted.get("status", "failed"),
    "tablesLocation": extracted.get("tablesLocation"),
    "tableCount": extracted.get("tableCount"),
    "engine": extracted.get("engine"),
    "errorCode": extracted.get("errorCode"),
    "error": extracted.get("error"),
    "durationMs": duration_ms,
    "finishedAt": utc_now_iso(),
  }


class BackgroundWorker:
  def __init__(self, store: SqliteQueueStore):
    self.store = store
    self._task: Optional[asyncio.Task[Any]] = None
    self._stopped = asyncio.Event()

  async def start(self) -> None:
    self._stopped.clear()
    self._task = asyncio.create_task(self._run_loop())

  async def stop(self) -> None:
    self._stopped.set()
    if self._task:
      await self._task

  async def _run_loop(self) -> None:
    semaphore = asyncio.Semaphore(WORKER_CONCURRENCY)
    while not self._stopped.is_set():
      batch = await asyncio.to_thread(self.store.lease, "queue_requests", WORKER_BATCH, LEASE_SECONDS)
      if not batch:
        await asyncio.sleep(WORKER_POLL_MS / 1000.0)
        continue

      async def process_one(message: Dict[str, Any]) -> None:
        async with semaphore:
          queue_id = message["queueMessageId"]
          payload = message["payload"]
          started = time.time()
          extracted = await asyncio.to_thread(
            run_table_extraction,
            str(payload.get("docId", "unknown")),
            str(payload.get("rawPdfPath", "")),
          )
          duration_ms = int((time.time() - started) * 1000)
          result_payload = build_result_message(payload, extracted, duration_ms)
          await asyncio.to_thread(self.store.publish, "queue_results", [result_payload])
          await asyncio.to_thread(self.store.ack, "queue_requests", [queue_id])

      await asyncio.gather(*(process_one(item) for item in batch))


app = FastAPI(title="OSGF Table Service", version="1.0.0")
store = SqliteQueueStore(QUEUE_DB_PATH)
worker = BackgroundWorker(store)


@app.on_event("startup")
async def on_startup() -> None:
  Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
  if WORKER_ENABLED:
    ensure_pdfplumber_available()
    await worker.start()


@app.on_event("shutdown")
async def on_shutdown() -> None:
  if WORKER_ENABLED:
    await worker.stop()


@app.get("/health")
async def health() -> Dict[str, Any]:
  return {
    "status": "ok",
    "workerEnabled": WORKER_ENABLED,
    "workerConcurrency": WORKER_CONCURRENCY,
    "queueDbPath": QUEUE_DB_PATH,
    "outputDir": OUTPUT_DIR,
    "ts": utc_now_iso(),
  }


@app.post("/v1/queue/requests")
async def publish_requests(req: PublishMessagesRequest, authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
  require_token(authorization)
  accepted = await asyncio.to_thread(store.publish, "queue_requests", req.messages)
  return {"accepted": accepted}


@app.post("/v1/queue/requests/lease", response_model=LeaseResponse)
async def lease_requests(req: LeaseRequest, authorization: Optional[str] = Header(default=None)) -> LeaseResponse:
  require_token(authorization)
  messages = await asyncio.to_thread(store.lease, "queue_requests", min(max(1, req.limit), 100), LEASE_SECONDS)
  return LeaseResponse(messages=messages)


@app.post("/v1/queue/requests/ack")
async def ack_requests(req: AckRequest, authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
  require_token(authorization)
  deleted = await asyncio.to_thread(store.ack, "queue_requests", req.ids)
  return {"acked": deleted}


@app.post("/v1/queue/results")
async def publish_results(req: PublishMessagesRequest, authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
  require_token(authorization)
  accepted = await asyncio.to_thread(store.publish, "queue_results", req.messages)
  return {"accepted": accepted}


@app.post("/v1/queue/results/lease", response_model=LeaseResponse)
async def lease_results(req: LeaseRequest, authorization: Optional[str] = Header(default=None)) -> LeaseResponse:
  require_token(authorization)
  messages = await asyncio.to_thread(store.lease, "queue_results", min(max(1, req.limit), 100), LEASE_SECONDS)
  return LeaseResponse(messages=messages)


@app.post("/v1/queue/results/ack")
async def ack_results(req: AckRequest, authorization: Optional[str] = Header(default=None)) -> Dict[str, Any]:
  require_token(authorization)
  deleted = await asyncio.to_thread(store.ack, "queue_results", req.ids)
  return {"acked": deleted}
