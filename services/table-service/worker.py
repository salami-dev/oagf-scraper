#!/usr/bin/env python3
import hashlib
import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


QUEUE_DB_PATH = os.getenv("ASYNC_QUEUE_PATH", "data/async-queue.sqlite")
OUTPUT_DIR = os.getenv("OUTPUT_EXTRACTED_DIR", "data/extracted")
POLL_INTERVAL_MS = int(os.getenv("TABLE_WORKER_POLL_INTERVAL_MS", "1000"))
MAX_BATCH = int(os.getenv("TABLE_WORKER_MAX_BATCH", "5"))


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def ensure_output_dir() -> None:
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(QUEUE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def lease_requests(conn: sqlite3.Connection, limit: int) -> List[sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT queueMessageId, payload
        FROM queue_requests
        WHERE status='pending'
        ORDER BY createdAt ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    if not rows:
        return []

    for row in rows:
        conn.execute(
            "UPDATE queue_requests SET status='leased' WHERE queueMessageId=?",
            (row["queueMessageId"],),
        )
    conn.commit()
    return rows


def ack_request(conn: sqlite3.Connection, queue_message_id: str) -> None:
    conn.execute("DELETE FROM queue_requests WHERE queueMessageId=?", (queue_message_id,))
    conn.commit()


def publish_result(conn: sqlite3.Connection, payload: Dict[str, Any]) -> None:
    queue_message_id = hashlib.sha256(
        f"{payload.get('jobId','')}|{payload.get('docId','')}|{utc_now_iso()}".encode("utf-8")
    ).hexdigest()[:24]
    conn.execute(
        """
        INSERT INTO queue_results(queueMessageId, payload, status, createdAt)
        VALUES (?, ?, 'pending', ?)
        """,
        (queue_message_id, json.dumps(payload), utc_now_iso()),
    )
    conn.commit()


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
    page_payloads: List[Dict[str, Any]] = []
    table_count = 0

    try:
        with pdfplumber.open(raw_pdf_path) as pdf:
            for i, page in enumerate(pdf.pages, start=1):
                tables = page.extract_tables() or []
                normalized = []
                for idx, rows in enumerate(tables):
                    normalized.append(
                        {
                            "index": idx,
                            "rowCount": len(rows),
                            "rows": rows,
                        }
                    )
                table_count += len(tables)
                page_payloads.append(
                    {
                        "pageNumber": i,
                        "tableCount": len(tables),
                        "tables": normalized,
                    }
                )

        payload = {
            "docId": doc_id,
            "status": "ok",
            "tableCount": table_count,
            "pageCount": len(page_payloads),
            "pages": page_payloads,
            "engine": "pdfplumber",
            "extractedAt": utc_now_iso(),
        }
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


def process_once(conn: sqlite3.Connection) -> int:
    requests = lease_requests(conn, MAX_BATCH)
    if not requests:
        return 0

    for row in requests:
        queue_message_id = row["queueMessageId"]
        try:
            message = json.loads(row["payload"])
            job_id = message["jobId"]
            doc_id = message["docId"]
            raw_pdf_path = message["rawPdfPath"]
            started = time.time()
            extracted = run_table_extraction(doc_id, raw_pdf_path)
            duration_ms = int((time.time() - started) * 1000)
            publish_result(
                conn,
                {
                    "version": "v1",
                    "type": "tables.extract.result",
                    "jobId": job_id,
                    "docId": doc_id,
                    "status": extracted["status"],
                    "tablesLocation": extracted["tablesLocation"],
                    "tableCount": extracted["tableCount"],
                    "engine": extracted["engine"],
                    "errorCode": extracted.get("errorCode"),
                    "error": extracted["error"],
                    "durationMs": duration_ms,
                    "finishedAt": utc_now_iso(),
                },
            )
        except Exception as exc:
            publish_result(
                conn,
                {
                    "version": "v1",
                    "type": "tables.extract.result",
                    "jobId": "unknown",
                    "docId": "unknown",
                    "status": "failed",
                    "errorCode": "worker_exception",
                    "error": f"worker_exception: {exc}",
                    "finishedAt": utc_now_iso(),
                },
            )
        finally:
            ack_request(conn, queue_message_id)
    return len(requests)


def main() -> None:
    ensure_output_dir()
    try:
        import pdfplumber  # noqa: F401
    except Exception as exc:
        raise RuntimeError(
            "pdfplumber is required for table worker. Install with: pip install -r services/table-service/requirements.txt"
        ) from exc
    conn = connect_db()
    print(
        json.dumps(
            {
                "msg": "table_worker_started",
                "queueDbPath": QUEUE_DB_PATH,
                "outputDir": OUTPUT_DIR,
                "pollIntervalMs": POLL_INTERVAL_MS,
                "maxBatch": MAX_BATCH,
            }
        )
    )

    try:
        while True:
            processed = process_once(conn)
            if processed == 0:
                time.sleep(POLL_INTERVAL_MS / 1000.0)
    except KeyboardInterrupt:
        print(json.dumps({"msg": "table_worker_stopped"}))
    finally:
        conn.close()


if __name__ == "__main__":
    main()
