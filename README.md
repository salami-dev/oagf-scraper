# osgf-extractor

Production-grade TypeScript pipeline for crawling, downloading, and extracting OAGF FAAC report documents.

## Status

- Block 0 complete: bootstrap/tooling.
- Block 1 complete: types/config/CLI skeleton.
- Block 2 complete: structured logs + metrics.
- Block 3 complete: SQLite store + idempotency state model.
- Block 4 complete: HTML-first crawler + pagination + Playwright fallback path.
- Block 5 complete: pluggable sinks + local JSONL sink.
- Block 6 complete: concurrent downloader + retries + integrity metadata.
- Block 7 complete: extraction stage + table plugin placeholder.
- Block 8 complete: full-run orchestrator + run lifecycle + `--max-docs`.
- Block 9 complete: parser/store/downloader/CLI tests + fixtures.
- Block 10 complete: production hardening docs.

## Project Structure

```text
src/
  config/
  core/
  crawl/
  download/
  extract/
  sink/
  store/
  observability/
  cli/
  types/
tests/
scripts/
```

## How It Works

1. `crawl`: fetches listing HTML pages, discovers PDF URLs, resolves pagination, emits discovered work-items.
2. `download`: pulls pending docs from SQLite, downloads with retries/backoff and atomic file writes, records hash/size/content type.
3. `extract`: pulls downloaded docs, extracts text and page count, writes extracted output, records result.
4. `extract-submit`: async mode producer that enqueues extraction jobs to queue adapter (SQLite or HTTP service).
5. `extract-collect`: async mode consumer that ingests completed extraction results back into SQLite store.
6. `run`: orchestrates `crawl -> download -> extract` under a tracked `runId`.
7. `poll`: repeats pipeline execution on an interval for periodic ingestion.
8. `status`: prints aggregate document state from SQLite.

## Run Locally

```bash
npm install
npm run build
npm run start -- crawl --dry-run
npm run start -- crawl
npm run start -- download
npm run start -- extract
npm run start -- extract-submit --max-docs 25
npm run start -- extract-collect --max-docs 25
npm run start -- run --max-docs 25
npm run start -- run-async --max-docs 25 --ignore-https-errors
npm run start -- run-async-supervised --max-docs 25 --ignore-https-errors
npm run start -- poll --iterations 1 --max-docs 25
npm run start -- status
```

## Commands

- `crawl [--dry-run] [--ignore-https-errors] [--config <path>]`
- `download [--force] [--ignore-https-errors] [--config <path>]`
- `extract [--force] [--config <path>]`
- `extract-submit [--force] [--max-docs <n>] [--config <path>]`
- `extract-collect [--max-docs <n>] [--config <path>]`
- `run [--max-docs <n>] [--ignore-https-errors] [--config <path>]`
- `run-async [--max-docs <n>] [--ignore-https-errors] [--config <path>]`
- `run-async-supervised [--max-docs <n>] [--ignore-https-errors] [--config <path>]`
- `poll [--iterations <n>] [--interval-minutes <n>] [--max-docs <n>] [--ignore-https-errors] [--config <path>]`
- `status [--config <path>]`

## Config Reference

Default config values come from `src/config/loadConfig.ts` and can be overridden by JSON config file and env vars.

- `baseUrl` (`BASE_URL`): default `https://oagf.gov.ng/publications/faac-report/`
- `userAgent` (`USER_AGENT`)
- `requestTimeoutMs` (`REQUEST_TIMEOUT_MS`)
- `downloadTimeoutMs` (`DOWNLOAD_TIMEOUT_MS`)
- `ignoreHttpsErrors` (`IGNORE_HTTPS_ERRORS`)
- `pollIntervalMinutes` (`POLL_INTERVAL_MINUTES`)
- `verifyDownloadedFilesOnStartup` (`VERIFY_DOWNLOADED_FILES_ON_STARTUP`)
- `revalidateAfterDays` (`REVALIDATE_AFTER_DAYS`)
- `changeDetectionMode` (`CHANGE_DETECTION_MODE`: `none|head|conditional_get`)
- `crawlConcurrency` (`CRAWL_CONCURRENCY`)
- `downloadConcurrency` (`DOWNLOAD_CONCURRENCY`)
- `extractConcurrency` (`EXTRACT_CONCURRENCY`)
- `maxPages` (`MAX_PAGES`)
- `maxDownloadAttempts` (`MAX_DOWNLOAD_ATTEMPTS`)
- `maxExtractAttempts` (`MAX_EXTRACT_ATTEMPTS`)
- `asyncQueueMode` (`ASYNC_QUEUE_MODE`: `local_sqlite|http`)
- `storePath` (`STORE_PATH`): default `data/state.sqlite`
- `asyncQueuePath` (`ASYNC_QUEUE_PATH`): default `data/async-queue.sqlite` (used when `asyncQueueMode=local_sqlite`)
- `asyncQueueHttpBaseUrl` (`ASYNC_QUEUE_HTTP_BASE_URL`): default `http://127.0.0.1:8081` (used when `asyncQueueMode=http`)
- `asyncQueueHttpToken` (`ASYNC_QUEUE_HTTP_TOKEN`) optional bearer token for HTTP queue service
- `asyncQueueHttpTimeoutMs` (`ASYNC_QUEUE_HTTP_TIMEOUT_MS`)
- `outputDirs.raw` (`OUTPUT_RAW_DIR`): default `data/raw`
- `outputDirs.extracted` (`OUTPUT_EXTRACTED_DIR`): default `data/extracted`
- `outputDirs.manifests` (`OUTPUT_MANIFESTS_DIR`): default `data/manifests`

## Sink Adapters

Set `SINK_TYPE`:

- `local_jsonl` (default): appends JSONL manifests:
  - `data/manifests/discovered.jsonl`
  - `data/manifests/downloads.jsonl`
  - `data/manifests/extracts.jsonl`
- `sqs` (stub): needs `SQS_QUEUE_URL`
- `rabbit`: needs `RABBIT_URL`
- `http`: needs `HTTP_SINK_ENDPOINT` and optional `HTTP_SINK_TOKEN`
- `grpc` (stub): needs `GRPC_SINK_TARGET`

SQS, RabbitMQ, and HTTP adapters are implemented; gRPC remains a stub.

## Async Table Service (HTTP Mode)

Use this when table extraction is delegated to Python sidecar workers.

1. Install service dependencies:

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r services/table-service/requirements.txt
```

2. Start service:

```bash
uvicorn app:app --app-dir services/table-service --host 127.0.0.1 --port 8081
```

3. Configure Node pipeline:

```bash
set SINK_TYPE=local_jsonl
set ASYNC_QUEUE_MODE=http
set ASYNC_QUEUE_HTTP_BASE_URL=http://127.0.0.1:8081
set IGNORE_HTTPS_ERRORS=true
```

4. Submit + collect asynchronously:

```bash
npm run start -- extract-submit --max-docs 100
npm run start -- extract-collect --max-docs 100
npm run start -- run-async-supervised --max-docs 100 --ignore-https-errors
```

Service endpoints:
- `GET /health`
- `POST /v1/queue/requests`
- `POST /v1/queue/requests/lease`
- `POST /v1/queue/requests/ack`
- `POST /v1/queue/results`
- `POST /v1/queue/results/lease`
- `POST /v1/queue/results/ack`

Service env vars:
- `ASYNC_QUEUE_PATH` SQLite queue file path
- `OUTPUT_EXTRACTED_DIR` directory for `*.tables.json`
- `TABLE_SERVICE_TOKEN` optional bearer token for API auth
- `TABLE_SERVICE_ENABLE_WORKER` enable built-in background worker (`1`/`0`)
- `TABLE_WORKER_CONCURRENCY` max parallel PDF table jobs
- `TABLE_WORKER_MAX_BATCH` lease batch size
- `TABLE_WORKER_POLL_INTERVAL_MS` polling delay for worker loop
- `TABLE_WORKER_LEASE_SECONDS` lease duration before job can be reclaimed
- `TABLE_WORKER_ENABLE_OCR_FALLBACK` optional experimental fallback mode marker (`1`/`0`)
- `ASYNC_COLLECT_MAX_IDLE_ROUNDS` max empty collect polls before `run-async` exits (default `120`)
- `ASYNC_COLLECT_POLL_INTERVAL_MS` interval between async collect polls (default `1000`)

When `--max-docs` is used with `run-async`, crawling will stop once enough documents are discovered for that run.

## Observability

Structured JSON logs include:

- `ts`, `level`, `msg`, `component`, `runId`
- optional `docId`, `url`, `pageUrl`, `attempt`, plus operation-specific fields

Metrics tracked:

- counters: `pages_crawled`, `docs_discovered`, `docs_enqueued`, `downloads_ok`, `downloads_failed`, `extracts_ok`, `extracts_failed`
- timers: `page_fetch_ms`, `download_ms`, `extract_ms`

Each CLI command prints a final metrics summary payload.

## Failure Modes And Resume

- State is persisted in SQLite (`documents`, `runs`) so reruns resume from last known state.
- Idempotency safeguards:
  - downloaded docs with stored `sha256` + `rawLocation` are not re-downloaded unless `--force`
  - extracted docs with `extracted_ok` are not re-extracted unless `--force`
  - `404` download failures are treated as permanent
  - optional remote revalidation checks `ETag` / `Last-Modified` and requeues changed docs
  - optional local raw-file reconciliation requeues docs if `rawLocation` file is missing
- Crashes are resumable: rerun the same command and pending items continue.

## Extending To Other Sites

- Add a new parser/crawler module in `src/crawl/` for site-specific HTML patterns.
- Keep discovery deterministic (direct document URLs) and avoid brittle selectors.
- Reuse downloader/extractor/store/sink components unchanged.
- If JS-rendered content is unavoidable, use the Playwright fallback path as a renderer-only step.

## Security Notes

- Do not log credentials, auth headers, or secret tokens.
- Keep all file writes inside configured output directories.
- Use deterministic `docId` generation from URL hash; avoid using untrusted path fragments directly.
- Validate and sanitize externally derived strings before using them in logs or filenames.

## Testing

```bash
npm run typecheck
npm run lint
npm test
```
