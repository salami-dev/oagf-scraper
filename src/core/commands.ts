import { spawn, ChildProcess } from "node:child_process";
import { URL } from "node:url";
import { AppConfig } from "../config";
import { getFetchDispatcher } from "./fetch";
import { crawlFaacPages } from "../crawl";
import { runDownloader } from "../download/downloader";
import { collectExtractResults, createAsyncQueue, runExtractor, submitExtractJobs } from "../extract";
import { PipelineStore } from "../store";
import { Logger, MetricsRegistry } from "../observability";
import { Sink } from "../sink";

export interface CommandContext {
  runId: string;
  config: AppConfig;
  store: PipelineStore;
  logger: Logger;
  metrics: MetricsRegistry;
  sink: Sink;
}

export async function runCrawl(ctx: CommandContext, dryRun: boolean, maxDocs?: number): Promise<void> {
  ctx.logger.info("crawl_start", { pageUrl: ctx.config.baseUrl, mode: dryRun ? "dry-run" : "normal", maxDocs });
  const discoveredCount = await crawlFaacPages(
    {
      config: ctx.config,
      logger: ctx.logger,
      metrics: ctx.metrics,
      store: ctx.store,
      sink: ctx.sink,
    },
    { dryRun, maxDocs },
  );
  ctx.logger.info("crawl_complete", { discoveredCount });
}

export async function runDownload(ctx: CommandContext, force = false, maxDocs?: number): Promise<void> {
  ctx.logger.info("download_start", { force, maxDocs });
  const summary = await runDownloader({
    config: ctx.config,
    logger: ctx.logger,
    metrics: ctx.metrics,
    store: ctx.store,
    sink: ctx.sink,
    force,
    maxDocs,
  });
  ctx.logger.info("download_complete", { ...summary });
}

export async function runExtract(ctx: CommandContext, force = false, maxDocs?: number): Promise<void> {
  ctx.logger.info("extract_start", { force, maxDocs });
  const summary = await runExtractor({
    config: ctx.config,
    logger: ctx.logger,
    metrics: ctx.metrics,
    store: ctx.store,
    sink: ctx.sink,
    force,
    maxDocs,
  });
  ctx.logger.info("extract_complete", { ...summary });
}

export async function runExtractSubmit(ctx: CommandContext, force = false, maxDocs?: number): Promise<void> {
  const queue = createAsyncQueue(ctx.config);
  try {
    const summary = await submitExtractJobs({
      runId: ctx.runId,
      store: ctx.store,
      queue,
      logger: ctx.logger,
      force,
      limit: maxDocs ?? 100,
      maxExtractAttempts: ctx.config.maxExtractAttempts,
    });
    ctx.logger.info("extract_submit_command_complete", { ...summary });
  } finally {
    await queue.close();
  }
}

export async function runExtractCollect(ctx: CommandContext, maxDocs?: number): Promise<void> {
  const queue = createAsyncQueue(ctx.config);
  try {
    const summary = await collectExtractResults({
      store: ctx.store,
      queue,
      logger: ctx.logger,
      limit: maxDocs ?? 100,
    });
    ctx.logger.info("extract_collect_command_complete", { ...summary });
  } finally {
    await queue.close();
  }
}

export async function runPipeline(ctx: CommandContext, maxDocs?: number): Promise<void> {
  const startedAt = new Date().toISOString();
  await ctx.store.startRun(ctx.runId, startedAt);
  ctx.logger.info("pipeline_start", { maxDocs });

  try {
    await runCrawl(ctx, false, maxDocs);
    await runDownload(ctx, false, maxDocs);
    await runExtract(ctx, false, maxDocs);
    await ctx.store.finishRun(ctx.runId, "completed", new Date().toISOString());
    ctx.logger.info("pipeline_complete", { maxDocs });
  } catch (error) {
    await ctx.store.finishRun(ctx.runId, "failed", new Date().toISOString());
    throw error;
  }
}

export async function runAsyncPipeline(ctx: CommandContext, maxDocs?: number): Promise<void> {
  const startedAt = new Date().toISOString();
  await ctx.store.startRun(ctx.runId, startedAt);
  ctx.logger.info("pipeline_async_start", { maxDocs });

  const maxCollectRounds = 300;
  const maxIdleRounds = Number.isFinite(Number.parseInt(process.env.ASYNC_COLLECT_MAX_IDLE_ROUNDS ?? "", 10))
    ? Number.parseInt(process.env.ASYNC_COLLECT_MAX_IDLE_ROUNDS ?? "120", 10)
    : 120;
  const collectPollIntervalMs = Number.isFinite(Number.parseInt(process.env.ASYNC_COLLECT_POLL_INTERVAL_MS ?? "", 10))
    ? Number.parseInt(process.env.ASYNC_COLLECT_POLL_INTERVAL_MS ?? "1000", 10)
    : 1_000;

  try {
    await runCrawl(ctx, false, maxDocs);
    await runDownload(ctx, false, maxDocs);

    const queue = createAsyncQueue(ctx.config);
    try {
      const submitSummary = await submitExtractJobs({
        runId: ctx.runId,
        store: ctx.store,
        queue,
        logger: ctx.logger,
        force: false,
        limit: maxDocs ?? 100,
        maxExtractAttempts: ctx.config.maxExtractAttempts,
      });
      ctx.logger.info("pipeline_async_submit_complete", { ...submitSummary });
      const targetResults = submitSummary.submitted;
      if (targetResults === 0) {
        ctx.logger.info("pipeline_async_collect_complete", {
          processedTotal: 0,
          okTotal: 0,
          failedTotal: 0,
        });
      } else {
        let processedTotal = 0;
        let okTotal = 0;
        let failedTotal = 0;
        let idleRounds = 0;

        for (let round = 1; round <= maxCollectRounds; round += 1) {
          const pendingByTarget = Math.max(targetResults - processedTotal, 0);
          const remaining = maxDocs !== undefined ? Math.max(maxDocs - processedTotal, 0) : Math.max(100, pendingByTarget);
          if (remaining === 0) {
            break;
          }

          const collectSummary = await collectExtractResults({
            store: ctx.store,
            queue,
            logger: ctx.logger,
            limit: Math.max(1, remaining),
          });

          processedTotal += collectSummary.processed;
          okTotal += collectSummary.ok;
          failedTotal += collectSummary.failed;

          if (collectSummary.processed === 0) {
            idleRounds += 1;
            if (idleRounds >= maxIdleRounds) {
              break;
            }
            await sleep(collectPollIntervalMs);
            continue;
          }

          if (processedTotal >= targetResults) {
            break;
          }
          idleRounds = 0;
        }

        ctx.logger.info("pipeline_async_collect_complete", {
          processedTotal,
          okTotal,
          failedTotal,
        });
      }
    } finally {
      await queue.close();
    }

    await ctx.store.finishRun(ctx.runId, "completed", new Date().toISOString());
    ctx.logger.info("pipeline_async_complete", { maxDocs });
  } catch (error) {
    await ctx.store.finishRun(ctx.runId, "failed", new Date().toISOString());
    throw error;
  }
}

export async function runAsyncPipelineSupervised(ctx: CommandContext, maxDocs?: number): Promise<void> {
  if (ctx.config.asyncQueueMode !== "http") {
    throw new Error("run-async-supervised requires asyncQueueMode=http");
  }

  const supervisor = await ensureTableService(ctx);
  try {
    await runAsyncPipeline(ctx, maxDocs);
  } finally {
    await supervisor.stop();
  }
}

export async function runStatus(ctx: CommandContext): Promise<void> {
  ctx.logger.info("status_start");
  const stats = await ctx.store.getStats();
  ctx.logger.info("status_complete", { stats });
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function runPoll(ctx: CommandContext, maxDocs?: number, iterations?: number): Promise<void> {
  let runCount = 0;
  ctx.logger.info("poll_start", {
    maxDocs,
    iterations: iterations ?? "infinite",
    pollIntervalMinutes: ctx.config.pollIntervalMinutes,
  });

  while (iterations === undefined || runCount < iterations) {
    runCount += 1;
    const iterationRunId = `${ctx.runId}_poll_${runCount}`;
    const iterationContext: CommandContext = {
      ...ctx,
      runId: iterationRunId,
      logger: ctx.logger.child("poll_iteration"),
    };

    try {
      await runPipeline(iterationContext, maxDocs);
    } catch (error) {
      iterationContext.logger.error("poll_iteration_failed", {
        iteration: runCount,
        error: error instanceof Error ? error.message : String(error),
      });
    }

    if (iterations !== undefined && runCount >= iterations) {
      break;
    }

    await sleep(Math.max(1, ctx.config.pollIntervalMinutes) * 60 * 1000);
  }

  ctx.logger.info("poll_complete", { iterationsExecuted: runCount });
}

interface ServiceSupervisor {
  stop(): Promise<void>;
}

async function ensureTableService(ctx: CommandContext): Promise<ServiceSupervisor> {
  const alreadyHealthy = await isTableServiceHealthy(ctx.config);
  if (alreadyHealthy) {
    ctx.logger.info("table_service_healthcheck_ok", { baseUrl: ctx.config.asyncQueueHttpBaseUrl, launched: false });
    return { stop: async () => Promise.resolve() };
  }

  const target = new URL(ctx.config.asyncQueueHttpBaseUrl);
  const pythonCmd = process.env.TABLE_SERVICE_PYTHON ?? "python";
  const proc = spawn(
    pythonCmd,
    ["-m", "uvicorn", "app:app", "--app-dir", "services/table-service", "--host", target.hostname, "--port", target.port || "80"],
    {
      stdio: "pipe",
      env: process.env,
    },
  );

  proc.stdout?.on("data", (chunk) => {
    ctx.logger.info("table_service_stdout", { data: String(chunk).trim() });
  });
  proc.stderr?.on("data", (chunk) => {
    ctx.logger.warn("table_service_stderr", { data: String(chunk).trim() });
  });

  const timeoutMs = 30_000;
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const healthy = await isTableServiceHealthy(ctx.config);
    if (healthy) {
      ctx.logger.info("table_service_healthcheck_ok", { baseUrl: ctx.config.asyncQueueHttpBaseUrl, launched: true });
      return {
        stop: async () => stopChild(proc),
      };
    }
    if (proc.exitCode !== null) {
      throw new Error(`table service exited early with code ${proc.exitCode}`);
    }
    await sleep(500);
  }

  await stopChild(proc);
  throw new Error("table service failed healthcheck after startup timeout");
}

async function isTableServiceHealthy(config: AppConfig): Promise<boolean> {
  try {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 2_000);
    const response = await fetch(`${config.asyncQueueHttpBaseUrl.replace(/\/+$/, "")}/health`, {
      method: "GET",
      dispatcher: getFetchDispatcher(config.ignoreHttpsErrors),
      signal: controller.signal,
    } as any);
    clearTimeout(timeout);
    if (!response.ok) {
      return false;
    }
    const body = (await response.json()) as { status?: string };
    return body.status === "ok";
  } catch {
    return false;
  }
}

async function stopChild(proc: ChildProcess): Promise<void> {
  if (proc.exitCode !== null) {
    return;
  }
  proc.kill();
  await new Promise<void>((resolve) => {
    const timer = setTimeout(() => {
      try {
        proc.kill("SIGKILL");
      } catch {
        // no-op
      }
      resolve();
    }, 3_000);
    proc.once("exit", () => {
      clearTimeout(timer);
      resolve();
    });
  });
}
