import { AppConfig } from "../config";
import { crawlFaacPages } from "../crawl";
import { runDownloader } from "../download/downloader";
import { runExtractor } from "../extract";
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

export async function runCrawl(ctx: CommandContext, dryRun: boolean): Promise<void> {
  ctx.logger.info("crawl_start", { pageUrl: ctx.config.baseUrl, mode: dryRun ? "dry-run" : "normal" });
  const discoveredCount = await crawlFaacPages(
    {
      config: ctx.config,
      logger: ctx.logger,
      metrics: ctx.metrics,
      store: ctx.store,
      sink: ctx.sink,
    },
    { dryRun },
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

export async function runPipeline(ctx: CommandContext, maxDocs?: number): Promise<void> {
  const startedAt = new Date().toISOString();
  await ctx.store.startRun(ctx.runId, startedAt);
  ctx.logger.info("pipeline_start", { maxDocs });

  try {
    await runCrawl(ctx, false);
    await runDownload(ctx, false, maxDocs);
    await runExtract(ctx, false, maxDocs);
    await ctx.store.finishRun(ctx.runId, "completed", new Date().toISOString());
    ctx.logger.info("pipeline_complete", { maxDocs });
  } catch (error) {
    await ctx.store.finishRun(ctx.runId, "failed", new Date().toISOString());
    throw error;
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
