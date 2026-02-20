import { loadConfig } from "../config";
import { runCrawl, runDownload, runExtract, runPipeline, runPoll, runStatus } from "../core/commands";
import { createRunId, Logger, MetricsRegistry } from "../observability";
import { createSink } from "../sink";
import { createStore } from "../store";

export type CommandName = "crawl" | "download" | "extract" | "run" | "poll" | "status";

export interface ParsedCliArgs {
  command: CommandName;
  dryRun: boolean;
  force: boolean;
  ignoreHttpsErrors: boolean;
  iterations?: number;
  intervalMinutes?: number;
  maxDocs?: number;
  configPath?: string;
}

const HELP_TEXT = `
Usage:
  osgf-extractor <command> [options]

Commands:
  crawl --dry-run
  download
  extract
  run
  poll
  status

Options:
  --config <path>  Optional path to JSON config file
  --dry-run        Dry-run mode (supported by crawl currently)
  --force          Force rerun for download/extract stages
  --ignore-https-errors  Ignore TLS certificate errors (use only when required)
  --max-docs <n>   Limit docs processed in run command
  --iterations <n> Limit polling iterations (poll command)
  --interval-minutes <n> Override poll interval minutes
  -h, --help       Show this help
`;

function parseCommand(raw: string | undefined): CommandName | undefined {
  if (!raw) {
    return undefined;
  }

  if (raw === "crawl" || raw === "download" || raw === "extract" || raw === "run" || raw === "poll" || raw === "status") {
    return raw;
  }

  return undefined;
}

export function parseCliArgs(argv: string[]): ParsedCliArgs | "help" {
  if (argv.includes("-h") || argv.includes("--help")) {
    return "help";
  }

  const command = parseCommand(argv[0]);
  if (!command) {
    return "help";
  }

  let configPath: string | undefined;
  const configIndex = argv.indexOf("--config");
  if (configIndex >= 0 && argv[configIndex + 1]) {
    configPath = argv[configIndex + 1];
  }

  const dryRun = argv.includes("--dry-run");
  const force = argv.includes("--force");
  const ignoreHttpsErrors = argv.includes("--ignore-https-errors");
  const maxDocsIndex = argv.indexOf("--max-docs");
  const maxDocsRaw = maxDocsIndex >= 0 ? argv[maxDocsIndex + 1] : undefined;
  const maxDocsParsed = maxDocsRaw ? Number.parseInt(maxDocsRaw, 10) : undefined;
  const maxDocs = Number.isFinite(maxDocsParsed) ? maxDocsParsed : undefined;
  const iterationsIndex = argv.indexOf("--iterations");
  const iterationsRaw = iterationsIndex >= 0 ? argv[iterationsIndex + 1] : undefined;
  const iterationsParsed = iterationsRaw ? Number.parseInt(iterationsRaw, 10) : undefined;
  const intervalIndex = argv.indexOf("--interval-minutes");
  const intervalRaw = intervalIndex >= 0 ? argv[intervalIndex + 1] : undefined;
  const intervalParsed = intervalRaw ? Number.parseInt(intervalRaw, 10) : undefined;
  return {
    command,
    dryRun,
    force,
    ignoreHttpsErrors,
    maxDocs,
    iterations: Number.isFinite(iterationsParsed) ? iterationsParsed : undefined,
    intervalMinutes: Number.isFinite(intervalParsed) ? intervalParsed : undefined,
    configPath,
  };
}

export async function runCli(argv: string[]): Promise<number> {
  const parsed = parseCliArgs(argv);
  if (parsed === "help") {
    console.log(HELP_TEXT.trim());
    return 0;
  }

  let config = loadConfig(parsed.configPath);
  if (parsed.ignoreHttpsErrors) {
    config = {
      ...config,
      ignoreHttpsErrors: true,
    };
  }
  if (parsed.intervalMinutes !== undefined) {
    config = {
      ...config,
      pollIntervalMinutes: parsed.intervalMinutes,
    };
  }
  const runId = createRunId();
  const store = createStore(config);
  const sink = createSink(config, runId);
  const metrics = new MetricsRegistry();
  const logger = new Logger({ component: "cli", runId });
  const context = { runId, config, store, sink, logger, metrics };

  logger.info("command_start", {
    command: parsed.command,
    dryRun: parsed.dryRun,
    force: parsed.force,
    ignoreHttpsErrors: config.ignoreHttpsErrors,
    maxDocs: parsed.maxDocs,
    iterations: parsed.iterations,
    intervalMinutes: config.pollIntervalMinutes,
  });

  try {
    switch (parsed.command) {
      case "crawl":
        await runCrawl({ ...context, logger: logger.child("crawl") }, parsed.dryRun);
        break;
      case "download":
        await runDownload({ ...context, logger: logger.child("download") }, parsed.force);
        break;
      case "extract":
        await runExtract({ ...context, logger: logger.child("extract") }, parsed.force);
        break;
      case "run":
        await runPipeline({ ...context, logger: logger.child("pipeline") }, parsed.maxDocs);
        break;
      case "poll":
        await runPoll({ ...context, logger: logger.child("poll") }, parsed.maxDocs, parsed.iterations);
        break;
      case "status":
        await runStatus({ ...context, logger: logger.child("status") });
        break;
      default:
        console.error(`Unsupported command: ${parsed.command}`);
        return 1;
    }

    logger.info("command_complete", { command: parsed.command });
    return 0;
  } finally {
    await store.close();
    metrics.printSummary();
  }
}

export function getHelpText(): string {
  return HELP_TEXT.trim();
}
