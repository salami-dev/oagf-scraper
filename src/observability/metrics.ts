import { MetricCounterName, MetricTimerName } from "./types";

interface HistogramSummary {
  count: number;
  min: number;
  max: number;
  avg: number;
}

export class MetricsRegistry {
  private readonly counters = new Map<MetricCounterName, number>();
  private readonly timers = new Map<MetricTimerName, number[]>();

  incrementCounter(name: MetricCounterName, value = 1): void {
    this.counters.set(name, (this.counters.get(name) ?? 0) + value);
  }

  startTimer(name: MetricTimerName): () => number {
    const startedAt = Date.now();
    return () => {
      const durationMs = Date.now() - startedAt;
      const values = this.timers.get(name) ?? [];
      values.push(durationMs);
      this.timers.set(name, values);
      return durationMs;
    };
  }

  getCounters(): Record<MetricCounterName, number> {
    return {
      pages_crawled: this.counters.get("pages_crawled") ?? 0,
      docs_discovered: this.counters.get("docs_discovered") ?? 0,
      docs_enqueued: this.counters.get("docs_enqueued") ?? 0,
      downloads_ok: this.counters.get("downloads_ok") ?? 0,
      downloads_failed: this.counters.get("downloads_failed") ?? 0,
      extracts_ok: this.counters.get("extracts_ok") ?? 0,
      extracts_failed: this.counters.get("extracts_failed") ?? 0,
    };
  }

  getTimerSummaries(): Record<MetricTimerName, HistogramSummary> {
    return {
      page_fetch_ms: this.summarize("page_fetch_ms"),
      download_ms: this.summarize("download_ms"),
      extract_ms: this.summarize("extract_ms"),
    };
  }

  printSummary(): void {
    console.log(
      JSON.stringify(
        {
          ts: new Date().toISOString(),
          level: "info",
          msg: "metrics_summary",
          counters: this.getCounters(),
          timers: this.getTimerSummaries(),
        },
        null,
        2,
      ),
    );
  }

  private summarize(name: MetricTimerName): HistogramSummary {
    const values = this.timers.get(name) ?? [];
    if (values.length === 0) {
      return { count: 0, min: 0, max: 0, avg: 0 };
    }

    let total = 0;
    let min = values[0];
    let max = values[0];
    for (const value of values) {
      total += value;
      min = Math.min(min, value);
      max = Math.max(max, value);
    }

    return {
      count: values.length,
      min,
      max,
      avg: Number((total / values.length).toFixed(2)),
    };
  }
}
