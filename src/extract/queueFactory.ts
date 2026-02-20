import { AppConfig } from "../config";
import { AsyncTableQueue } from "./asyncQueue";
import { HttpAsyncQueueAdapter } from "./httpAsyncQueueAdapter";
import { LocalSqliteQueueAdapter } from "./localSqliteQueueAdapter";

export function createAsyncQueue(config: AppConfig): AsyncTableQueue {
  if (config.asyncQueueMode === "http") {
    return new HttpAsyncQueueAdapter({
      baseUrl: config.asyncQueueHttpBaseUrl,
      token: config.asyncQueueHttpToken,
      timeoutMs: config.asyncQueueHttpTimeoutMs,
      ignoreHttpsErrors: config.ignoreHttpsErrors,
    });
  }

  return new LocalSqliteQueueAdapter(config.asyncQueuePath);
}
