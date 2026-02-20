import { AppConfig } from "../config";
import { PipelineStore } from "./types";
import { SqliteStore } from "./sqliteStore";

export function createStore(config: AppConfig): PipelineStore {
  return new SqliteStore(config.storePath);
}

export * from "./types";
