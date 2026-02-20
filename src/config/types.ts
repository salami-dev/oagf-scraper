export interface OutputDirs {
  raw: string;
  extracted: string;
  manifests: string;
}

export interface AppConfig {
  baseUrl: string;
  userAgent: string;
  ignoreHttpsErrors: boolean;
  pollIntervalMinutes: number;
  verifyDownloadedFilesOnStartup: boolean;
  revalidateAfterDays: number;
  changeDetectionMode: "none" | "head";
  requestTimeoutMs: number;
  downloadTimeoutMs: number;
  crawlConcurrency: number;
  downloadConcurrency: number;
  extractConcurrency: number;
  maxPages: number;
  maxDownloadAttempts: number;
  maxExtractAttempts: number;
  outputDirs: OutputDirs;
  storePath: string;
}

export type ConfigOverrides = Partial<Omit<AppConfig, "outputDirs">> & {
  outputDirs?: Partial<OutputDirs>;
};
