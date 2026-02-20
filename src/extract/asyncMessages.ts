export type ExtractRequestMessageV1 = {
  version: "v1";
  type: "tables.extract.request";
  jobId: string;
  runId: string;
  docId: string;
  rawPdfPath: string;
  fileSha256?: string;
  attempt: number;
  submittedAt: string;
  options?: {
    pageFrom?: number;
    pageTo?: number;
    timeoutMs?: number;
  };
};

export type ExtractResultMessageV1 = {
  version: "v1";
  type: "tables.extract.result";
  jobId: string;
  docId: string;
  status: "ok" | "no_tables" | "failed";
  errorCode?: "dependency_missing" | "file_not_found" | "file_access_denied" | "parse_error" | "worker_exception" | "unknown";
  tablesLocation?: string;
  tableCount?: number;
  engine?: string;
  error?: string;
  durationMs?: number;
  finishedAt: string;
};

function isString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

export function isExtractRequestMessageV1(value: unknown): value is ExtractRequestMessageV1 {
  if (!value || typeof value !== "object") {
    return false;
  }
  const v = value as ExtractRequestMessageV1;
  return (
    v.version === "v1" &&
    v.type === "tables.extract.request" &&
    isString(v.jobId) &&
    isString(v.runId) &&
    isString(v.docId) &&
    isString(v.rawPdfPath) &&
    Number.isInteger(v.attempt) &&
    isString(v.submittedAt)
  );
}

export function isExtractResultMessageV1(value: unknown): value is ExtractResultMessageV1 {
  if (!value || typeof value !== "object") {
    return false;
  }
  const v = value as ExtractResultMessageV1;
  return (
    v.version === "v1" &&
    v.type === "tables.extract.result" &&
    isString(v.jobId) &&
    isString(v.docId) &&
    (v.status === "ok" || v.status === "no_tables" || v.status === "failed") &&
    isString(v.finishedAt)
  );
}
