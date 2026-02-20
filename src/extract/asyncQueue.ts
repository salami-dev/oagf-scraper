import { ExtractRequestMessageV1, ExtractResultMessageV1 } from "./asyncMessages";

export interface QueuedExtractRequest {
  queueMessageId: string;
  message: ExtractRequestMessageV1;
}

export interface QueuedExtractResult {
  queueMessageId: string;
  message: ExtractResultMessageV1;
}

export interface AsyncTableQueue {
  publishRequest(message: ExtractRequestMessageV1): Promise<void>;
  consumeRequests(limit: number): Promise<QueuedExtractRequest[]>;
  ackRequest(queueMessageId: string): Promise<void>;
  publishResult(message: ExtractResultMessageV1): Promise<void>;
  consumeResults(limit: number): Promise<QueuedExtractResult[]>;
  ackResult(queueMessageId: string): Promise<void>;
  close(): Promise<void>;
}
