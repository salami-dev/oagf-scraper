import { DocumentDiscoveredItem, DownloadResult, ExtractionResult } from "../types";
import { BaseSink } from "./baseSink";

export class GrpcSink extends BaseSink {
  private readonly target?: string;

  constructor(target?: string) {
    super();
    this.target = target;
  }

  async publishDiscovered(_items: DocumentDiscoveredItem[]): Promise<void> {
    this.ensureConfigured("gRPC", Boolean(this.target));
    throw new Error("gRPC sink sending not yet implemented");
  }

  async publishDownloadResult(_results: DownloadResult[]): Promise<void> {
    this.ensureConfigured("gRPC", Boolean(this.target));
    throw new Error("gRPC sink sending not yet implemented");
  }

  async publishExtractionResult(_results: ExtractionResult[]): Promise<void> {
    this.ensureConfigured("gRPC", Boolean(this.target));
    throw new Error("gRPC sink sending not yet implemented");
  }
}
