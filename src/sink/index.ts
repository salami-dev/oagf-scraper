import { AppConfig } from "../config";
import { GrpcSink } from "./grpcSink";
import { HttpSink } from "./httpSink";
import { LocalJsonlSink } from "./localJsonlSink";
import { RabbitSink } from "./rabbitSink";
import { SqsSink } from "./sqsSink";
import { Sink } from "./types";

export function createSink(config: AppConfig, runId: string): Sink {
  const sinkType = (process.env.SINK_TYPE ?? "local_jsonl").toLowerCase();

  switch (sinkType) {
    case "local_jsonl":
      return new LocalJsonlSink(config, runId);
    case "sqs":
      return new SqsSink(process.env.SQS_QUEUE_URL);
    case "rabbit":
      return new RabbitSink(process.env.RABBIT_URL);
    case "http":
      return new HttpSink(process.env.HTTP_SINK_ENDPOINT, process.env.HTTP_SINK_TOKEN);
    case "grpc":
      return new GrpcSink(process.env.GRPC_SINK_TARGET);
    default:
      throw new Error(`Unsupported sink type: ${sinkType}`);
  }
}

export * from "./types";
