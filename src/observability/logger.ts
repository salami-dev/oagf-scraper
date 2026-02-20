import { LogFields, LogLevel } from "./types";

export interface LoggerContext {
  component: string;
  runId: string;
}

export class Logger {
  private readonly context: LoggerContext;

  constructor(context: LoggerContext) {
    this.context = context;
  }

  child(component: string): Logger {
    return new Logger({ component, runId: this.context.runId });
  }

  debug(msg: string, fields?: LogFields): void {
    this.write("debug", msg, fields);
  }

  info(msg: string, fields?: LogFields): void {
    this.write("info", msg, fields);
  }

  warn(msg: string, fields?: LogFields): void {
    this.write("warn", msg, fields);
  }

  error(msg: string, fields?: LogFields): void {
    this.write("error", msg, fields);
  }

  private write(level: LogLevel, msg: string, fields?: LogFields): void {
    const payload = {
      ts: new Date().toISOString(),
      level,
      msg,
      component: this.context.component,
      runId: this.context.runId,
      ...(fields ?? {}),
    };

    const line = JSON.stringify(payload);
    if (level === "error") {
      console.error(line);
      return;
    }
    console.log(line);
  }
}
