export function createRunId(now = new Date()): string {
  const suffix = Math.random().toString(36).slice(2, 8);
  return `run_${now.toISOString().replace(/[:.]/g, "-")}_${suffix}`;
}
