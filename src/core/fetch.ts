import { Agent } from "undici";

let insecureAgent: Agent | undefined;

function getInsecureAgent(): Agent {
  if (!insecureAgent) {
    insecureAgent = new Agent({
      connect: {
        rejectUnauthorized: false,
      },
    });
  }
  return insecureAgent;
}

export function getFetchDispatcher(ignoreHttpsErrors: boolean): Agent | undefined {
  if (!ignoreHttpsErrors) {
    return undefined;
  }
  return getInsecureAgent();
}
