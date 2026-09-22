import type { TestContext } from "vitest";
import { createFixtureLifetime } from "../../test/helpers/fixture-lifetime.js";
import { createDeferred } from "../../test/helpers/promise.js";
import type { MemoryEmbeddingProviderAdapter } from "../plugins/memory-embedding-providers.js";
import type { GatewayServer } from "./server.js";

type EmbeddingRequest = (
  body: unknown,
  headers?: Record<string, string>,
  signal?: AbortSignal,
) => Promise<Response>;

export function createEmbeddingHttpRequest(getPort: () => number): EmbeddingRequest {
  return async (body, headers, signal) =>
    await fetch(`http://127.0.0.1:${getPort()}/v1/embeddings`, {
      method: "POST",
      signal,
      headers: {
        authorization: "Bearer secret",
        "content-type": "application/json",
        "x-openclaw-scopes": "operator.write",
        ...headers,
      },
      body: JSON.stringify(body),
    });
}

export function withEmbeddingProviderCleanup(
  context: Pick<TestContext, "signal" | "onTestFinished">,
  options: {
    request: EmbeddingRequest;
    drain: () => Promise<void>;
    adapter: Pick<MemoryEmbeddingProviderAdapter, "transport">;
    server: Pick<GatewayServer, "close">;
  },
  run: (fixture: {
    close: () => Promise<void>;
    releaseClose: () => void;
    request: (body: unknown) => Promise<Response>;
    waitForClose: (request: Promise<Response>) => Promise<void>;
  }) => Promise<void>,
): Promise<void> {
  const lifetime = createFixtureLifetime();
  context.onTestFinished(() => lifetime.cleanup());
  return lifetime.run(async () => {
    const stopping = new AbortController();
    const signal = AbortSignal.any([context.signal, stopping.signal]);
    const entered = createDeferred();
    const closeGate = createDeferred();
    const cancelled = createDeferred();
    const requests: Promise<Response>[] = [];
    let completed = false;
    const abort = () => {
      closeGate.resolve();
      cancelled.resolve();
    };
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) {
      abort();
    }
    try {
      signal.throwIfAborted();
      Reflect.set(options.adapter, "transport", "local");
      await run({
        close: async () => {
          entered.resolve();
          await closeGate.promise;
        },
        releaseClose: closeGate.resolve,
        request: (body) => {
          signal.throwIfAborted();
          const pending = options.request(body, undefined, signal);
          requests.push(pending);
          return pending;
        },
        waitForClose: async (request) => {
          await Promise.race([
            entered.promise,
            request.then((response) => {
              throw new Error(
                `Embedding request completed before cleanup entry (${response.status})`,
              );
            }),
            cancelled.promise,
          ]);
          signal.throwIfAborted();
        },
      });
      signal.throwIfAborted();
      completed = true;
    } finally {
      closeGate.resolve();
      stopping.abort();
      try {
        await Promise.allSettled(requests);
        if (completed && !context.signal.aborted) {
          await lifetime.verifyCleanup(options.drain);
        } else {
          // Client abort does not join HTTP work that has not reached provider admission.
          await lifetime.verifyCleanup(() =>
            options.server.close({ reason: "embedding cleanup fixture failed or cancelled" }),
          );
        }
      } finally {
        signal.removeEventListener("abort", abort);
        Reflect.set(options.adapter, "transport", "remote");
      }
    }
  });
}
