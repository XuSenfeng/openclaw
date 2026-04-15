import { AsyncLocalStorage } from "node:async_hooks";

// Re-implementing the core logic for gateway-request-scope to make the plugin self-contained.
// This ensures the plugin can run on standard OpenClaw distributions without modifying core configs.

export type PluginRuntimeGatewayRequestScope = {
  context?: unknown;
  client?: unknown;
  isWebchatConnect: () => boolean;
  pluginId?: string;
};

const PLUGIN_RUNTIME_GATEWAY_REQUEST_SCOPE_KEY: unique symbol = Symbol.for(
  "openclaw.pluginRuntimeGatewayRequestScope",
);

function resolveGlobalSingleton<T>(key: symbol, create: () => T): T {
  const globalStore = globalThis as Record<PropertyKey, unknown>;
  if (Object.prototype.hasOwnProperty.call(globalStore, key)) {
    return globalStore[key] as T;
  }
  const created = create();
  globalStore[key] = created;
  return created;
}

const pluginRuntimeGatewayRequestScope = resolveGlobalSingleton<
  AsyncLocalStorage<PluginRuntimeGatewayRequestScope>
>(
  PLUGIN_RUNTIME_GATEWAY_REQUEST_SCOPE_KEY,
  () => new AsyncLocalStorage<PluginRuntimeGatewayRequestScope>(),
);

/**
 * Runs plugin gateway handlers with request-scoped context that runtime helpers can read.
 */
export function withPluginRuntimeGatewayRequestScope<T>(
  scope: PluginRuntimeGatewayRequestScope,
  run: () => T,
): T {
  return pluginRuntimeGatewayRequestScope.run(scope, run);
}
