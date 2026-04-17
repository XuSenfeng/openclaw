import { createChatChannelPlugin, createChannelPluginBase } from "openclaw/plugin-sdk/channel-core";
import { attachChannelToResult } from "openclaw/plugin-sdk/channel-send-result";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import { resolveStorePath } from "openclaw/plugin-sdk/session-store-runtime";
import { createStandardChannelSetupStatus } from "openclaw/plugin-sdk/setup";
// @ts-ignore - Internal import for registry state symbol
import { PLUGIN_REGISTRY_STATE } from "../../src/plugins/runtime-state.js";
// @ts-ignore - Internal import for subagent mode check
import { getActivePluginRuntimeSubagentMode } from "../../src/plugins/runtime.js";
// @ts-ignore - Internal import for global singleton
import { resolveGlobalSingleton } from "../../src/shared/global-singleton.js";
import { withPluginRuntimeGatewayRequestScope } from "./gateway-request-scope.js";

const GATEWAY_SUBAGENT_SYMBOL: unique symbol = Symbol.for(
  "openclaw.plugin.gatewaySubagentRuntime",
) as unknown as typeof GATEWAY_SUBAGENT_SYMBOL;

const FALLBACK_GATEWAY_CONTEXT_STATE_KEY: unique symbol = Symbol.for(
  "openclaw.fallbackGatewayContextState",
);

const PYTHON_PLATFORM_RUNTIME_STATE_KEY: unique symbol = Symbol.for(
  "openclaw.plugin.pythonPlatformRuntimeState",
);

type PythonPlatformRuntimeState = {
  processedMessageIds: Set<string>;
  activeConversationByUser: Map<string, string>;
  conversationTurnsBySession: Map<string, Array<{ role: "user" | "assistant"; content: string }>>;
};

function getGatewaySubagentRuntime() {
  const state = resolveGlobalSingleton<{ subagent: unknown }>(GATEWAY_SUBAGENT_SYMBOL, () => ({
    subagent: undefined,
  }));
  return state.subagent as { run: (opts: unknown) => Promise<unknown> } | undefined;
}

function forceGatewaySubagentBinding() {
  try {
    const globalState = globalThis as Record<string, unknown>;
    const state = globalState[PLUGIN_REGISTRY_STATE as unknown as string] as {
      runtimeSubagentMode: string;
    };
    if (state && state.runtimeSubagentMode !== "gateway-bindable") {
      state.runtimeSubagentMode = "gateway-bindable";
    }
  } catch {
    // Ignore errors
  }
}

function getFallbackGatewayContext() {
  const state = resolveGlobalSingleton<Record<string, unknown>>(
    FALLBACK_GATEWAY_CONTEXT_STATE_KEY,
    () => ({
      context: undefined,
      resolveContext: undefined,
    }),
  );
  const resolved = (state.resolveContext as () => unknown)?.();
  return resolved ?? state.context;
}

function getPythonPlatformRuntimeState(): PythonPlatformRuntimeState {
  return resolveGlobalSingleton<PythonPlatformRuntimeState>(
    PYTHON_PLATFORM_RUNTIME_STATE_KEY,
    () => ({
      processedMessageIds: new Set<string>(),
      activeConversationByUser: new Map<string, string>(),
      conversationTurnsBySession: new Map<
        string,
        Array<{ role: "user" | "assistant"; content: string }>
      >(),
    }),
  );
}

export default definePluginEntry({
  id: "python-platform",
  name: "Python Virtual Platform",
  description: "Connects to Python WebSocket server remotely and answers chat",
  register(api) {
    if (api.registrationMode !== "full") {
      return;
    }

    let ws: { send: (data: string) => void; readyState: number } | null = null;
    let isConnecting = false;
    let wsUrl = "";
    const runtimeState = getPythonPlatformRuntimeState();
    const processedMessageIds = runtimeState.processedMessageIds;
    const activeConversationByUser = runtimeState.activeConversationByUser;
    const conversationTurnsBySession = runtimeState.conversationTurnsBySession;

    const appendConversationTurn = (
      sessionKey: string,
      role: "user" | "assistant",
      content: string,
    ) => {
      const normalized = content.trim();
      if (!normalized) {
        return;
      }
      const turns = conversationTurnsBySession.get(sessionKey) ?? [];
      turns.push({ role, content: normalized });
      // Keep only the latest 20 turns to avoid unbounded growth.
      if (turns.length > 20) {
        turns.splice(0, turns.length - 20);
      }
      conversationTurnsBySession.set(sessionKey, turns);
    };

    const buildMessageWithConversationContext = (sessionKey: string, latestUserMessage: string) => {
      const history = conversationTurnsBySession.get(sessionKey) ?? [];
      if (!history.length) {
        return latestUserMessage;
      }
      const historyText = history
        .slice(-12)
        .map((turn) => `${turn.role === "user" ? "User" : "Assistant"}: ${turn.content}`)
        .join("\n");
      return [
        "Continue the same conversation using the following history.",
        "",
        historyText,
        "",
        `User: ${latestUserMessage}`,
      ].join("\n");
    };

    const resolveConversationId = (payload: Record<string, unknown>) => {
      const userId = typeof payload.user_id === "string" ? payload.user_id : "unknown";
      const incomingChatId = typeof payload.chat_id === "string" ? payload.chat_id : "";
      const existing = activeConversationByUser.get(userId);
      if (existing) {
        return { userId, conversationId: existing };
      }
      const initial = incomingChatId || "default_chat";
      activeConversationByUser.set(userId, initial);
      return { userId, conversationId: initial };
    };

    const connect = async () => {
      if (isConnecting) {
        return;
      }
      if (ws && (ws as { readyState: number }).readyState === 1) {
        return;
      }
      isConnecting = true;

      try {
        // 修复：api.config 是一个对象字面量，不是包含 get() 方法的对象
        const config = (api.config as Record<string, Record<string, unknown>>).channels?.[
          "python-platform"
        ] as Record<string, unknown> | undefined;
        wsUrl =
          (config?.wsUrl as string) || process.env.PYTHON_PLATFORM_WS_URL || "ws://127.0.0.1:8765";

        const WebSocket = (await import("ws")).default;
        api.logger.info(`[python-platform] Connecting to ${wsUrl}...`);

        const socket = new WebSocket(wsUrl);
        ws = socket as unknown as { send: (data: string) => void; readyState: number };

        socket.on("open", () => {
          isConnecting = false;
          api.logger.info(`[python-platform] Connected to server at ${wsUrl}`);
        });

        socket.on("message", async (data: unknown) => {
          const raw = Array.isArray(data)
            ? Buffer.concat(data).toString("utf8")
            : (data as Buffer | string).toString("utf8");
          let payload: Record<string, unknown>;
          try {
            payload = JSON.parse(raw) as Record<string, unknown>;
          } catch {
            return;
          }

          if (payload.type === "start_new_conversation") {
            const userId = typeof payload.user_id === "string" ? payload.user_id : "unknown";
            const nextChatId =
              typeof payload.chat_id === "string" && payload.chat_id.trim()
                ? payload.chat_id
                : `chat-${Date.now()}`;
            activeConversationByUser.set(userId, nextChatId);
            api.logger.info(`[python-platform] Conversation switched for ${userId}: ${nextChatId}`);
            return;
          }

          if (payload.type === "inbound_message") {
            const messageId = typeof payload.message_id === "string" ? payload.message_id : "";
            if (messageId) {
              if (processedMessageIds.has(messageId)) {
                return;
              }
              processedMessageIds.add(messageId);
              // Keep the cache size manageable
              if (processedMessageIds.size > 1000) {
                const firstId = processedMessageIds.values().next().value;
                if (firstId) {
                  processedMessageIds.delete(firstId);
                }
              }
            }

            const { userId, conversationId } = resolveConversationId(payload);
            const content = typeof payload.content === "string" ? payload.content : "";
            const userName = typeof payload.user_name === "string" ? payload.user_name : userId;

            api.logger.info(`[python-platform] Inbound from ${userName}: ${content}`);
            if (!content.trim()) {
              return;
            }

            // 强制开启子代理绑定模式
            forceGatewaySubagentBinding();

            const sessionKey = `python-platform:${userId}:${conversationId}`;
            const fallbackContext = getFallbackGatewayContext();
            const messageForRun = buildMessageWithConversationContext(sessionKey, content);
            appendConversationTurn(sessionKey, "user", content);

            // 预先关联 Session 以便 subagent.run 能够找到分发目标 (delivery target)
            // 解决 "Delivering to Python Platform requires target" 错误，因为 subagent.run 会丢弃多余参数
            try {
              const runtime = api.runtime as unknown as {
                channel: {
                  session: {
                    updateLastRoute: (params: unknown) => Promise<unknown>;
                  };
                };
              };
              const storePath = resolveStorePath(
                (
                  (api.config as Record<string, unknown>)?.session as
                    | Record<string, unknown>
                    | undefined
                )?.store as string | undefined,
              );
              await runtime.channel.session.updateLastRoute({
                storePath,
                sessionKey,
                deliveryContext: {
                  channel: "python-platform",
                  to: userId,
                  accountId: "default",
                  threadId: conversationId,
                },
              });
            } catch (e) {
              api.logger.error(
                `[python-platform] Failed to associate session: ${(e as Error).message}`,
              );
            }

            if (api.logger.debug) {
              api.logger.debug(
                `[python-platform] Dispatch: session=${sessionKey}, ctx=${!!fallbackContext}, channel=python-platform`,
              );
            }

            try {
              // Log subagent mode for debugging
              api.logger.info(
                `[python-platform] Active subagent mode: ${getActivePluginRuntimeSubagentMode()}`,
              );

              // Use withPluginRuntimeGatewayRequestScope to bypass "only available during a gateway request" error
              await withPluginRuntimeGatewayRequestScope(
                {
                  context: fallbackContext,
                  client: {
                    connect: {
                      protocol: "internal",
                      version: "1.0",
                      deviceId: "python-platform",
                      role: "operator",
                      scopes: ["operator.write", "operator.read"],
                    },
                    internal: {
                      allowModelOverride: true,
                    },
                    // 明确提供必要的作用域以执行子代理 (Subagent)
                    scopes: [
                      "operator.write",
                      "operator.read",
                      "subagent",
                      "media-understanding",
                      "image-generation",
                      "web-search",
                      "tasks.run",
                      "tasks.flow",
                    ],
                    id: "python-platform-client",
                  } as unknown,
                  isWebchatConnect: () => false,
                  pluginId: "python-platform",
                },
                async () => {
                  // IMPORTANT: Directly access the subagent from the global singleton
                  // This bypasses the api.runtime.subagent proxy which checks allowGatewaySubagentBinding
                  // that might have been false during plugin initialization.
                  const subagent =
                    getGatewaySubagentRuntime() ||
                    (
                      api.runtime as unknown as {
                        subagent: { run: (opts: unknown) => Promise<unknown> };
                      }
                    ).subagent;

                  if (!subagent) {
                    throw new Error(
                      "Subagent runtime not available. Ensure Gateway is initialized.",
                    );
                  }

                  // Using a more direct 'run' call if available, or ensuring we are in the correct async context
                  const runResult = await (
                    subagent as { run: (opts: unknown) => Promise<unknown> }
                  ).run({
                    sessionKey,
                    message: messageForRun,
                    deliver: true,
                    channel: "python-platform",
                    replyChannel: "python-platform",
                    accountId: "default",
                    replyAccountId: "default",
                    to: userId,
                    threadId: conversationId,
                    // 使用 messageId 作为幂等键以便在 Dashboard 中合并显示
                    idempotencyKey: messageId || `py-${userId}-${conversationId}-${Date.now()}`,
                  });
                  api.logger.info(
                    `[python-platform] Subagent run started: ${JSON.stringify(runResult)}`,
                  );
                },
              );
            } catch (err: unknown) {
              const error = err as Error;
              api.logger.error(`[python-platform] Dispatch error: ${error.message}`);
              if (error.stack) {
                api.logger.error(`[python-platform] Stack trace: ${error.stack}`);
              }
            }
          }
        });

        socket.on("close", () => {
          isConnecting = false;
          ws = null;
          api.logger.warn("[python-platform] Disconnected. Reconnecting in 5s...");
          setTimeout(() => {
            void connect();
          }, 5000);
        });

        socket.on("error", (e: unknown) => {
          isConnecting = false;
          const error = e as { code?: string; message: string };
          if (error.code !== "ECONNREFUSED") {
            api.logger.error(`[python-platform] WS Error: ${error.message}`);
          }
        });
      } catch (error) {
        isConnecting = false;
        throw error;
      }
    };

    const channelPlugin = createChatChannelPlugin({
      base: createChannelPluginBase({
        id: "python-platform",
        meta: {
          label: "Python Platform",
        },
        config: {
          listAccountIds: (_cfg: unknown) => ["default"],
          resolveAccount: ({ accountId }: { accountId: string }) => ({
            accountId,
            configured: true,
            label: "Python Platform (Default)",
          }),
        },
        setupWizard: {
          channel: "python-platform",
          status: createStandardChannelSetupStatus({
            channelLabel: "Python Platform",
            configuredLabel: "configured",
            unconfiguredLabel: "not configured",
            configuredHint: "connected via WebSocket",
            unconfiguredHint: "needs WebSocket URL",
            configuredScore: 1,
            unconfiguredScore: 10,
            resolveConfigured: ({ cfg }: { cfg: unknown }) =>
              !!(cfg as Record<string, Record<string, Record<string, unknown>>>).channels?.[
                "python-platform"
              ]?.wsUrl,
          }),
          prepare: async ({
            cfg,
            credentialValues,
          }: {
            cfg: unknown;
            credentialValues: unknown;
          }) => ({
            cfg,
            credentialValues,
            accountId: "default",
          }),
          credentials: [
            {
              inputKey: "wsUrl",
              credentialLabel: "WebSocket URL",
              preferredEnvVar: "PYTHON_PLATFORM_WS_URL",
              helpTitle: "WebSocket URL",
              helpLines: [
                "The URL of your Python platform server.",
                "Example: ws://192.168.0.3:8765",
              ],
              inputPrompt: "Enter WebSocket URL",
              defaultValue: "ws://127.0.0.1:8765",
              inspect: ({ cfg }: { cfg: unknown }) => ({
                accountConfigured: !!(
                  cfg as Record<string, Record<string, Record<string, unknown>>>
                ).channels?.["python-platform"]?.wsUrl,
                resolvedValue: (cfg as Record<string, Record<string, Record<string, unknown>>>)
                  .channels?.["python-platform"]?.wsUrl as string,
              }),
              applySet: async ({ cfg, value }: { cfg: unknown; value: unknown }) => {
                const patch = { wsUrl: value };
                const { patchTopLevelChannelConfigSection } =
                  await import("openclaw/plugin-sdk/setup");
                /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
                const cfgAny = cfg as any;
                return patchTopLevelChannelConfigSection({
                  cfg: cfgAny,
                  channel: "python-platform",
                  enabled: true,
                  patch,
                });
              },
            },
          ],
          /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
        } as any,
        setup: async (_ctx: unknown) => {
          if (!ws) {
            await connect();
          }
          return { success: true };
        },
        /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
      } as any) as any,
      outbound: {
        /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
        deliver: async (payload: any) => {
          if (!ws || (ws as { readyState: number }).readyState !== 1) {
            /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
            const result: any = {
              success: false,
              error: "WebSocket not connected",
            };
            /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
            const attach: any = attachChannelToResult;
            return attach(result, "python-platform");
          }

          const { sessionKey, content } = payload as { sessionKey: string; content: string };
          const parts = sessionKey.split(":");
          const userId = parts[1] || "unknown";
          const chatId = parts[2] || "unknown";

          appendConversationTurn(sessionKey, "assistant", content);

          (ws as { send: (data: string) => void }).send(
            JSON.stringify({
              type: "send_message",
              chat_id: chatId,
              to_user_id: userId,
              content: content,
            }),
          );

          /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
          const attach: any = attachChannelToResult;
          /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
          return attach({ success: true } as any, "python-platform");
        },
        sendText: async (payload: unknown) => {
          if (!ws || (ws as { readyState: number }).readyState !== 1) {
            /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
            const result: any = {
              success: false,
              error: "WebSocket not connected",
            };
            /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
            const attach: any = attachChannelToResult;
            return attach(result, "python-platform");
          }

          const { to, text, threadId } = payload as { to: string; text: string; threadId: string };
          const outboundSessionKey = `python-platform:${to}:${threadId || "unknown"}`;

          appendConversationTurn(outboundSessionKey, "assistant", text);

          (ws as { send: (data: string) => void }).send(
            JSON.stringify({
              type: "send_message",
              chat_id: threadId || "unknown",
              to_user_id: to,
              content: text,
            }),
          );

          /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
          const attach: any = attachChannelToResult;
          /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
          return attach({ success: true } as any, "python-platform");
        },
        /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
      } as any,
    });

    /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
    api.registerChannel(channelPlugin as any);

    // 主动触发一次连接
    void connect();
  },
});
