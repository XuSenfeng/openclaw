/**
 * Setup entry for Python Platform channel plugin
 */

import { Type } from "@sinclair/typebox";
import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";
import {
  createStandardChannelSetupStatus,
  patchTopLevelChannelConfigSection,
} from "openclaw/plugin-sdk/setup";

export default definePluginEntry({
  id: "python-platform-setup",
  name: "Python Platform Setup",
  description: "Setup wizard for Python Platform",
  register(api: unknown) {
    const apiTyped = api as { registerSetup: (p: unknown) => void };
    if (typeof apiTyped.registerSetup === "function") {
      apiTyped.registerSetup({
        id: "python-platform",
        async setup(_api: unknown) {
          return {
            success: true,
            wizard: {
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
              sections: [
                {
                  title: "Connection Settings",
                  fields: [
                    {
                      id: "wsUrl",
                      label: "WebSocket URL",
                      type: "string",
                      description:
                        "The URL of your Python platform server (e.g., ws://192.168.0.3:8765)",
                      default: "ws://127.0.0.1:8765",
                      schema: Type.String({ format: "uri" }),
                    },
                  ],
                },
              ],
              async finalize({ cfg, values }: { cfg: unknown; values: unknown }) {
                /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
                const cfgAny = cfg as any;
                /* eslint-disable-next-line @typescript-eslint/no-explicit-any */
                const valuesAny = values as any;
                return patchTopLevelChannelConfigSection({
                  cfg: cfgAny,
                  channel: "python-platform",
                  enabled: true,
                  patch: {
                    wsUrl: valuesAny.wsUrl as string,
                  },
                });
              },
            } as unknown,
          };
        },
      });
    }
  },
});
