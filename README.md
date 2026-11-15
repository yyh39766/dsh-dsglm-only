# dsh-dsglm-only

A DeepSeek Harness provider plugin: one route that offers **only DeepSeek and Zhipu GLM** models, merged across every channel [`dsh-providers-hub`](https://www.npmjs.com/package/dsh-providers-hub) exposes, with automatic failover between those channels.

Registering this route does not remove other providers' models from the selector. Filtering those is a separate job — see [Making the selector show only these models](#making-the-selector-show-only-these-models).

## What it does

`dsh-providers-hub` exposes a large model list across ~30 upstream channels (WorkBuddy, Trae, ZCode, CodeBuddy, Qoder, OpenCode, its own aggregate route, and others). The same logical model often appears on several of them, spelled differently:

| channel | id for GLM-5.3 |
|---|---|
| `workbuddy` | `glm-5.3` |
| `aggregate` | `glm-5-3` |
| `qoder` | `gmodel` |
| `zcode` | `GLM-5.3` |

This plugin registers one provider route (`dsglm` by default) whose catalog is built by:

1. reading the hub's `/v1/models` list,
2. keeping only entries whose id or display name is DeepSeek or GLM,
3. merging entries that denote the same model into one row, keyed on the **display name** rather than the channel id,
4. remembering every channel as a *candidate* for that row.

A request then goes to the first candidate and, if that channel fails **before emitting any output**, falls back to the next one. Once a chunk has been emitted there is no failover — a silent mid-stream switch would leave the session with two half-answers.

On a machine with the hub's usual channels configured, the catalog collapses to about 20 rows, each backed by up to a dozen candidate channels.

## Requirements

- **DSH 0.2.x** with the plugin manager.
- **[`dsh-providers-hub`](https://www.npmjs.com/package/dsh-providers-hub)** installed in the same profile, with at least one channel logged in. This plugin proxies through that hub's OpenAI-compatible gateway on `http://127.0.0.1:8326/v1`; it implements no upstream protocol itself. Without the hub running you get an empty catalog and a `MISSING_CREDENTIAL`-style error on request.
- **Node 22.5+**.

## Install

```sh
dsh plugin --profile <profile> add dsh-dsglm-only
```

Then restart DSH. The route appears in the model selector as **DeepSeek · GLM（多渠聚合）**.

Verify it loaded:

```sh
# should print the dsglm route and its model count
grep -c dsglm ~/.dsh/profiles/<profile>/cordis.patch.yml
```

## Making the selector show only these models

The plugin adds a route; it does not subtract other routes — DSH has no seam for that, and `ctx.llm.registerAdapter` can only add. To make DeepSeek/GLM the *only* thing visible, also narrow the other plugins through their own settings:

| plugin | setting | effect |
|---|---|---|
| `dsh-connect-workbuddy` | `regions.<cn\|global>.enabledModelIds` | allow-list; `[]` means show everything |
| `dsh-connect-trae` | `regions.<cn\|ai\|enterprise>.enabledModelIds`, `regions.ai.enabled: false` | allow-list per region |
| `opencode2dsh` | `allowedModels` | allow-list; `[]` means no filtering |
| `dsh-providers-hub` | `disabledModels` in `~/.dsh/jet-hub/state.json`, `providerClosed` | deny-list per channel |
| `@liustack/modlens` | `discover: []` | stop it mirroring other routes as `modlens-*` |

All of these live in the profile's `cordis.patch.yml` (or, for the hub, its own state file) and take effect after a restart. Settings are not file-watched.

## Configuration

Everything is optional; the defaults work on a stock hub install.

| key | default | meaning |
|---|---|---|
| `enabled` | `true` | `false` takes the whole route down |
| `providerId` | `dsglm` | route name; changing it changes the selector group |
| `providerName` | `DeepSeek · GLM（多渠聚合）` | label shown in the selector |
| `gatewayBaseUrl` | `http://127.0.0.1:8326/v1` | the hub's OpenAI-compatible gateway |
| `apiKeyFile` | `$DSH_HOME/openai-gateway/api-key` | gateway bearer token file |
| `cacheFile` | `$DSH_HOME/dsglm-only/catalog.json` | on-disk catalog cache |
| `providerPriority` | official → workbuddy → trae → zcode → buddy → qoder → opencode → aggregate | candidate order, unlisted channels last |
| `maxCandidates` | `3` (max 12) | how many channels one request may try |

Example:

```yaml
- id: dsh-dsglm-only
  name: dsh-dsglm-only
  config:
    maxCandidates: 5
    providerPriority: [deepseek-official, workbuddy, aggregate]
```

## Behaviour worth knowing

**The catalog survives an unreachable gateway.** It caches to disk and keeps the last good list rather than emptying the selector. If the hub starts a moment after this plugin, the plugin retries eight times at 1.5 s intervals, then falls back to the 20-minute refresh timer.

**`inputModalities` is declared as `['text']`.** The gateway projects images to text placeholders before this plugin sees them; claiming image support here would be false.

**Unknown models fail loudly.** Asking for a model outside the catalog raises `UNKNOWN_MODEL` with the model name in the message. Passing `<channel>/<model>` explicitly bypasses the catalog, which is useful when debugging a specific channel.

**Usage counters follow DSH's disjoint accounting.** `inputTokens` excludes cached input; `cacheReadTokens` carries the cached portion; `totalTokens` is the sum. OpenAI reports a combined prompt count, so the cached part is subtracted out.

**One provider's failure is not a route failure.** A channel that returns 405 or 429 is logged and skipped; only when every candidate fails does the request error.

## Limits

- Depends on `dsh-providers-hub` being installed, running, and holding a working channel. This is a front-end for that hub, not an independent client.
- Text only.
- Upstream channel quotas, rate limits and risk controls apply unchanged — a channel that is throttled will simply be skipped by failover, not fixed.

## Tests

```sh
npm test        # offline: family matching and model merging, no network needed
npm run test:live   # needs dsh-providers-hub running with a signed-in channel
```

`npm test` covers the only logic this plugin owns — which models belong to the
DeepSeek/GLM family (including the trap where CodeBuddy's `deep-model` tier alias
looks like DeepSeek but is not), and how the per-channel spellings of one model
collapse onto a single row.

`npm run test:live` exercises the real path: catalog build, `resolveModel`,
`prepareCall`, and a real streaming request including a tool call. It prints
`SKIP` and exits zero when no gateway is reachable, so it never fails on a
machine without the hub.

## License

MIT
