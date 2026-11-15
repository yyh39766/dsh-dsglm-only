/**
 * dsh-dsglm-only — 一条只含 DeepSeek / 智谱 GLM 的聚合路由。
 *
 * WHAT IT IS
 *   注册一条 provider 路由 `dsglm`。它的模型清单 = 各渠道（workbuddy / trae /
 *   zcode / buddy / qoder / opencode / aggregate …）里**真实存在的 DeepSeek 与
 *   GLM 模型**，按"逻辑模型"归并去重；同一个逻辑模型在多渠道出现时，请求按
 *   优先顺序逐候选回退，直到一家接得上。
 *
 * WHY A GATEWAY PROXY AND NOT A DIRECT ADAPTER
 *   各家渠道的鉴权、指纹、SSE 协议形态都不一样（有的要 cookie、有的要设备码
 *   OAuth、有的网页端私有接口）。dsh-providers-hub 的 OpenAI 兼容网关已经把
 *   这些全部收敛成一套 `/v1/chat/completions`，所以本插件只做两件事：
 *     1) 目录侧：把网关 `/v1/models` 收敛成"只有 DeepSeek/GLM"的一份干净目录；
 *     2) 调用侧：把 DSH 的 GenerateOptions 翻译成 OpenAI 请求，把 SSE 翻译回
 *        DSH 的 StreamChunk，并在候选渠道之间做回退。
 *   这样协议实现只有一份（在 hub 里，已在真机验证），本插件保持薄。
 *
 * PROTOCOL FACTS（实测，2026-11-15）
 *   - 网关：`POST http://127.0.0.1:8326/v1/chat/completions`，Bearer =
 *     `$DSH_HOME/openai-gateway/api-key` 的 43 字符内容。
 *   - model 必须是 `<源provider>/<源modelId>`（网关按第一个 `/` 切分）。
 *   - SSE：`data: {choices:[{delta:{content|reasoning_content|tool_calls}}]}`，
 *     结束帧 `data: {choices:[], usage:{prompt_tokens, completion_tokens,
 *     prompt_tokens_details:{cached_tokens}}}`，最后 `data: [DONE]`。
 *   - 工具调用的 `arguments` 是流式增量，必须按 index 拼接。
 *   - 失败帧 `data: {error:{message, code, status}}`。
 *
 * DSH 契约（必须逐条满足，否则整条路由从选择器消失）
 *   - prepareCall 必须返回 { model, stream }，且 dsh-llm 走的是 **call.stream**，
 *     不是顶层 stream —— 所以这里显式包一层。
 *   - providerInfo(provider).id === provider，name 非空。
 *   - resolveModel 必须按入参回填 provider/id；contextWindow 必须是正整数。
 *   - usage 必须在 finish 之前发；finish 之后不能再有任何 chunk。
 */

import { randomUUID } from 'node:crypto'
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { homedir } from 'node:os'
import { dirname, join } from 'node:path'

//#region constants

const DEFAULT_PROVIDER_ID = 'dsglm'
const DEFAULT_PROVIDER_NAME = 'DeepSeek · GLM（多渠聚合）'
const DEFAULT_GATEWAY_BASE = 'http://127.0.0.1:8326/v1'

/** 目标模型族：只要 DeepSeek 与智谱 GLM。用全词，避免 `deep-model` 这类档位别名误命中。 */
const TARGET_PATTERN = /(deepseek|(^|[^a-z])glm([^a-z]|$)|chatglm|zhipu)/i

/**
 * 整族排除的渠道。
 *   modlens-*    ：@liustack/modlens 的镜像 route，与源 route 内容重复
 *   jet-hub-auto ：hub 的"临期优先"自动档位，不是具体模型
 *   doubaowork   ：豆包，非目标
 *   trae-global  ：gemini/gpt/kimi/minimax，无一条目标模型
 */
const EXCLUDED_PROVIDER_PATTERNS = [/^modlens-/, /^jet-hub-auto$/, /^doubaowork$/, /^trae-global$/, /^deepseek-modlens$/]

/** 已知的假阳性：CodeBuddy 的档位别名，不是 DeepSeek 模型。 */
const FALSE_POSITIVE_KEYS = new Set(['deep-model', 'deep-model-x'])

/** 渠道优先级：越靠前越先试。官方账号优先，聚合路由兜底。 */
const DEFAULT_PROVIDER_PRIORITY = [
  'deepseek-official',
  'deepseek-account',
  'workbuddy',
  'workbuddy-global',
  'trae',
  'zcode',
  'buddy',
  'qoder',
  'qodercn',
  'opencode',
  'opencode2dsh',
  'aggregate',
]

const CATALOG_REFRESH_INTERVAL_MS = 1_200_000
const CATALOG_LOAD_TIMEOUT_MS = 8_000
const SSE_IDLE_TIMEOUT_MS = 300_000

/**
 * 启动自举：providers-hub 的网关与本插件在同一批加载，
 * 首次启动时网关往往还没开始监听。这里给 8 次、每次 1.5 秒的窗口，
 * 拿到目录就停；拿不到也不无限重试，交给 20 分钟的定时刷新。
 */
const BOOTSTRAP_ATTEMPTS = 8
const BOOTSTRAP_INTERVAL_MS = 1_500

/** reasoning 档位索引：reasoning 固定 0，正文固定 1，工具调用从 2 起。 */
const REASONING_INDEX = 0
const ANSWER_INDEX = 1
const FIRST_TOOL_INDEX = 2

//#endregion

//#region helpers

function dshHome() {
  const fromEnv = process.env.DSH_HOME
  if (typeof fromEnv === 'string' && fromEnv.trim() !== '') return fromEnv.trim()
  return join(homedir(), '.dsh')
}

function readGatewayKey(explicitPath) {
  const path = typeof explicitPath === 'string' && explicitPath !== ''
    ? explicitPath
    : join(dshHome(), 'openai-gateway', 'api-key')
  try {
    const raw = readFileSync(path, 'utf8').trim()
    return raw === '' ? undefined : raw
  } catch {
    return undefined
  }
}

/**
 * 网关展示名去掉倍率尾巴与 modlens 标记，例如 `GLM-5.3 · x0.69` → `GLM-5.3`。
 * 倍率尾巴用"取中点前的第一段"来处理，比正则更耐受分隔符变体。
 */
function cleanName(value, fallback) {
  if (typeof value !== 'string') return fallback
  let name = value.split('·')[0].replace(/\s*\(modlens vision\)\s*$/i, '').trim()
  if (name === '') name = String(fallback ?? '').trim()
  return name
}

/**
 * 逻辑模型键：同一条模型在任意渠道都应落到同一个键，这样选择器里只出现一次。
 *
 * 键取自展示名而不是渠道 id —— 渠道 id 各家不一（qoder 用 `gmodel`、workbuddy 用
 * `glm-5.3`、aggregate 用 `glm-5-3`），但展示名是同一个模型名。例：
 *   `GLM-5.3` / `glm-5.3` / `glm-5-3`      → `glm-5-3`
 *   `DeepSeek-V4.1-Flash` / `deepseek-v4.1-flash` → `deepseek-v4-1-flash`
 */
function logicalKey(value) {
  let key = String(value).toLowerCase()
  key = key.replace(/\([^)]*\)/g, '')
  key = key.replace(/[._\s]+/g, '-')
  key = key.replace(/-+/g, '-').replace(/^-|-$/g, '')
  // 渠道后缀不是模型身份：aggregate 把 `deepseek-v4-pro-official` 与
  // `deepseek-v4-pro` 当两条，opencode 的免费额度给 `...-free`，volc/lkeap/sg
  // 是同一模型的部署变体。剥掉它们，同一个模型才会收进同一行。
  for (;;) {
    const stripped = key.replace(/-(official|free|volc|lkeap|sg)$/, '')
    if (stripped === key || stripped === '') break
    key = stripped
  }
  return key
}

function providerOf(modelRef, explicitOwner) {
  if (typeof explicitOwner === 'string' && explicitOwner !== '') return explicitOwner
  const slash = modelRef.indexOf('/')
  return slash > 0 ? modelRef.slice(0, slash) : ''
}

function modelIdOf(modelRef) {
  const slash = modelRef.indexOf('/')
  return slash > 0 ? modelRef.slice(slash + 1) : modelRef
}

function isExcludedProvider(provider) {
  return EXCLUDED_PROVIDER_PATTERNS.some((pattern) => pattern.test(provider))
}

function isTargetEntry(provider, modelId, name) {
  if (isExcludedProvider(provider)) return false
  const key = logicalKey(modelId)
  if (FALSE_POSITIVE_KEYS.has(key)) return false
  return TARGET_PATTERN.test(modelId) || TARGET_PATTERN.test(name)
}

function positiveInt(value) {
  return Number.isSafeInteger(value) && value > 0 ? value : undefined
}

function messageText(message) {
  const content = message?.content
  if (typeof content === 'string') return content
  if (!Array.isArray(content)) return ''
  return content
    .filter((block) => block?.type === 'text' && typeof block.text === 'string')
    .map((block) => block.text)
    .join('')
}

function toolCallsOf(message) {
  const content = message?.content
  if (!Array.isArray(content)) return []
  return content
    .filter((block) => block?.type === 'tool-call')
    .map((block) => ({
      id: String(block.id ?? `dsglm-call-${randomUUID()}`),
      type: 'function',
      function: {
        name: String(block.name ?? ''),
        arguments: typeof block.arguments === 'string' && block.arguments !== '' ? block.arguments : '{}',
      },
    }))
    .filter((call) => call.function.name !== '')
}

/** DSH 消息 → OpenAI chat 消息。图片/文件在装配阶段已被投影成文本占位。 */
function toOpenAiMessages(options) {
  const system = []
  if (typeof options.system === 'string' && options.system.trim() !== '') system.push(options.system.trim())
  const out = []
  const messages = Array.isArray(options.messages) ? options.messages : []
  for (const message of messages) {
    const role = message?.role
    if (role === 'system' || role === 'developer') {
      const text = messageText(message).trim()
      if (text !== '') system.push(text)
      continue
    }
    if (role === 'user') {
      out.push({ role: 'user', content: messageText(message) })
      continue
    }
    if (role === 'assistant') {
      const text = messageText(message)
      const calls = toolCallsOf(message)
      const entry = { role: 'assistant' }
      if (text !== '') entry.content = text
      if (calls.length > 0) entry.tool_calls = calls
      if (entry.content === undefined && entry.tool_calls === undefined) continue
      out.push(entry)
      continue
    }
    if (role === 'tool') {
      const callId = String(message?.toolCallId ?? message?.source?.callId ?? '')
      if (callId === '') continue
      out.push({ role: 'tool', tool_call_id: callId, content: messageText(message) })
    }
  }
  if (system.length > 0) out.unshift({ role: 'system', content: system.join('\n\n') })
  return out
}

function toOpenAiTools(tools) {
  if (!Array.isArray(tools) || tools.length === 0) return undefined
  const mapped = []
  for (const tool of tools) {
    if (typeof tool?.name !== 'string' || tool.name === '') continue
    mapped.push({
      type: 'function',
      function: {
        name: tool.name,
        description: typeof tool.description === 'string' ? tool.description : '',
        parameters: tool.parameters !== null && typeof tool.parameters === 'object'
          ? tool.parameters
          : { type: 'object', properties: {} },
      },
    })
  }
  return mapped.length > 0 ? mapped : undefined
}

/** 切 SSE 帧；返回已完整的数据行与未完成的尾巴。 */
function parseSseFrames(buffer) {
  const frames = []
  let rest = buffer
  for (;;) {
    const match = /\r?\n\r?\n/.exec(rest)
    if (match === null) break
    const raw = rest.slice(0, match.index)
    rest = rest.slice(match.index + match[0].length)
    const dataLines = []
    for (const line of raw.split(/\r?\n/)) {
      if (!line.startsWith('data:')) continue
      dataLines.push(line.slice(5).trimStart())
    }
    if (dataLines.length > 0) frames.push(dataLines.join('\n'))
  }
  return { frames, rest }
}

function usageFrom(payload) {
  const usage = payload?.usage
  if (usage === null || typeof usage !== 'object') return undefined
  const prompt = Number.isFinite(usage.prompt_tokens) ? usage.prompt_tokens : undefined
  const completion = Number.isFinite(usage.completion_tokens) ? usage.completion_tokens : undefined
  if (prompt === undefined && completion === undefined) return undefined
  const cached = Number.isFinite(usage.prompt_tokens_details?.cached_tokens)
    ? usage.prompt_tokens_details.cached_tokens
    : 0
  // DSH 的 TokenUsage 是 DISJOINT 计数：inputTokens 只算未缓存输入。
  const inputTokens = Math.max(0, (prompt ?? 0) - cached)
  const outputTokens = completion ?? 0
  const reasoningTokens = Number.isFinite(usage.completion_tokens_details?.reasoning_tokens)
    ? usage.completion_tokens_details.reasoning_tokens
    : undefined
  return {
    inputTokens,
    outputTokens,
    totalTokens: inputTokens + cached + outputTokens,
    ...(cached > 0 ? { cacheReadTokens: cached } : {}),
    ...(reasoningTokens === undefined ? {} : { reasoningTokens }),
  }
}

//#endregion

//#region catalog

/**
 * 目录：从网关 `/v1/models` 收敛出"只有 DeepSeek/GLM"的一份清单，并把同一逻辑
 * 模型在各渠道的出现收成候选列表。目录落盘缓存，网关没起来时也能渲染选择器。
 */
class Catalog {
  #gatewayBase
  #apiKeyFile
  #cacheFile
  #priority
  #log
  #entries = new Map()
  #loadedAt = 0
  #inflight

  constructor({ gatewayBase, apiKeyFile, cacheFile, priority, log }) {
    this.#gatewayBase = gatewayBase
    this.#apiKeyFile = apiKeyFile
    this.#cacheFile = cacheFile
    this.#priority = priority
    this.#log = log
  }

  get size() {
    return this.#entries.size
  }

  entries() {
    return [...this.#entries.values()]
  }

  get(key) {
    return this.#entries.get(key)
  }

  rankOf(provider) {
    const index = this.#priority.indexOf(provider)
    return index === -1 ? this.#priority.length + 1 : index
  }

  /** 磁盘缓存：网关不可用时选择器仍有内容。 */
  loadCache() {
    try {
      if (!existsSync(this.#cacheFile)) return
      const raw = JSON.parse(readFileSync(this.#cacheFile, 'utf8'))
      if (!Array.isArray(raw?.models)) return
      const entries = new Map()
      for (const model of raw.models) {
        if (typeof model?.key !== 'string' || !Array.isArray(model.candidates)) continue
        entries.set(model.key, model)
      }
      if (entries.size > 0) {
        this.#entries = entries
        this.#loadedAt = Number(raw.at) || Date.now()
      }
    } catch (error) {
      this.#log?.warn?.(`dsh-dsglm-only: catalog cache unreadable (${String(error)})`)
    }
  }

  saveCache() {
    try {
      mkdirSync(dirname(this.#cacheFile), { recursive: true })
      writeFileSync(
        this.#cacheFile,
        JSON.stringify({ at: this.#loadedAt, models: this.entries() }, null, 2),
        'utf8',
      )
    } catch (error) {
      this.#log?.warn?.(`dsh-dsglm-only: catalog cache unwritable (${String(error)})`)
    }
  }

  needsRefresh() {
    return Date.now() - this.#loadedAt > CATALOG_REFRESH_INTERVAL_MS
  }

  /** 拉取一次。失败时保留旧目录（绝不把选择器清空）。 */
  async refresh(force = false) {
    if (!force && !this.needsRefresh()) return
    if (this.#inflight !== undefined) return this.#inflight
    this.#inflight = this.#fetchCatalog().finally(() => {
      this.#inflight = undefined
    })
    return this.#inflight
  }

  async #fetchCatalog() {
    const key = readGatewayKey(this.#apiKeyFile)
    if (key === undefined) {
      this.#log?.warn?.('dsh-dsglm-only: gateway API key not found; keeping the cached catalog')
      return
    }
    let payload
    try {
      const response = await fetch(`${this.#gatewayBase}/models`, {
        headers: { authorization: `Bearer ${key}` },
        signal: AbortSignal.timeout(CATALOG_LOAD_TIMEOUT_MS),
      })
      if (!response.ok) {
        this.#log?.warn?.(`dsh-dsglm-only: gateway /models answered HTTP ${response.status}`)
        return
      }
      payload = await response.json()
    } catch (error) {
      this.#log?.warn?.(`dsh-dsglm-only: gateway /models unreachable (${String(error)})`)
      return
    }
    const list = Array.isArray(payload?.data) ? payload.data : []
    const grouped = new Map()
    for (const item of list) {
      if (typeof item?.id !== 'string' || item.id === '') continue
      const provider = providerOf(item.id, typeof item.owned_by === 'string' ? item.owned_by : undefined)
      if (provider === '') continue
      const modelId = modelIdOf(item.id)
      const name = cleanName(item.name, modelId)
      if (!isTargetEntry(provider, modelId, name)) continue
      // 归并键取展示名：各家渠道的 id 拼写不同，展示名才是同一个模型的共同标识。
      const key = logicalKey(name)
      if (key === '') continue
      const reasoning = reasoningOf(item)
      const contextWindow = positiveInt(item.context_window)
      let entry = grouped.get(key)
      if (entry === undefined) {
        entry = { key, name, candidates: [], contextWindow: undefined, input: undefined, reasoning: undefined }
        grouped.set(key, entry)
      }
      entry.candidates.push({ provider, model: modelId, name })
      if (entry.contextWindow === undefined && contextWindow !== undefined) entry.contextWindow = contextWindow
      if (entry.reasoning === undefined && reasoning !== undefined) entry.reasoning = reasoning
      if (Array.isArray(item.input)) entry.input = item.input
    }
    if (grouped.size === 0) {
      this.#log?.warn?.('dsh-dsglm-only: gateway returned no DeepSeek/GLM models; keeping the cached catalog')
      return
    }
    for (const entry of grouped.values()) {
      entry.candidates.sort((a, b) => this.rankOf(a.provider) - this.rankOf(b.provider))
      const best = entry.candidates[0]
      // 名称取首选渠道的写法（最干净的一个），候选内的 name 只作参考。
      entry.name = best.name
      entry.model = best.model
    }
    this.#entries = grouped
    this.#loadedAt = Date.now()
    this.saveCache()
    this.#log?.info?.(`dsh-dsglm-only: catalog refreshed with ${grouped.size} DeepSeek/GLM model(s)`)
  }
}

/** 网关在每条模型上给了 reasoning 能力，能解析就用，解析不了就不声明档位。 */
function reasoningOf(item) {
  const value = item?.reasoning
  if (value === null || typeof value !== 'object') return undefined
  const efforts = Array.isArray(value.efforts)
    ? value.efforts
      .filter((effort) => effort !== null && typeof effort === 'object' && typeof effort.id === 'string' && effort.id !== '')
      .map((effort) => ({
        id: effort.id,
        name: typeof effort.name === 'string' && effort.name !== '' ? effort.name : effort.id,
      }))
    : []
  if (efforts.length === 0) return undefined
  const available = new Set(efforts.map((effort) => effort.id))
  const fallback = efforts.some((effort) => effort.id === 'high') ? 'high' : efforts[0].id
  const preferred = typeof value.default === 'string' && available.has(value.default) ? value.default : fallback
  return { efforts, defaultEffort: preferred }
}

//#endregion

//#region adapter

class DsglmError extends Error {
  code

  constructor(message, code) {
    super(message)
    this.name = 'DsglmError'
    this.code = code
  }
}

class DsglmAdapter {
  #catalog
  #gatewayBase
  #apiKeyFile
  #providerName
  #maxCandidates
  #log

  constructor({ catalog, gatewayBase, apiKeyFile, providerName, maxCandidates, log }) {
    this.#catalog = catalog
    this.#gatewayBase = gatewayBase
    this.#apiKeyFile = apiKeyFile
    this.#providerName = providerName
    this.#maxCandidates = maxCandidates
    this.#log = log
  }

  providerInfo(provider) {
    return { id: provider, name: this.#providerName }
  }

  providerRetryPolicy() {
    return {
      mode: 'normal',
      maxRetries: 2,
      retryableCodes: ['RATE_LIMIT', 'NETWORK', 'TIMEOUT', 'SERVER', 'EMPTY_RESPONSE'],
      initialDelayMs: 800,
      maxDelayMs: 12_000,
      jitterRatio: 0.2,
    }
  }

  /** 本聚合路由只声明文本能力：图片在装配阶段已被投影成文本占位。 */
  imageRequestPricing() {
    return undefined
  }

  async listModels(provider) {
    if (this.#catalog.size === 0) await this.#catalog.refresh(true)
    else void this.#catalog.refresh().catch(() => {})
    return this.#catalog.entries().map((entry) => ({
      provider,
      id: entry.key,
      name: entry.name,
      inputModalities: ['text'],
    }))
  }

  resolveModel(provider, model) {
    const entry = this.#catalog.get(String(model))
    const contextWindow = positiveInt(entry?.contextWindow)
    return {
      provider,
      id: model,
      name: entry?.name ?? String(model),
      inputModalities: ['text'],
      ...(contextWindow === undefined ? {} : { context: { contextWindow } }),
      ...(entry?.reasoning === undefined
        ? {}
        : { reasoning: { efforts: entry.reasoning.efforts, defaultEffort: entry.reasoning.defaultEffort } }),
    }
  }

  /**
   * dsh-llm 走的是 `prepareCall` 返回的 `call.stream`，从不直接调顶层 `stream`。
   * 这里显式包一层，保证行为与契约一致。
   */
  async prepareCall(provider, model, signal) {
    if (this.#catalog.size === 0) {
      await this.#catalog.refresh(true)
    } else {
      void this.#catalog.refresh().catch(() => {})
    }
    void signal
    return {
      model: this.resolveModel(provider, model),
      stream: (options) => this.stream(options),
    }
  }

  /** 候选渠道：目录里同逻辑模型的全部来源，按优先级排序，截断到上限。 */
  #candidatesFor(model) {
    const key = String(model)
    const entry = this.#catalog.get(key)
    if (entry !== undefined && Array.isArray(entry.candidates) && entry.candidates.length > 0) {
      return entry.candidates.slice(0, this.#maxCandidates)
    }
    // 目录未加载或键未知：允许直接传 `<provider>/<model>` 透传，便于排障。
    const provider = providerOf(key)
    if (provider !== '') return [{ provider, model: modelIdOf(key) }]
    return []
  }

  /**
   * 一家渠道的一次尝试。产出 StreamChunk；失败时抛错，由外层决定是否换下一家。
   * `state` 跨候选共享，记录已发射的块，避免回退时重复 block-start。
   */
  async *#attempt(baseOptions, candidate, state) {
    const apiKey = readGatewayKey(this.#apiKeyFile)
    if (apiKey === undefined) {
      throw new DsglmError(
        `网关密钥不可读（期望 ${join(dshHome(), 'openai-gateway', 'api-key')}）。请确认 dsh-providers-hub 已启动。`,
        'MISSING_CREDENTIAL',
      )
    }

    const body = {
      model: `${candidate.provider}/${candidate.model}`,
      messages: toOpenAiMessages(baseOptions),
      stream: true,
    }
    const tools = toOpenAiTools(baseOptions.tools)
    if (tools !== undefined) body.tools = tools
    if (Number.isFinite(baseOptions.temperature)) body.temperature = baseOptions.temperature
    if (Number.isFinite(baseOptions.maxTokens) && baseOptions.maxTokens > 0) {
      body.max_tokens = Math.floor(baseOptions.maxTokens)
    }
    if (Array.isArray(baseOptions.stop) && baseOptions.stop.length > 0) body.stop = baseOptions.stop
    if (typeof baseOptions.reasoningEffort === 'string' && baseOptions.reasoningEffort !== '') {
      body.reasoning_effort = baseOptions.reasoningEffort
    }

    const response = await fetch(`${this.#gatewayBase}/chat/completions`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: `Bearer ${apiKey}`, accept: 'text/event-stream' },
      body: JSON.stringify(body),
      signal: baseOptions.signal,
    })

    if (!response.ok) {
      const detail = await response.text().catch(() => '')
      const code = response.status === 401 || response.status === 403
        ? 'AUTH'
        : response.status === 429 ? 'RATE_LIMIT' : `HTTP_${response.status}`
      throw new DsglmError(
        `${candidate.provider} 拒接本请求（HTTP ${response.status}）${detail === '' ? '' : `：${detail.slice(0, 400)}`}`,
        code,
      )
    }

    const reader = response.body?.getReader()
    if (reader === undefined) throw new DsglmError(`${candidate.provider} 返回了空响应体。`, 'EMPTY_RESPONSE')

    const decoder = new TextDecoder()
    const toolCalls = new Map()
    let buffer = ''
    let finished = false
    let finishKind = 'stop'
    let usage
    let sawPayload = false

    try {
      while (!finished) {
        const { value, done } = await reader.read()
        if (done === true) break
        buffer += decoder.decode(value, { stream: true })
        const parsed = parseSseFrames(buffer)
        buffer = parsed.rest
        for (const data of parsed.frames) {
          if (data === '' ) continue
          if (data === '[DONE]') {
            finished = true
            break
          }
          let payload
          try {
            payload = JSON.parse(data)
          } catch {
            continue
          }
          sawPayload = true
          if (payload?.error !== undefined && payload.error !== null) {
            const message = typeof payload.error.message === 'string' ? payload.error.message : '未知上游错误'
            const status = Number.isFinite(payload.error.status) ? payload.error.status : undefined
            const code = typeof payload.error.code === 'string' && payload.error.code !== ''
              ? payload.error.code
              : status === 429 ? 'RATE_LIMIT' : 'UPSTREAM'
            throw new DsglmError(`${candidate.provider}：${message}`, code)
          }

          const usageFrame = usageFrom(payload)
          if (usageFrame !== undefined) usage = usageFrame

          const choice = Array.isArray(payload?.choices) ? payload.choices[0] : undefined
          const delta = choice?.delta
          if (delta !== null && typeof delta === 'object') {
            if (typeof delta.reasoning_content === 'string' && delta.reasoning_content !== '') {
              for (const chunk of state.openReasoning()) {
                yield chunk
                state.emitted += 1
              }
              yield { type: 'reasoning-delta', index: REASONING_INDEX, text: delta.reasoning_content }
              state.emitted += 1
            }
            if (typeof delta.content === 'string' && delta.content !== '') {
              for (const chunk of state.openAnswer()) {
                yield chunk
                state.emitted += 1
              }
              state.answer += delta.content
              yield { type: 'text-delta', index: ANSWER_INDEX, text: delta.content }
              state.emitted += 1
            }
            if (Array.isArray(delta.tool_calls)) {
              for (const fragment of delta.tool_calls) {
                const upstreamIndex = Number.isInteger(fragment?.index) ? fragment.index : 0
                let record = toolCalls.get(upstreamIndex)
                if (record === undefined) {
                  record = {
                    index: FIRST_TOOL_INDEX + toolCalls.size,
                    id: typeof fragment?.id === 'string' && fragment.id !== ''
                      ? fragment.id
                      : `dsglm-call-${randomUUID()}`,
                    name: typeof fragment?.function?.name === 'string' ? fragment.function.name : '',
                    arguments: '',
                  }
                  toolCalls.set(upstreamIndex, record)
                  yield { type: 'block-start', index: record.index, blockType: 'tool-call' }
                  state.emitted += 1
                }
                if (record.name === '' && typeof fragment?.function?.name === 'string') {
                  record.name = fragment.function.name
                }
                const deltaArguments = typeof fragment?.function?.arguments === 'string'
                  ? fragment.function.arguments
                  : ''
                record.arguments += deltaArguments
                yield {
                  type: 'tool-call-delta',
                  index: record.index,
                  id: record.id,
                  ...(record.name === '' ? {} : { name: record.name }),
                  argumentsDelta: deltaArguments,
                }
                state.emitted += 1
              }
            }
          }

          if (typeof choice?.finish_reason === 'string' && choice.finish_reason !== '') {
            finishKind = choice.finish_reason === 'tool_calls'
              ? 'tool-calls'
              : choice.finish_reason === 'length' ? 'max-tokens' : 'stop'
          }
        }
      }
    } finally {
      await reader.cancel().catch(() => {})
      reader.releaseLock()
    }

    if (!sawPayload) {
      throw new DsglmError(`${candidate.provider} 没有返回任何可解析的流数据。`, 'EMPTY_RESPONSE')
    }

    // 收尾：先关已开的正文/推理块，再关工具块。usage 必须在 finish 之前。
    if (state.reasoningOpen) {
      yield { type: 'block-end', index: REASONING_INDEX, block: { type: 'reasoning', text: '' } }
      state.reasoningOpen = false
      state.emitted += 1
    }
    if (state.answerOpen) {
      yield { type: 'block-end', index: ANSWER_INDEX, block: { type: 'text', text: state.answer } }
      state.answerOpen = false
      state.emitted += 1
    }
    for (const record of toolCalls.values()) {
      yield {
        type: 'block-end',
        index: record.index,
        block: { type: 'tool-call', id: record.id, name: record.name, arguments: record.arguments },
      }
      state.emitted += 1
    }

    if (usage !== undefined) {
      yield { type: 'usage', usage }
      state.emitted += 1
    }
    yield {
      type: 'finish',
      reason: toolCalls.size > 0
        ? { kind: 'tool-calls' }
        : finishKind === 'max-tokens' ? { kind: 'max-tokens' } : { kind: 'stop' },
    }
  }

  /** 逐个候选回退；只有"一个 chunk 都还没发出去"时才允许换下一家。 */
  async *stream(options) {
    const candidates = this.#candidatesFor(options.model)
    if (candidates.length === 0) {
      throw new DsglmError(
        `模型 "${options.model}" 不在 DeepSeek/GLM 目录里。请刷新目录，或直接用 "<渠道>/<模型>" 形式指定。`,
        'UNKNOWN_MODEL',
      )
    }

    const state = {
      emitted: 0,
      answer: '',
      reasoningOpen: false,
      answerOpen: false,
      /**
       * 惰性开块：返回需要先发的 block-start。用闭包数组而不是生成器，
       * 调用点更好读，也不会在多次调用之间丢失状态。
       */
      openReasoning() {
        if (state.reasoningOpen) return []
        state.reasoningOpen = true
        return [{ type: 'block-start', index: REASONING_INDEX, blockType: 'reasoning' }]
      },
      openAnswer() {
        if (state.answerOpen) return []
        state.answerOpen = true
        return [{ type: 'block-start', index: ANSWER_INDEX, blockType: 'text' }]
      },
    }

    let lastError
    for (const candidate of candidates) {
      try {
        for await (const chunk of this.#attempt(options, candidate, state)) {
          yield chunk
        }
        return
      } catch (error) {
        // 已经吐过内容就别回退：重复的块会让会话记录自相矛盾。
        if (state.emitted > 0 || options.signal?.aborted === true) throw error
        lastError = error
        this.#log?.warn?.(`dsh-dsglm-only: 渠道 ${candidate.provider} 失败，换下一家（${String(error)}）`)
      }
    }
    throw lastError ?? new DsglmError('所有候选渠道都无法完成本次请求。', 'UPSTREAM')
  }
}

//#endregion

export { Catalog, DsglmAdapter, DsglmError, logicalKey, isTargetEntry, cleanName }

//#region plugin

export const name = 'dsh-dsglm-only'

/** 没有 LLM seam 就没有这条路由。 */
export const inject = ['llm']

export function apply(ctx, config) {
  const settings = config !== null && typeof config === 'object' ? config : {}
  if (settings.enabled === false) return

  if (ctx.llm === undefined || typeof ctx.llm.registerAdapter !== 'function') {
    ctx.logger?.error?.('dsh-dsglm-only: llm 服务不可用，无法注册 provider')
    return
  }

  const providerId = typeof settings.providerId === 'string' && settings.providerId !== ''
    ? settings.providerId
    : DEFAULT_PROVIDER_ID
  const providerName = typeof settings.providerName === 'string' && settings.providerName !== ''
    ? settings.providerName
    : DEFAULT_PROVIDER_NAME
  const gatewayBase = typeof settings.gatewayBaseUrl === 'string' && settings.gatewayBaseUrl !== ''
    ? settings.gatewayBaseUrl.replace(/\/+$/, '')
    : DEFAULT_GATEWAY_BASE
  const priority = Array.isArray(settings.providerPriority) && settings.providerPriority.length > 0
    ? settings.providerPriority.map((item) => String(item))
    : DEFAULT_PROVIDER_PRIORITY
  const maxCandidates = Number.isSafeInteger(settings.maxCandidates) && settings.maxCandidates > 0
    ? Math.min(settings.maxCandidates, 12)
    : 3

  const catalog = new Catalog({
    gatewayBase,
    apiKeyFile: settings.apiKeyFile,
    cacheFile: typeof settings.cacheFile === 'string' && settings.cacheFile !== ''
      ? settings.cacheFile
      : join(dshHome(), 'dsglm-only', 'catalog.json'),
    priority,
    log: ctx.logger,
  })
  catalog.loadCache()

  const adapter = new DsglmAdapter({
    catalog,
    gatewayBase,
    apiKeyFile: settings.apiKeyFile,
    providerName,
    maxCandidates,
    log: ctx.logger,
  })

  const registration = ctx.llm.registerAdapter([providerId], adapter)
  ctx.logger?.info?.(`dsh-dsglm-only: 已注册 provider "${providerId}"（目录缓存 ${catalog.size} 条）`)

  // 目录刷新：先拉一次，再按间隔持续刷新；模型清单变化时通知消费者重载。
  const timer = setInterval(() => {
    const before = catalog.size
    void catalog.refresh(true).then(() => {
      if (catalog.size !== before) ctx.emit('llm/models-updated', providerId)
    }).catch(() => {})
  }, CATALOG_REFRESH_INTERVAL_MS)
  timer.unref?.()

  // 首次启动时网关可能还没起来（providers-hub 与本站同一批加载），
  // 这里做有上限的短重试：拿到目录就停，绝不无限重试。
  let bootstrapTimer
  let bootstrapLeft = BOOTSTRAP_ATTEMPTS
  const bootstrap = () => {
    void catalog.refresh(true).then(() => {
      if (catalog.size > 0) {
        ctx.logger?.info?.(`dsh-dsglm-only: 目录就绪，${catalog.size} 条模型`)
        ctx.emit('llm/models-updated', providerId)
        return
      }
      if (bootstrapLeft-- > 0) {
        bootstrapTimer = setTimeout(bootstrap, BOOTSTRAP_INTERVAL_MS)
        bootstrapTimer.unref?.()
      } else {
        ctx.logger?.warn?.('dsh-dsglm-only: 网关始终没给出 DeepSeek/GLM 目录；保留空目录，等下一次定时刷新')
      }
    }).catch(() => {
      if (bootstrapLeft-- > 0) {
        bootstrapTimer = setTimeout(bootstrap, BOOTSTRAP_INTERVAL_MS)
        bootstrapTimer.unref?.()
      }
    })
  }
  bootstrap()

  // 路由注册与定时器都归本 fiber 所有，HMR 替换模块时一并释放。
  ctx.effect(() => () => {
    clearInterval(timer)
    clearTimeout(bootstrapTimer)
    registration()
  })
}

//#endregion
