// CryoBoost change plans in a pane: the agent's apply_changes_now opens the plan as tables, one per project, a row
// per job and a column per setting the plan changes. The arrows move through the cells, Enter edits one, Apply or
// Decline decides. The plan, its edits and the decision are the CryoBoost server's (api/plans.py, the pane's
// verbs, never listed as tools); this module draws what the server answers and sends the person's keys back.
//
// Where the pane cannot be shown (no server, a plan that deletes for good, a terminal too narrow), the call goes on
// to the server and its terminal dialog, as without this module.

import { atom, read, update } from 'claude-code'
import type { EngineInterface, Register } from 'claude-code'

import type { Cell, Column, PlanTable, ProjectTable, Row, View } from '../types'

const PANE = 'crboost-plan'
const TOOL = 'mcp__crboost__apply_changes_now'
const SERVER = 'crboost'
// How long one wait on the server lasts: a decision or an edit on the plan's page ends it at once.
const WAIT_S = 15

const table = atom({ plugin: 'crboost-plan', key: 'table' } as const, null as PlanTable | null, {
  shape: 'table-1',
})
const FRESH: View = { tab: 0, focus: null, editing: null, busy: null, note: null, isError: false, edits: [] }
const view = atom({ plugin: 'crboost-plan', key: 'view' } as const, FRESH, { shape: 'view-1' })

type Api = EngineInterface
type Server = { base: string; token: string }
type Answer = { success: boolean; error?: string; [key: string]: unknown }
type Edit = { project: string; job: string; kind: string; field: string; value: string }

let server: Server | null = null
// Ends the agent's waiting call with the person's decision; set while a plan is in the pane.
let settle: ((outcome: Answer) => void) | null = null
// The person pressed Apply or Decline and the server is carrying it out: the plan moving is their doing.
let deciding = false
// The first column drawn, per project: the window over the columns follows the focus with a column to spare.
const offsets = new Map<number, number>()

// ── The server ────────────────────────────────────────────────────────────────

async function connect($: Api): Promise<Server> {
  // The forwarder found the user's server and checked its port is the user's: ask it where that is.
  const info = await $.mcp.call(SERVER, 'get_server_info', {})
  const text = info.content.map(b => b.text ?? '').join('')
  const data = JSON.parse(text || '{}') as Answer & { port?: number }
  if (!data.success || typeof data.port !== 'number') {
    throw new Error(data.error ?? 'the crboost server did not say its port')
  }
  const home = await $.env.get('HOME')
  if (!home) throw new Error('HOME is not set')
  const token = (await $.fs.read(`${home}/.crboost/token`)).trim()

  return { base: `http://127.0.0.1:${data.port}/api/v1`, token }
}

async function api($: Api, srv: Server, verb: string, args: Record<string, unknown>): Promise<Answer> {
  try {
    const r = await $.http.fetch(`${srv.base}/${verb}`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${srv.token}`, 'Content-Type': 'application/json' },
      body: JSON.stringify(args),
    })
    try {
      return JSON.parse(r.text) as Answer
    } catch {
      return { success: false, error: `${verb}: HTTP ${r.status}` }
    }
  } catch (e) {
    return { success: false, error: `${verb}: the crboost server did not answer (${String(e)})` }
  }
}

const isTable = (a: Answer): boolean => typeof a.plan_id === 'string' && Array.isArray(a.projects)

// ── What the agent reads back ─────────────────────────────────────────────────

const edited = (edits: string[]): string => (edits.length ? ` They edited it first: ${edits.join('; ')}.` : '')

function applied(r: Answer, edits: string[]): Answer {
  return {
    ...r,
    message:
      `The user applied the plan in the crboost pane.${edited(edits)} ` +
      'It runs step by step in the background: follow it with get_plan.',
  }
}

function declined(url: unknown, edits: string[]): Answer {
  return {
    success: false,
    declined: true,
    ui_url: url,
    error:
      `The user declined the plan in the crboost pane: nothing changed.${edited(edits)} ` +
      'Ask them what to change, or leave it on its page.',
  }
}

function undecided(url: unknown, why: string): Answer {
  return {
    success: false,
    ui_url: url,
    error: `${why}: nothing changed. The plan stays proposed on its page (ui_url) until it expires; ask the user.`,
  }
}

// ── Waiting for the person ────────────────────────────────────────────────────

async function waitForDecision($: Api, srv: Server, planId: string, signal: AbortSignal): Promise<Answer> {
  type Woke = { kind: 'done'; outcome: Answer } | { kind: 'poll'; change: Answer }
  const decided = new Promise<Woke>(resolve => {
    settle = outcome => resolve({ kind: 'done', outcome })
  })
  const aborted = new Promise<Woke>(resolve => {
    const stop = async () =>
      resolve({ kind: 'done', outcome: undecided((await read($, table))?.ui_url, 'The call was interrupted') })
    if (signal.aborted) void stop()
    else signal.addEventListener('abort', () => void stop(), { once: true })
  })
  const first = await read($, table)
  if (first === null) return undecided(undefined, 'The pane lost the plan')
  // What the plan was at the last look: the wait asks the server for anything else.
  let known = { revision: first.revision, status: first.status }
  for (;;) {
    const t = await read($, table)
    if (t === null) return undecided(undefined, 'The pane lost the plan')
    // A server call stays in flight while the person thinks, so this hook's own clock does not run.
    const poll = api($, srv, 'get_plan_change', {
      plan_id: planId,
      revision: known.revision,
      status: known.status,
      wait_s: WAIT_S,
    }).then((change): Woke => ({ kind: 'poll', change }))
    const got = await Promise.race([decided, aborted, poll])
    if (got.kind === 'done') return got.outcome
    const change = got.change
    if (!change.success) return { ...change, ui_url: t.ui_url }
    if (!change.moved) continue
    known = { revision: Number(change.revision), status: String(change.status) }
    // The pane's own Apply or Decline moved it: its answer is on the way.
    if (deciding) continue
    const status = known.status
    if (status === 'approved') {
      // Approved on the plan's page while the pane was open: carried out as the page's approval is.
      const r = await api($, srv, 'apply_changes', { plan_id: planId, revision: change.revision })
      const edits = (await read($, view)).edits

      return r.success ? { ...applied(r, edits), message: `The user approved the plan on its page. ${r.message}` } : r
    }
    if (status === 'rejected') return declined(t.ui_url, (await read($, view)).edits)
    if (status !== 'proposed') return { success: false, ui_url: t.ui_url, error: `The plan is ${status} now.` }
    if (known.revision !== t.revision) {
      // Edited on the page: drawn again.
      const fresh = await api($, srv, 'get_plan_table', { plan_id: planId })
      if (fresh.success && isTable(fresh)) {
        await update($, table, () => fresh as unknown as PlanTable)
        await update($, view, v => ({ ...v, note: `edited on the page: revision ${fresh.revision}`, isError: false }))
      }
    }
  }
}

// ── The person's edits and decision ───────────────────────────────────────────

async function sendEdit($: Api, edit: Edit, words: string): Promise<void> {
  const t = await read($, table)
  if (t === null || server === null) return
  await update($, view, v => ({ ...v, busy: 'saving…', note: null }))
  const r = await api($, server, 'edit_plan', { plan_id: t.plan_id, revision: t.revision, edits: [edit] })
  if (isTable(r)) await update($, table, () => r as unknown as PlanTable)
  await update($, view, v => ({
    ...v,
    busy: null,
    editing: null,
    note: r.success ? `${words} · revision ${String(r.revision)}` : (r.error ?? 'not changed'),
    isError: !r.success,
    edits: r.success ? [...v.edits, words] : v.edits,
  }))
}

async function keepDeletion($: Api, step: number, what: string): Promise<void> {
  const t = await read($, table)
  if (t === null || server === null) return
  await update($, view, v => ({ ...v, busy: 'saving…', note: null }))
  const r = await api($, server, 'edit_plan', { plan_id: t.plan_id, revision: t.revision, keep: [step] })
  if (isTable(r)) await update($, table, () => r as unknown as PlanTable)
  const words = `kept ${what}`
  await update($, view, v => ({
    ...v,
    busy: null,
    note: r.success ? `${words} · revision ${String(r.revision)}` : (r.error ?? 'not changed'),
    isError: !r.success,
    edits: r.success ? [...v.edits, words] : v.edits,
  }))
}

async function decide($: Api, decision: 'apply' | 'reject'): Promise<void> {
  const t = await read($, table)
  if (t === null || server === null || settle === null || deciding) return
  deciding = true
  await update($, view, v => ({ ...v, busy: decision === 'apply' ? 'applying…' : 'declining…', note: null }))
  const r = await api($, server, 'decide_plan', { plan_id: t.plan_id, revision: t.revision, decision })
  const edits = (await read($, view)).edits
  if (r.success) {
    settle(decision === 'apply' ? applied(r, edits) : declined(t.ui_url, edits))

    return
  }
  deciding = false
  if (isTable(r)) {
    // The plan moved under the person (an edit on its page): drawn again, to be decided again.
    await update($, table, () => r as unknown as PlanTable)
  } else if (decision === 'apply' && r.error && !/revision|deletes something for good/.test(r.error)) {
    // Refused for good (a project changed since the proposal): the agent hears why.
    settle({ ...r, ui_url: t.ui_url })

    return
  }
  await update($, view, v => ({ ...v, busy: null, note: r.error ?? 'not done', isError: true }))
}

async function showTab($: Api, tab: number): Promise<void> {
  await update($, view, v => (v.tab === tab ? v : { ...v, tab, editing: null }))
}

// ── Layout ────────────────────────────────────────────────────────────────────

const NAME_MIN = 12
const NAME_MAX = 30
const COL_MIN = 5
const COL_MAX = 16
const GAP = 2
const MARK: Record<string, string> = { added: '+', cloned: '+', removed: '−', deleted: '−' }

function fit(text: string, width: number): string {
  if (width <= 0) return ''
  return text.length > width ? `${text.slice(0, Math.max(0, width - 1))}…` : text.padEnd(width)
}

function columnWidth(p: ProjectTable, c: Column): number {
  let w = c.label.length
  for (const r of p.rows) {
    const cell = r.cells[c.id]
    if (cell) w = Math.max(w, cell.after.length)
  }

  return Math.min(COL_MAX, Math.max(COL_MIN, w))
}

function nameWidth(p: ProjectTable): number {
  const longest = Math.max(0, ...p.rows.map(r => r.name.length + 2))

  return Math.min(NAME_MAX, Math.max(NAME_MIN, longest))
}

// The columns from `first` that fit in `room` cells: the index of the last one.
function lastFitting(widths: number[], first: number, room: number): number {
  let used = 0
  let last = first - 1
  for (let i = first; i < widths.length; i += 1) {
    used += (widths[i] ?? COL_MIN) + GAP
    if (used > room && i > first) break
    last = i
  }

  return last
}

// The first column drawn: where it was, moved just enough that the focused column shows with one to spare on
// each side, so the arrows always have a cell to step to while more columns lie that way.
function windowStart(widths: number[], previous: number, focused: number | null, room: number): number {
  let first = Math.min(previous, Math.max(0, widths.length - 1))
  if (focused === null) return first
  if (focused - 1 < first) first = Math.max(0, focused - 1)
  const want = Math.min(focused + 1, widths.length - 1)
  while (first < focused && lastFitting(widths, first, room) < want) first += 1

  return first
}

const cellKey = (row: number, col: string): string => `c|${row}|${col}`

function parseCell(key: string | null): { row: number; col: string } | null {
  if (!key?.startsWith('c|')) return null
  const [, row, ...col] = key.split('|')

  return { row: Number(row), col: col.join('|') }
}

function describe(t: PlanTable, p: ProjectTable | undefined, v: View): { head: string; body: string } {
  const cell = parseCell(v.focus)
  if (p && cell) {
    const r = p.rows[cell.row]
    const c = r?.cells[cell.col]
    const col = p.columns.find(x => x.id === cell.col)
    if (r && c && col) {
      const how = c.editable
        ? c.type === 'bool'
          ? 'Enter switches it'
          : 'Enter edits it'
        : r.frozen
          ? 'has run: clone it to change it'
          : 'not edited here'
      const kind = col.kind === 'slurm' ? ' (SLURM)' : col.kind === 'input' ? ' (input)' : ''
      const limits = [c.type, c.bounds, c.choices ? c.choices.join(' | ') : ''].filter(Boolean).join(', ')

      return {
        head: `${r.name} · ${c.label}${kind}: ${c.changed ? `${c.now} → ${c.after}` : `${c.after}, unchanged`}`,
        body: [c.help, limits, how].filter(Boolean).join(' · '),
      }
    }
  }
  if (p && v.focus?.startsWith('r|')) {
    const r = p.rows[Number(v.focus.slice(2))]
    if (r) {
      const changes = Object.values(r.cells)
        .filter(c => c.changed)
        .map(c => `${c.label} ${c.now} → ${c.after}`)

      return {
        head: `${r.name} · ${r.what} · ${r.status}${r.frozen ? ' · has run' : ''}`,
        body: changes.join('; ') || 'no setting changed',
      }
    }
  }
  if (p) return { head: `${p.name}${p.tag ? ` (${p.tag})` : ''}: ${p.summary}`, body: '' }

  return { head: `${t.steps.length} steps, in the order they run`, body: '' }
}

// ── Hooks ─────────────────────────────────────────────────────────────────────

export const register: Register = on => {
  on('session.start', async ($, e, next) => {
    await $.command.register({ name: 'crboost-plan', description: 'Bring back the CryoBoost plan pane' })

    return next(e)
  })

  on('command.run', { command: 'crboost-plan' }, async $ => {
    if ((await read($, table)) === null) return { text: 'No CryoBoost plan is waiting for you.' }
    await $.ui.open({ id: PANE, title: 'crboost plan', focus: true })

    return { text: 'The CryoBoost plan pane is open.' }
  })

  // The agent's apply_changes_now: the plan goes in the pane, and the call waits for the person's decision.
  on('tool.call', { tool: TOOL }, async ($, e, next) => {
    const args = e as unknown as { plan_id?: unknown; revision?: unknown }
    const planId = args.plan_id
    if (typeof planId !== 'string' || settle !== null) return next(e)
    let srv: Server
    try {
      srv = await connect($)
    } catch (err) {
      $.ui.log(`crboost-plan: no pane (${String(err)}); the terminal dialog instead`, { to: 'debug' })

      return next(e)
    }
    const t = await api($, srv, 'get_plan_table', { plan_id: planId })
    const asked = typeof args.revision === 'number' ? args.revision : null
    const shown = t as unknown as PlanTable
    // The server answers what the pane would not take: a plan gone, ended, edited since, or one that deletes
    // something for good (approved on its page).
    if (!t.success || !isTable(t) || !shown.editable || shown.permanent.length > 0) return next(e)
    if (asked !== null && asked !== shown.revision) return next(e)

    server = srv
    deciding = false
    offsets.clear()
    await update($, table, () => shown)
    await update($, view, () => ({ ...FRESH }))
    const rows = shown.projects.reduce((n, p) => Math.max(n, p.rows.length + p.deletions.length), 0)
    const opened = await $.ui.open({
      id: PANE,
      title: `crboost · ${shown.title}`,
      focus: true,
      rows: Math.min(40, Math.max(14, rows + 14)),
    })
    if (!opened.isPlaced) {
      await update($, table, () => null)

      return next(e)
    }
    let outcome: Answer
    try {
      outcome = await waitForDecision($, srv, planId, next.signal)
    } finally {
      settle = null
      deciding = false
      await update($, table, () => null)
      await $.ui.close({ id: PANE })
    }

    const result = [{ type: 'text', text: JSON.stringify(outcome) }]

    return outcome.success ? { result } : { result, isError: true as const }
  })

  // Closed by the person before deciding: the agent hears so, nothing changed.
  on('ui.close', { id: PANE }, async ($, e, next) => {
    if (settle !== null && !deciding && e.origin.kind === 'person') {
      settle(undecided((await read($, table))?.ui_url, 'The user closed the crboost pane without deciding'))
    }

    return next(e)
  })

  // The focus ring: the table's window follows it, and a project's tab shows that project.
  on('ui.focus', { requestId: PANE }, async ($, e, next) => {
    const key = e.element ?? null
    if (key?.startsWith('t|')) {
      const tab = Number(key.slice(2))
      await update($, view, v => (v.tab === tab ? { ...v, focus: key } : { ...v, tab, focus: key, editing: null }))
    } else {
      await update($, view, v => ({ ...v, focus: key }))
    }

    return next(e)
  })

  on('ui.render', { component: 'Pane', requestId: PANE }, async ($, e) => {
    const { Box, Text, Button, Input, Select } = $.ui.resolve(e)
    const t = await read($, table)
    const v = await read($, view)
    if (t === null) return <Text dimColor>No CryoBoost plan is waiting.</Text>

    const width = e.props.bodyColumns
    const quiet = v.editing !== null || v.busy !== null
    const hot = (k: string): string | undefined => (quiet ? undefined : k)
    const p: ProjectTable | undefined = t.projects[v.tab]
    const info = describe(t, p, v)

    const tabs = (
      <Box flexDirection="row" gap={1}>
        {t.projects.map((proj, i) => (
          <Button
            key={`t|${i}`}
            plain
            hotkey={i < 9 ? hot(String(i + 1)) : undefined}
            onPress={() => void showTab($, i)}
          >
            <Text bold={i === v.tab} inverse={i === v.tab}>
              {` ${proj.name} `}
            </Text>
          </Button>
        ))}
        <Button key={`t|${t.projects.length}`} plain hotkey={hot('s')} onPress={() => void showTab($, t.projects.length)}>
          <Text bold={v.tab === t.projects.length} inverse={v.tab === t.projects.length}>
            {` steps (${t.steps.length}) `}
          </Text>
        </Button>
      </Box>
    )

    const head = (
      <Box flexDirection="column">
        <Text>
          <Text bold>{t.title}</Text>
          <Text dimColor>{`  ${t.plan_id} · revision ${t.revision} · ${t.status} · until ${t.expires}`}</Text>
        </Text>
        {tabs}
      </Box>
    )

    const footer = (
      <Box flexDirection="column" marginTop={1}>
        <Text dimColor wrap="truncate-end">
          {info.head}
        </Text>
        <Text dimColor wrap="truncate-end">
          {info.body || ' '}
        </Text>
        <Box flexDirection="row" gap={2} marginTop={1}>
          <Button key="apply" hotkey={hot('a')} variant="primary" onPress={() => void decide($, 'apply')}>
            Apply
          </Button>
          <Button key="decline" hotkey={hot('d')} onPress={() => void decide($, 'reject')}>
            Decline
          </Button>
          <Text color={v.isError ? 'error' : undefined} dimColor={!v.isError} wrap="truncate-end">
            {v.busy ?? v.note ?? `edit on the page instead: ${t.ui_url}`}
          </Text>
        </Box>
        <Text dimColor wrap="truncate-end">
          ↑↓←→ move · Enter edit · 1–9 projects · s steps · a apply · d decline · Esc back to the prompt
        </Text>
      </Box>
    )

    if (p === undefined) {
      return (
        <Box flexDirection="column">
          {head}
          <Box flexDirection="column" marginTop={1}>
            {t.steps.map((s, i) => (
              <Button key={`s|${i}`} plain onPress={() => undefined}>
                <Text color={s.permanent ? 'error' : undefined}>{`${s.index + 1}. ${s.what}`}</Text>
                {s.note ? <Text dimColor>{`  (${s.note})`}</Text> : null}
              </Button>
            ))}
          </Box>
          {footer}
        </Box>
      )
    }

    // The table: a name column, then the window of setting columns that fits.
    const nameW = nameWidth(p)
    const widths = p.columns.map(c => columnWidth(p, c))
    const focusedCell = parseCell(v.focus)
    const focusedCol = focusedCell ? p.columns.findIndex(c => c.id === focusedCell.col) : -1
    const room = Math.max(10, width - nameW - 4)
    const first = windowStart(widths, offsets.get(v.tab) ?? 0, focusedCol >= 0 ? focusedCol : null, room)
    offsets.set(v.tab, first)
    const last = lastFitting(widths, first, room)
    const shownCols = p.columns.slice(first, last + 1)

    const editor = (() => {
      if (v.editing === null) return null
      const r: Row | undefined = p.rows[v.editing.row]
      const c: Cell | undefined = r?.cells[v.editing.col]
      const col = p.columns.find(x => x.id === v.editing?.col)
      if (!r || !c || !col) return null
      const commit = (value: string): void => {
        if (value === c.value) {
          void update($, view, w => ({ ...w, editing: null }))

          return
        }
        void sendEdit(
          $,
          { project: p.key, job: r.job, kind: col.kind, field: col.name, value },
          `${r.name} · ${c.label} ${c.after} → ${value}`,
        )
      }
      const label = `${r.name} · ${c.label}${c.bounds ? ` (${c.bounds})` : ''}`

      return (
        <Box flexDirection="row" gap={2} marginTop={1}>
          {c.choices ? (
            <Select
              key="edit"
              label={label}
              autoFocus
              value={c.value}
              options={c.choices.map(x => ({ value: x, label: x }))}
              onSelect={value => commit(value)}
            />
          ) : (
            <Input key="edit" label={label} autoFocus value={c.value} onSubmit={value => commit(value)} />
          )}
          <Button key="cancel" onPress={() => void update($, view, w => ({ ...w, editing: null }))}>
            Cancel
          </Button>
        </Box>
      )
    })()

    const press = (row: number, colId: string): void => {
      const r = p.rows[row]
      const c = r?.cells[colId]
      const col = p.columns.find(x => x.id === colId)
      if (!r || !c || !col || v.busy !== null) return
      if (!c.editable) {
        void update($, view, w => ({ ...w, note: `${r.name} · ${c.label}: not edited here`, isError: true }))

        return
      }
      if (c.type === 'bool') {
        const value = c.after === 'on' ? 'off' : 'on'
        void sendEdit(
          $,
          { project: p.key, job: r.job, kind: col.kind, field: col.name, value },
          `${r.name} · ${c.label} ${c.after} → ${value}`,
        )

        return
      }
      void update($, view, w => ({ ...w, editing: { row, col: colId }, note: null }))
    }

    const more = `${first > 0 ? '‹ ' : '  '}${p.columns.length ? `settings ${first + 1}–${last + 1} of ${p.columns.length}` : ''}${last < p.columns.length - 1 ? ' ›' : ''}`

    return (
      <Box flexDirection="column">
        {head}
        <Text dimColor wrap="truncate-end">
          {`${p.name}${p.tag ? ` (${p.tag})` : ''}: ${p.summary}`}
        </Text>
        {p.values.map(([label, value]) => (
          <Text wrap="truncate-end">
            <Text dimColor>{`  ${label}: `}</Text>
            {value}
          </Text>
        ))}
        <Box flexDirection="column" marginTop={1}>
          <Box flexDirection="row">
            <Text dimColor>{fit(more, nameW + 1)}</Text>
            {shownCols.map((c, i) => (
              <Text bold dimColor={c.kind !== 'param'}>
                {' '.repeat(GAP)}
                {fit(c.label, widths[first + i] ?? COL_MIN)}
              </Text>
            ))}
          </Box>
          {p.rows.map((r, ri) => {
            const gone = r.mark === 'removed' || r.mark === 'deleted'

            return (
              <Box flexDirection="row">
                <Button key={`r|${ri}`} plain onPress={() => undefined}>
                  <Text strikethrough={gone} dimColor={gone}>
                    {fit(`${MARK[r.mark] ?? ' '} ${r.name}`, nameW + 1)}
                  </Text>
                </Button>
                {shownCols.map((c, i) => {
                  const cell = r.cells[c.id]
                  const w = widths[first + i] ?? COL_MIN
                  if (!cell) {
                    return <Text dimColor>{`${' '.repeat(GAP)}${fit('·', w)}`}</Text>
                  }

                  return (
                    <Box flexDirection="row">
                      <Text>{' '.repeat(GAP)}</Text>
                      <Button key={cellKey(ri, c.id)} plain onPress={() => press(ri, c.id)}>
                        <Text
                          bold={cell.changed}
                          color={cell.changed ? 'claude' : undefined}
                          dimColor={!cell.changed || !cell.editable}
                          strikethrough={gone}
                        >
                          {fit(cell.after, w)}
                        </Text>
                      </Button>
                    </Box>
                  )
                })}
              </Box>
            )
          })}
        </Box>
        {editor}
        {p.deletions.length > 0 && (
          <Box flexDirection="column" marginTop={1}>
            <Text bold>Deletes</Text>
            {p.deletions.map(d => (
              <Box flexDirection="row" gap={2}>
                {d.permanent ? (
                  <Text color="error">FOR GOOD</Text>
                ) : (
                  <Button key={`k|${d.step}`} onPress={() => void keepDeletion($, d.step, d.what)}>
                    Keep
                  </Button>
                )}
                <Text wrap="truncate-end">
                  {`${d.what} · ${d.size}${d.permanent ? '' : ' · to Trash'}`}
                  {d.orphans.length ? <Text dimColor>{` · leaves without input: ${d.orphans.join(', ')}`}</Text> : null}
                </Text>
              </Box>
            ))}
          </Box>
        )}
        {p.run && (
          <Box flexDirection="column" marginTop={1}>
            <Text bold>
              {`Runs ${p.run.jobs.length} job${p.run.jobs.length === 1 ? '' : 's'}`}
              {p.run.held_by_review ? <Text dimColor>{`, held for the ${p.run.held_by_review} review`}</Text> : null}
            </Text>
            {p.run.jobs.map(j => (
              <Text dimColor wrap="truncate-end">
                {`  ${fit(j.job, nameW)} ${fit(j.partition, 10)} ${fit(j.gpus, 12)} ${fit(`${j.cpus} CPUs`, 9)} ${fit(j.mem, 7)} ${j.time}${j.waits_for_review ? '  (waits for review)' : ''}`}
              </Text>
            ))}
            {p.run.refusals.map(x => (
              <Text color="error" wrap="truncate-end">{`  refused: ${x}`}</Text>
            ))}
            {p.run.left_running.length > 0 && (
              <Text dimColor wrap="truncate-end">{`  left running: ${p.run.left_running.join(', ')}`}</Text>
            )}
          </Box>
        )}
        {p.also.length > 0 && (
          <Box flexDirection="column" marginTop={1}>
            <Text bold>Also, not previewed</Text>
            {p.also.map(x => (
              <Text dimColor wrap="truncate-end">{`  ${x}`}</Text>
            ))}
          </Box>
        )}
        {footer}
      </Box>
    )
  })
}
