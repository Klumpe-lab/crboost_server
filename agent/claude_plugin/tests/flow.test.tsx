import type { On } from 'claude-code'
import { expect, test } from 'claude-code/testing'

// The agent's apply_changes_now, a stand-in CryoBoost server, and the person's keys in the pane.

const TABLE = {
  plan_id: 'p-1',
  revision: 1,
  status: 'proposed',
  title: 'Rebin',
  expires: '14:05',
  ui_url: 'http://host:24300/agent/plan/p-1',
  editable: true,
  permanent: [],
  projects: [
    {
      key: '/data/a',
      name: 'a',
      tag: '',
      summary: '1 setting',
      values: [],
      columns: [{ id: 'param:bin_factor', kind: 'param', name: 'bin_factor', label: 'Binning' }],
      rows: [
        {
          job: 'tsReconstruct',
          name: 'Reconstruct',
          mark: 'kept',
          clone_of: null,
          status: 'Scheduled',
          frozen: false,
          what: '1 setting',
          cells: {
            'param:bin_factor': {
              now: '4',
              after: '2',
              changed: true,
              editable: true,
              type: 'int',
              choices: null,
              bounds: '',
              help: '',
              label: 'Binning',
              value: '2',
            },
          },
        },
      ],
      deletions: [],
      run: null,
      also: [],
    },
  ],
  steps: [],
}

const PANE = {
  plugin: 'crboost-plan',
  component: 'Pane',
  requestId: 'crboost-plan',
  props: {
    title: 'crboost',
    isFocused: true,
    bodyColumns: 100,
    placement: 'inline',
    scroll: { offset: 0, bodyRows: 30 },
    view: {},
  },
} as const

type Sent = { verb: string; body: Record<string, unknown> }

// The server: answers each verb; get_plan_change waits until the plan moves, as the real one does.
function server(on: On, sent: Sent[]): void {
  let moved: (() => void) | null = null
  const changed = new Promise<void>(resolve => {
    moved = resolve
  })
  let table: Record<string, unknown> = { success: true, ...TABLE }
  on('mcp.call', () => ({
    value: { content: [{ type: 'text', text: JSON.stringify({ success: true, port: 24300 }) }], isError: false },
  }))
  on('env.get', () => ({ value: '/home/someone' }))
  on('fs.read', () => ({ value: 'TOKEN\n' }))
  on('ui.open', () => ({ value: { isPlaced: true } }))
  on('ui.close', () => ({ value: undefined }))
  on('http.fetch', async (_, e) => {
    const verb = e.url.split('/').pop() ?? ''
    const body = JSON.parse(String(e.init?.body ?? '{}')) as Record<string, unknown>
    sent.push({ verb, body })
    let answer: Record<string, unknown> = { success: true }
    if (verb === 'get_plan_table') answer = table
    if (verb === 'get_plan_change') {
      await changed
      answer = { success: true, moved: true, revision: table.revision, status: 'running' }
    }
    if (verb === 'edit_plan') {
      table = { ...table, revision: 2 }
      answer = table
    }
    if (verb === 'decide_plan') {
      answer =
        body.decision === 'apply'
          ? { success: true, task_id: 't-9', plan_id: 'p-1', message: 'Applying' }
          : { success: true, status: 'rejected' }
      moved?.()
    }

    return { value: { status: 200, ok: true, headers: {}, text: JSON.stringify(answer) } }
  })
}

test('Apply in the pane answers the agent with the run', async ($, on) => {
  const sent: Sent[] = []
  server(on, sent)
  const call = $.tool.call({ tool: 'mcp__crboost__apply_changes_now', plan_id: 'p-1', revision: 1 })
  // Wait for the pane to have the plan.
  for (let i = 0; i < 50 && !sent.some(s => s.verb === 'get_plan_change'); i += 1) await Promise.resolve()
  const ui = await $.ui.mount({ ...PANE, surface: 'terminal' })
  expect(await ui.find({ key: 'c|0|param:bin_factor' })).toBeDefined()

  await ui.press({ key: 'apply' })
  const r = await call
  const text = JSON.stringify(r)
  expect(text).toContain('applied the plan in the crboost pane')
  expect(text).toContain('t-9')
  expect(sent.find(s => s.verb === 'decide_plan')?.body).toEqual({ plan_id: 'p-1', revision: 1, decision: 'apply' })
  await ui.unmount()
})

test('Decline answers the agent that nothing changed', async ($, on) => {
  const sent: Sent[] = []
  server(on, sent)
  const call = $.tool.call({ tool: 'mcp__crboost__apply_changes_now', plan_id: 'p-1' })
  for (let i = 0; i < 50 && !sent.some(s => s.verb === 'get_plan_change'); i += 1) await Promise.resolve()
  const ui = await $.ui.mount({ ...PANE, surface: 'terminal' })

  await ui.press({ key: 'decline' })
  const text = JSON.stringify(await call)
  expect(text).toContain('declined the plan in the crboost pane')
  await ui.unmount()
})

test('an edit goes to the server and is told to the agent', async ($, on) => {
  const sent: Sent[] = []
  server(on, sent)
  const call = $.tool.call({ tool: 'mcp__crboost__apply_changes_now', plan_id: 'p-1' })
  for (let i = 0; i < 50 && !sent.some(s => s.verb === 'get_plan_change'); i += 1) await Promise.resolve()
  const ui = await $.ui.mount({ ...PANE, surface: 'terminal' })

  await ui.press({ key: 'c|0|param:bin_factor' })
  await ui.input({ key: 'edit', text: '3' })
  expect(sent.find(s => s.verb === 'edit_plan')?.body).toEqual({
    plan_id: 'p-1',
    revision: 1,
    edits: [{ project: '/data/a', job: 'tsReconstruct', kind: 'param', field: 'bin_factor', value: '3' }],
  })
  await ui.press({ key: 'apply' })
  const text = JSON.stringify(await call)
  expect(text).toContain('Reconstruct · Binning 2 → 3')
  expect(sent.find(s => s.verb === 'decide_plan')?.body).toEqual({ plan_id: 'p-1', revision: 2, decision: 'apply' })
  await ui.unmount()
})

test('no server: the call goes on to the terminal dialog', async ($, on) => {
  on('mcp.call', () => ({
    value: {
      content: [{ type: 'text', text: JSON.stringify({ success: false, error: 'no server' }) }],
      isError: false,
    },
  }))
  on('tool.call', () => ({ result: [{ type: 'text', text: 'the dialog' }] }))
  const r = await $.tool.call({ tool: 'mcp__crboost__apply_changes_now', plan_id: 'p-1' })
  expect(JSON.stringify(r)).toContain('the dialog')
})
