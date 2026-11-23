import type { On } from 'claude-code'
import { expect, test } from 'claude-code/testing'

import type { PlanTable, View } from '../types'

const cell = (now: string, after: string, extra: Record<string, unknown> = {}) => ({
  now,
  after,
  changed: now !== after,
  editable: true,
  type: 'int',
  choices: null,
  bounds: '≥ 1',
  help: 'Binning of the tomogram',
  label: 'Binning',
  value: after,
  ...extra,
})

const TABLE: PlanTable = {
  plan_id: 'p-1234',
  revision: 1,
  status: 'proposed',
  title: 'Rebin and rerun',
  expires: '14:05',
  ui_url: 'http://host:24300/agent/plan/p-1234',
  editable: true,
  permanent: [],
  projects: [
    {
      key: '/data/proj_a',
      name: 'proj_a',
      tag: '',
      summary: '+1 job, 3 settings',
      values: [],
      columns: [
        { id: 'param:bin_factor', kind: 'param', name: 'bin_factor', label: 'Binning' },
        { id: 'param:mode', kind: 'param', name: 'mode', label: 'Mode' },
        { id: 'slurm:time', kind: 'slurm', name: 'time', label: 'Time' },
      ],
      rows: [
        {
          job: 'tsReconstruct',
          name: 'Reconstruct',
          mark: 'kept',
          clone_of: null,
          status: 'Scheduled',
          frozen: false,
          what: '2 settings',
          cells: {
            'param:bin_factor': cell('4', '2'),
            'param:mode': cell('fast', 'slow', { type: 'choice', choices: ['fast', 'slow'], label: 'Mode', bounds: '' }),
            'slurm:time': cell('2:00:00', '2:00:00', { type: 'text', label: 'Time', bounds: '' }),
          },
        },
        {
          job: 'tsCtf',
          name: 'CTF',
          mark: 'added',
          clone_of: null,
          status: 'Scheduled',
          frozen: false,
          what: 'added',
          cells: { 'param:bin_factor': cell('8', '8') },
        },
      ],
      deletions: [{ what: 'job012 (Denoise)', size: '1.2 GB', permanent: false, step: 3, orphans: [] }],
      run: {
        jobs: [
          { job: 'tsCtf', partition: 'g', gpus: 'gpu:1', cpus: '8', mem: '32G', time: '1:00:00', waits_for_review: false },
        ],
        held_by_review: null,
        refusals: [],
        left_running: [],
      },
      also: [],
    },
    {
      key: '/data/proj_b',
      name: 'proj_b',
      tag: 'new project',
      summary: 'new project',
      values: [['Pixel size (Å)', '1.5']],
      columns: [],
      rows: [],
      deletions: [],
      run: null,
      also: ['Import 12 tilt series'],
    },
  ],
  steps: [{ index: 0, what: 'Set Binning of Reconstruct to 2', note: null, permanent: false, state: 'pending' }],
}

const FRESH: View = { tab: 0, focus: null, editing: null, busy: null, note: null, isError: false, edits: [] }

// What the pane finds stored when it first draws: the plan the tool call put there.
function seed(on: On): void {
  const first: Record<string, unknown> = {
    table: { shape: 'table-1', value: TABLE },
    view: { shape: 'view-1', value: FRESH },
  }
  on('state.get', async (_, e, next) => {
    // A call on `$` is answered `{ value }`: here the read, `{ value, version }`.
    const read = await next(e)
    const start = first[e.key]
    if (read.value?.value !== undefined || start === undefined) return read

    return { value: { value: start, version: 0 } } as typeof read
  })
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

test('the plan draws as a table, and Enter on a cell opens its editor', async ($, on) => {
  seed(on)
  for (const surface of ['terminal'] as const) {
    const ui = await $.ui.mount({ ...PANE, surface })

    expect(await ui.find({ key: 'c|0|param:bin_factor' })).toBeDefined()
    expect(await ui.find({ key: 'c|1|param:bin_factor' })).toBeDefined()
    // CTF has no Mode: no cell to stop on.
    expect(await ui.find({ key: 'c|1|param:mode' })).toBeUndefined()
    expect(await ui.find({ key: 'apply' })).toBeDefined()
    expect(await ui.find({ key: 'k|3' })).toBeDefined()

    await ui.press({ key: 'c|0|param:bin_factor' })
    expect(await ui.find({ key: 'edit' })).toBeDefined()
    await ui.press({ key: 'cancel' })
    expect(await ui.find({ key: 'edit' })).toBeUndefined()

    // A choice opens as a pick.
    await ui.press({ key: 'c|0|param:mode' })
    expect((await ui.find({ key: 'edit' }))?.type).toBe('Select')
    await ui.press({ key: 'cancel' })

    // The second project: a new one, its values, no table.
    await ui.press({ key: 't|1' })
    expect(await ui.find({ type: 'Text', text: /Pixel size/ })).toBeDefined()
    // The steps.
    await ui.press({ key: 't|2' })
    expect(await ui.find({ key: 's|0' })).toBeDefined()
    await ui.unmount()
  }
})

test('a narrow pane draws a window of the columns', async ($, on) => {
  seed(on)
  const ui = await $.ui.mount({ ...PANE, surface: 'terminal', props: { ...PANE.props, bodyColumns: 40 } })

  expect(await ui.find({ key: 'c|0|param:bin_factor' })).toBeDefined()
  expect(await ui.find({ key: 'c|0|slurm:time' })).toBeUndefined()
  await ui.unmount()
})
