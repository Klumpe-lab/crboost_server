// The plan as the CryoBoost server's get_plan_table answers it (services/agent_plan_table.py), and the pane's view.

export type Cell = {
  now: string
  after: string
  changed: boolean
  editable: boolean
  type: string
  choices: string[] | null
  bounds: string
  help: string
  label: string
  value: string
}

export type Column = { id: string; kind: string; name: string; label: string }

export type Row = {
  job: string
  name: string
  mark: string
  clone_of: string | null
  status: string
  frozen: boolean
  what: string
  cells: Record<string, Cell>
}

export type DeletionRow = { what: string; size: string; permanent: boolean; step: number; orphans: string[] }

export type RunJob = {
  job: string
  partition: string
  gpus: string
  cpus: string
  mem: string
  time: string
  waits_for_review: boolean
}

export type Run = { jobs: RunJob[]; held_by_review: string | null; refusals: string[]; left_running: string[] }

export type ProjectTable = {
  key: string
  name: string
  tag: string
  summary: string
  values: string[][]
  columns: Column[]
  rows: Row[]
  deletions: DeletionRow[]
  run: Run | null
  also: string[]
}

export type StepRow = { index: number; what: string; note: string | null; permanent: boolean; state: string }

export type PlanTable = {
  plan_id: string
  revision: number
  status: string
  title: string
  expires: string
  ui_url: string
  editable: boolean
  permanent: string[]
  projects: ProjectTable[]
  steps: StepRow[]
}

export type Editing = { row: number; col: string }

export type View = {
  // The project shown; one past the last is the plan's steps.
  tab: number
  // The key of the element holding the pane's focus ring.
  focus: string | null
  editing: Editing | null
  busy: string | null
  note: string | null
  isError: boolean
  // What the person changed, in words, for the agent.
  edits: string[]
}

declare module 'claude-code' {
  interface PluginState {
    'crboost-plan': { table: Shaped<PlanTable | null>; view: Shaped<View> }
  }
}
