import { beforeAll, describe, it } from 'vitest'

import { setPanicHook, Rhizome, InputTuple } from '../lib/node/rhizomedb_wasm.js'
import { runRhizomeTest } from "./rhizomedb/rhizomedb.test.js"

beforeAll(async () => {
  setPanicHook();
})

runRhizomeTest({
  runner: { describe, it },
  rhizome: {
    Rhizome,
    InputTuple
  }
})
