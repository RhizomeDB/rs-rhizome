import { beforeAll, describe, it } from 'vitest'

import { setPanicHook, Rhizome, InputTuple } from '../lib/node/rhizome_wasm.js'
import { runRhizomeTest } from "./rhizome/rhizome.test.js"

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
