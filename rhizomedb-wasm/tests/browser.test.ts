import init, { setPanicHook, Rhizome, InputTuple } from '../lib/browser/rhizomedb_wasm.js'
import { runRhizomeTest } from "./rhizomedb/rhizomedb.test.js"

before(async () => {
  await init()

  setPanicHook();
})

runRhizomeTest({
  runner: { describe, it },
  rhizome: {
    Rhizome,
    InputTuple
  }
})
