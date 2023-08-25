// This entry point is inserted into ./lib/workerd to support Cloudflare workers

import WASM from "./rhizomedb_wasm_bg.wasm";
import { initSync } from "./rhizomedb_wasm.js";
initSync(WASM);
export * from "./rhizomedb_wasm.js";
