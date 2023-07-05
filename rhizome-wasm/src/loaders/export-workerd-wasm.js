// This entry point is inserted into ./lib/workerd to support Cloudflare workers

import WASM from "./rhizome_wasm_bg.wasm";
import { initSync } from "./rhizome_wasm.js";
initSync(WASM);
export * from "./rhizome_wasm.js";
