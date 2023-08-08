<div align="center">
  <a href="https://github.com/rhizomedb/rs-rhizome" target="_blank">
    <img src="https://raw.githubusercontent.com/rhizomedb/rs-rhizome/main/assets/a_logo.png" alt="rhizome Logo" width="100"></img>
  </a>

  <h1 align="center">rhizomedb-wasm</h1>

  <p>
    <a href="https://crates.io/crates/rhizomedb-wasm">
      <img src="https://img.shields.io/crates/v/rhizomedb-wasm?label=crates" alt="Crate">
    </a>
    <a href="https://npmjs.com/package/rhizomedb">
      <img src="https://img.shields.io/npm/v/rhizomedb" alt="npm">
    </a>
    <a href="https://codecov.io/gh/rhizomedb/rs-rhizome">
      <img src="https://codecov.io/gh/rhizomedb/rs-rhizome/branch/main/graph/badge.svg?token=SOMETOKEN" alt="Code Coverage"/>
    </a>
    <a href="https://github.com/rhizomedb/rs-rhizome/actions?query=">
      <img src="https://github.com/rhizomedb/rs-rhizome/actions/workflows/tests_and_checks.yml/badge.svg" alt="Build Status">
    </a>
    <a href="https://github.com/rhizomedb/rs-rhizome/blob/main/LICENSE-APACHE">
      <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License-Apache">
    </a>
    <a href="https://github.com/rhizomedb/rs-rhizome/blob/main/LICENSE-MIT">
      <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License-MIT">
    </a>
    <a href="https://docs.rs/rhizomedb">
      <img src="https://img.shields.io/static/v1?label=Docs&message=docs.rs&color=blue" alt="Docs">
    </a>
    <a href="https://fission.codes/discord">
      <img src="https://img.shields.io/static/v1?label=Discord&message=join%20us!&color=mediumslateblue" alt="Discord">
    </a>
  </p>
</div>

<div align="center"><sub>:warning: Work in progress :warning:</sub></div>

## rhizomedb-wasm

WebAssembly bindings for Rhizome.

## Outline

- [rhizomedb-wasm](#rhizomedb-wasm)
- [Outline](#outline)
- [Set-up](#set-up)
  - [Build for Javascript](#build-for-javascript)
- [Testing the Project](#testing-the-project)
- [License](#license)
  - [Contribution](#contribution)

### Build for Javascript

The `npm run build` command will compile the code in this directory into
Wasm and generate a `lib` folder, containing for each target, the Wasm binary, type declarations, and a Javascript-wrapper

  ```console
  npm run build
  ```

## Testing the Project

For running tests, use the following command:

```console
npm run test
```

## License

This project is licensed under either of

- Apache License, Version 2.0, ([LICENSE-APACHE](./LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](./LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.


[apache]: https://www.apache.org/licenses/LICENSE-2.0
[mit]: http://opensource.org/licenses/MIT
[node-js]: https://nodejs.dev/en/
[npm]: https://www.npmjs.com/
