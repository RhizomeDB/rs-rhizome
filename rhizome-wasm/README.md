<div align="center">
  <a href="https://github.com/rhizomedb/rs-rhizome" target="_blank">
    <img src="https://raw.githubusercontent.com/rhizomedb/rs-rhizome/main/assets/a_logo.png" alt="rhizome Logo" width="100"></img>
  </a>

  <h1 align="center">rhizome-wasm</h1>

  <p>
    <a href="https://crates.io/crates/rhizome-wasm">
      <img src="https://img.shields.io/crates/v/rhizome-wasm?label=crates" alt="Crate">
    </a>
    <a href="https://npmjs.com/package/rhizome">
      <img src="https://img.shields.io/npm/v/rhizome" alt="npm">
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
    <a href="https://docs.rs/rhizome">
      <img src="https://img.shields.io/static/v1?label=Docs&message=docs.rs&color=blue" alt="Docs">
    </a>
    <a href="https://fission.codes/discord">
      <img src="https://img.shields.io/static/v1?label=Discord&message=join%20us!&color=mediumslateblue" alt="Discord">
    </a>
  </p>
</div>

<div align="center"><sub>:warning: Work in progress :warning:</sub></div>

## rhizome-wasm

WebAssembly bindings for Rhizome.

## Outline

- [rhizome-wasm](#rhizome-wasm)
- [Outline](#outline)
- [Set-up](#set-up)
  - [Build for Javascript](#build-for-javascript)
- [Testing the Project](#testing-the-project)
- [Publishing a Package](#publishing-a-package)
- [License](#license)
  - [Contribution](#contribution)

## Set-up

We'll use [`wasm-pack`][wasm-pack] for building, testing, and publishing
our Wasm project.

### Build for Javascript

The `wasm-pack build` command will compile the code in this directory into
Wasm and generate a `pkg` folder by default, containing the Wasm binary, a
Javascript-wrapper file, the rhizome-wasm README (and version), and a
`package.json` file.

- Targetting node:

  ```console
  wasm-pack build --target nodejs
  ```

- Targetting browswers:

  ```console
  wasm-pack build --target web
  ```

- Targetting bundlers like [webpack][webpack]:

  ```console
  wasm-pack build --target bundler
  ```

## Testing the Project

For running tests in the current directory, use one of these commands:

- Run tests expected to execute in [Node.js][node-js]:

```console
wasm-pack test --node
```

- Run browser tests in a headless browwer:

```console
wasm-pack test --headless --firefox --chrome --safari
```

*Note*: Make sure you have the appropriate browser installed when running
locally.

## Publishing a Package

Once you've [built the package](#build-for-javascript), which lives under
`pkg` by default (or a sub-directory of your choosing), you can pack and
publish it to [npm][npm] via (given credentials):

```console
wasm-pack publish
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
[wasm-pack]: https://rustwasm.github.io/docs/wasm-pack/
[webpack]: https://webpack.js.org/
