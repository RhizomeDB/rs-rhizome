<div align="center">
  <a href="https://github.com/fission-codes/rs-rhizome" target="_blank">
    <img src="https://github.com/fission-codes/kit/blob/main/images/logo-icon-coloured.png?raw=true" alt="Fission logo" width="100" />
  </a>

  <h1 align="center">rhizome</h1>

  <p>
    <a href="https://crates.io/crates/rhizome">
      <img src="https://img.shields.io/crates/v/rhizome?label=crates" alt="Crate">
    </a>
    <a href="https://npmjs.com/package/rhizome">
      <img src="https://img.shields.io/npm/v/rhizome" alt="Npm">
    </a>
    <a href="https://codecov.io/gh/fission-codes/rs-rhizome">
      <img src="https://codecov.io/gh/fission-codes/rs-rhizome/branch/main/graph/badge.svg?token=SOMETOKEN" alt="Code Coverage"/>
    </a>
    <a href="https://github.com/fission-codes/rs-rhizome/actions?query=">
      <img src="https://github.com/fission-codes/rs-rhizome/actions/workflows/tests_and_checks.yml/badge.svg" alt="Build Status">
    </a>
    <a href="https://github.com/fission-codes/rs-rhizome/blob/main/LICENSE-APACHE">
      <img src="https://img.shields.io/badge/License-Apache%202.0-blue.svg" alt="License-Apache">
    </a>
    <a href="https://github.com/fission-codes/rs-rhizome/blob/main/LICENSE-MIT">
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

## Rhizome

Rhizome is an in-development database for use in building local-first applications over a content addressable store, like IPFS.

## Outline

- [Outline](#outline)
- [Crates](#crates)
- [Usage](#usage)
- [Testing the Project](#testing-the-project)
- [Benchmarking the Project](#benchmarking-the-project)
- [Setting-up rhizome-wasm](#setting-up-rhizome-wasm)
- [Contributing](#contributing)
  - [Nix](#nix)
  - [Formatting](#formatting)
  - [Pre-commit Hook](#pre-commit-hook)
  - [Recommended Development Flow](#recommended-development-flow)
  - [Conventional Commits](#conventional-commits)
- [Getting Help](#getting-help)
- [External Resources](#external-resources)
- [License](#license)
  - [Contribution](#contribution)

## Crates

- [rhizome](https://github.com/fission-codes/rs-rhizome/tree/main/rhizome)
- [rhizome-wasm](https://github.com/fission-codes/rs-rhizome/tree/main/)

## Usage

- Add the following to the `[dependencies]` section of your `Cargo.toml` file
  for using the rust-only `rhizome` crate/workspace:

```toml
rhizome = "0.1.0"
```

- Add the following to the `[dependencies]` section of your `Cargo.toml` file
  for using `rhizome-wasm` crate/workspace:

```toml
rhizome-wasm = "0.1.0"
```

## Testing the Project

- Run tests for crate/workspace `rhizome`:

  ```console
  cd rhizome && cargo test --all-features
  ```

- To run tests for crate/workspace `rhizome-wasm`, follow
  the instructions in [rhizome-wasm](./rhizome-wasm#testing-the-project),
  which leverages [wasm-pack][wasm-pack].

## Benchmarking the Project

For benchmarking and measuring performance, this workspaces provides
a Rust-specific benchmarking package leveraging [criterion][criterion] and a
`test_utils` feature flag for integrating [proptest][proptest] within the
suite for working with [strategies][strategies] and sampling from randomly
generated values.

- Run benchmarks

  ```console
  cargo bench -p rhizome-benches
  ```

*Note*: Currently, this workspace only supports Rust-native benchmarking, as
`wasm-bindgen` support for criterion is still [an open issue][criterion-bindgen].
However, with some extra work, benchmarks can be compiled to [wasi][wasi] and
run with [wasmer][wasmer]/[wasmtime][wasmtime] or in the brower with
[webassembly.sh][wasmsh]. Please catch-up with wasm support for criterion on their
[user-guide][criterion-user-guide].

## Setting-up rhizome-wasm

The Wasm targetted version of this project relies on [wasm-pack][wasm-pack]
for building, testing, and publishing artifacts sutiable for
[Node.js][node-js], web broswers, or bundlers like [webpack][webpack].

Please read more on working with `wasm-pack` directly in
[rhizome-wasm](./rhizome-wasm#set-up).

## Contributing

:balloon: We're thankful for any feedback and help in improving our project!
We have a [contributing guide](./CONTRIBUTING.md) to help you get involved. We
also adhere to our [Code of Conduct](./CODE_OF_CONDUCT.md).

### Nix
This repository contains a [Nix flake][nix-flake] that initiates both the Rust
toolchain set in [rust-toolchain.toml](./rust-toolchain.toml) and a
[pre-commit hook](#pre-commit-hook). It also installs helpful cargo binaries for
development. Please install [nix][nix] and [direnv][direnv] to get started.

Run `nix develop` or `direnv allow` to load the `devShell` flake output,
according to your preference.

### Formatting

For formatting Rust in particular, please use `cargo +nightly fmt` as it uses
specific nightly features we recommend by default.

### Pre-commit Hook

This library recommends using [pre-commit][pre-commit] for running pre-commit
hooks. Please run this before every commit and/or push.

- If you are doing interim commits locally, and for some reason if you _don't_
  want pre-commit hooks to fire, you can run
  `git commit -a -m "Your message here" --no-verify`.

### Recommended Development Flow

- We recommend leveraging [cargo-watch][cargo-watch],
  [cargo-expand][cargo-expand] and [irust][irust] for Rust development.
- We recommend using [cargo-udeps][cargo-udeps] for removing unused dependencies
  before commits and pull-requests.

### Conventional Commits

This project *lightly* follows the [Conventional Commits
convention][commit-spec-site] to help explain
commit history and tie in with our release process. The full specification
can be found [here][commit-spec]. We recommend prefixing your commits with
a type of `fix`, `feat`, `docs`, `ci`, `refactor`, etc..., structured like so:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

## Getting Help

For usage questions, usecases, or issues reach out to us in our [Discord channel](https://fission.codes/discord).

We would be happy to try to answer your question or try opening a new issue on Github.

## External Resources

These are references to specifications, talks and presentations, etc.

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
[cargo-expand]: https://github.com/dtolnay/cargo-expand
[cargo-udeps]: https://github.com/est31/cargo-udeps
[cargo-watch]: https://github.com/watchexec/cargo-watch
[commit-spec]: https://www.conventionalcommits.org/en/v1.0.0/#specification
[commit-spec-site]: https://www.conventionalcommits.org/
[criterion]: https://github.com/bheisler/criterion.rs
[criterion-bindgen]: https://github.com/bheisler/criterion.rs/issues/270
[direnv]:https://direnv.net/
[irust]: https://github.com/sigmaSd/IRust
[mit]: http://opensource.org/licenses/MIT
[nix]:https://nixos.org/download.html
[nix-flake]: https://nixos.wiki/wiki/Flakes
[node-js]: https://nodejs.dev/en/
[pre-commit]: https://pre-commit.com/
[proptest]: https://github.com/proptest-rs/proptest
[strategies]: https://docs.rs/proptest/latest/proptest/strategy/trait.Strategy.html
[criterion-user-guide]: https://github.com/bheisler/criterion.rs/blob/version-0.4/book/src/user_guide/wasi.md
[wasi]: https://wasi.dev/
[wasmer]: https://wasmer.io/
[wasmtime]: https://docs.wasmtime.dev/
[wasmsh]: https://webassembly.sh/
[wasm-pack]: https://rustwasm.github.io/docs/wasm-pack/
[webpack]: https://webpack.js.org/
