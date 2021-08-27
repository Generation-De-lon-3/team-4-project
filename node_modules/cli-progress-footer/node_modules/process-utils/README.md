[![*nix build status][nix-build-image]][nix-build-url]
[![Windows build status][win-build-image]][win-build-url]
[![Tests coverage][cov-image]][cov-url]
![Transpilation status][transpilation-image]
[![npm version][npm-image]][npm-url]

# process-utils

## Utilities for [Node.js `process`](https://nodejs.org/api/process.html) handling

### Installation

```bash
npm install process-utils
```

### Available utilities

#### `create-env(properties = {})`

Creates `env` object. It's a plain object ensured to have only string property values

```javascript
const createEnv = require("process-utils/create-env");

const env = createEnv();
env.FOO = 12;

console.log(env.FOO); // "12"

spawn(program, args, { env }); // Use to invoke other processes in deterministic environment
```

##### Initialization properties

`env` can be pre-initialized with some properties

```javascript
const env = createEnv({ FOO: 12 });
console.log(env.FOO); // "12"
```

#### `override-env`

Overrides `process.env` until returned `restoreEnv()` is called. Helpful when testing modules which behavior relies on environment settings.

```javascript
const overrideEnv = require("process-utils/override-env");

process.env.FOO = "bar";
const { restoreEnv, originalEnv } = overrideEnv();
// Exposes original `process.env`
console.log(originalEnv.FOO); // "bar";
// Current `process.env` points other (empty plain) object
console.log(process.env.FOO); // undefined;

// Provides a callback to restore previous state
restoreEnv();
console.log(process.env.FOO); // "bar"
```

Optionally _callback_ can be passed to `overrideEnv`, it's invoked immediately, and only for a time of it's execution `process.env` is overriden:

```javascript
const overrideEnv = require("process-utils/override-env");

process.env.FOO = "bar";
// Passed callback is invoked immediately
overrideEnv(originalEnv => {
  // Exposes original `process.env`
  console.log(originalEnv.FOO); // "bar";
  // Current `process.env` points other (empty plain) object
  console.log(process.env.FOO); // undefined;
});
// After return the state is restored
console.log(process.env.FOO); // "bar"
```

However if _callback_ returns _thenable_ then `process.env` is restored when given _thenable_ resolves:

```javascript
const overrideEnv = require("process-utils/override-env");

process.env.FOO = "bar";
overrideEnv(() => new Promise(resolve => setTimeout(resolve, 100)));
// Still process.env is not resolved
console.log(process.env.FOO); // undefined
setTimeout(() => {
  // Original process.env is back
  console.log(process.env.FOO); // "bar"
}, 110);
```

##### Supported options

###### whitelist `iterable` (default: `[]`)

Provide a whitelist of env vars to expose on a copy

```javascript
process.env.FOO = "bar";
process.env.LOREM = "ipsum";
const { restoreEnv, originalEnv } = overrideEnv({ whitelist: ["FOO"] });
// Exposes onlywhitelisted props
console.log(process.env.FOO); // "bar"
console.log(process.env.LOREM); // undefined
```

###### asCopy `boolean` (default: `false`)

Override env as copy of original

```javascript
process.env.FOO = "bar";
const { restoreEnv, originalEnv } = overrideEnv({ asCopy: true });
// Exposes process.env props
console.log(process.env.FOO); // "bar"

process.env.BAR = "elo";
// Further updates doesn't affect cached original env
console.log(originalEnv.BAR); // undefined

// Provides a callback to restore previous state
restoreEnv();
console.log(process.env.BAR); // undefined
```

###### veriables `object` (default: `null`)

Variables to be exposed on overriden `process.env`

```javascript
process.env.FOO = "bar";
const { restoreEnv, originalEnv, createEnv } = overrideEnv({ variables: { ELO: 12 } });
// Exposes process.env props
console.log(process.env.FOO); // undefined
console.log(process.env.ELO); // "12"

restoreEnv();
```

#### `override-argv`

Overrides `process.argv` until returned `restoreArgv()` is called. Helpful when testing modules which behavior relies on command line arguments

```javascript
const overrideArgv = require("process-utils/override-argv");

const { restoreArg, originalArgv } = overrideEnv();
// Exposes original `process.argv`
console.log(originalArgv);
// Counterpart by default contains only first item from original argv
console.log(process.argv);

// Provides a callback to restore previous state
restoreArgv();
```

Optionally _callback_ can be passed to `overrideEnv`, it's invoked immediately, and only for a time of it's execution `process.argc` is overriden. if _callback_ returns _thenable_ then `process.argv` is restored when given _thenable_ resolves.

##### Supported options

###### sliceAt `integer` (default: `1`)

Til which index should original `process.argv` be exposed on counterpart

###### args `iterable` (default: `[]`)

Arguments to add to counterpart `process.argv`

#### `override-cwd`

Overrides process current working directory (as resolved via `process.cwd()`) until returned `restoreCwd()` is called. Helpful when testing modules which behavior relies on current working directory resolution

```javascript
const overrideCwd = require("process-utils/override-cwd");

const { restoreCwd, originalCwd } = overrideCwd("/some/new/test/cwd");
// Exposes original `process.argv`
console.log(originalCwd);
//
console.log(process.cwd()); // /some/new/test/cwd

// Provides a callback to restore previous state
restoreCwd();
```

Optionally _callback_ can be passed to `overrideCwd`, it's invoked immediately, and only for a time of it's execution current working directory is overriden. if _callback_ returns _thenable_ then current working directly is restored when given _thenable_ resolves.

#### `override-stdout-write` & `override-stderr-write`

Override `process.stdout.write` or `process.stderr.write` with provided alternative

```javascript
const overrideStdoutWrite = require("process-utils/override-stdout-write");

// Configure decorator that will strip ANSI codes
const {
  originalStdoutWrite, // Original `write` bound to `process.stdout`
  originalWrite, // Original `write` on its own
  restoreStdoutWrite // Allows to restore previous state
} = overrideStdoutWrite((data, superWrite) => superWrite(stripAnsi(data)));

process.stdout.write(someAnsiCodeDecoratedString); // will be output with ANSI codes stripped out

// Restore previous state
restoreStdoutWrite();
```

### Tests

```bash
npm test
```

[nix-build-image]: https://semaphoreci.com/api/v1/medikoo-org/process-utils/branches/master/shields_badge.svg
[nix-build-url]: https://semaphoreci.com/medikoo-org/process-utils
[win-build-image]: https://ci.appveyor.com/api/projects/status/mgttc0h68grk2i6s?svg=true
[win-build-url]: https://ci.appveyor.com/api/projects/status/mgttc0h68grk2i6s
[cov-image]: https://img.shields.io/codecov/c/github/medikoo/process-utils.svg
[cov-url]: https://codecov.io/gh/medikoo/process-utils
[transpilation-image]: https://img.shields.io/badge/transpilation-free-brightgreen.svg
[npm-image]: https://img.shields.io/npm/v/process-utils.svg
[npm-url]: https://www.npmjs.com/package/process-utils
