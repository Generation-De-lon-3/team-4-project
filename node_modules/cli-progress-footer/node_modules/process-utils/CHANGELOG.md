# Changelog

All notable changes to this project will be documented in this file. See [standard-version](https://github.com/conventional-changelog/standard-version) for commit guidelines.

## [2.6.0](https://github.com/medikoo/process-utils/compare/v2.5.0...v2.6.0) (2019-10-21)

### Features

- **Override Env:** `variables` option ([bbc70f2](https://github.com/medikoo/process-utils/commit/bbc70f2))
- `createEnv` util ([2599d53](https://github.com/medikoo/process-utils/commit/2599d53))
- Support initial properties in `createEnv` util ([3e64ae9](https://github.com/medikoo/process-utils/commit/3e64ae9))

### Tests

- Improve coverage ([3337766](https://github.com/medikoo/process-utils/commit/3337766))
- Improve test names ([75c6cc1](https://github.com/medikoo/process-utils/commit/75c6cc1))
- Split overrideEnv tests ([f12192c](https://github.com/medikoo/process-utils/commit/f12192c))

## [2.5.0](https://github.com/medikoo/process-utils/compare/v2.4.0...v2.5.0) (2019-09-03)

### Features

- overrideCwd util ([1f72bca](https://github.com/medikoo/process-utils/commit/1f72bca))

### Tests

- Improve overrideArgv tests ([eded4dc](https://github.com/medikoo/process-utils/commit/eded4dc))

## [2.4.0](https://github.com/medikoo/process-utils/compare/v2.3.1...v2.4.0) (2019-09-03)

### Features

- overrideArgv util ([f83fc37](https://github.com/medikoo/process-utils/commit/f83fc37))

### [2.3.1](https://github.com/medikoo/process-utils/compare/v2.3.0...v2.3.1) (2019-05-30)

### Bug Fixes

- copy only if property exists ([48c81ae](https://github.com/medikoo/process-utils/commit/48c81ae))

## [2.3.0](https://github.com/medikoo/process-utils/compare/v2.2.0...v2.3.0) (2019-05-28)

### Features

- 'whitelist' option for overrideEnv ([22117af](https://github.com/medikoo/process-utils/commit/22117af))

## [2.2.0](https://github.com/medikoo/process-utils/compare/v2.1.0...v2.2.0) (2019-05-28)

### Features

- support callback in overrideEnv ([19f5950](https://github.com/medikoo/process-utils/commit/19f5950))
- Support thenable based resolution ([6c02752](https://github.com/medikoo/process-utils/commit/6c02752))

# [2.1.0](https://github.com/medikoo/process-utils/compare/v2.0.1...v2.1.0) (2019-04-17)

### Features

- asCopy option for overrideEnv ([d04350c](https://github.com/medikoo/process-utils/commit/d04350c))

<a name="2.0.1"></a>

## [2.0.1](https://github.com/medikoo/process-utils/compare/v2.0.0...v2.0.1) (2018-10-02)

### Bug Fixes

- ensure set and defined properties are stringified ([6b94dc7](https://github.com/medikoo/process-utils/commit/6b94dc7)), closes [#1](https://github.com/medikoo/process-utils/issues/1)

<a name="2.0.0"></a>

# [2.0.0](https://github.com/medikoo/process-utils/compare/v1.1.0...v2.0.0) (2018-09-20)

### Features

- **override-env:** change contract, allow to override over async calls ([a15b894](https://github.com/medikoo/process-utils/commit/a15b894))
- add overrideStdoutWrite and overrideStderrWrite utils ([a487c4c](https://github.com/medikoo/process-utils/commit/a487c4c))

### BREAKING CHANGES

- **override-env:** Handling of overrideEnv change.
  Instead of exposing overriden env to time of invocation of passed
  callback, it now returns `restoreEnv` function which should be called
  after we wish process.env to be restored.
  This allows to override process.env over async calls

<a name="1.1.0"></a>

# [1.1.0](https://github.com/medikoo/process-utils/compare/v1.0.0...v1.1.0) (2018-05-30)

### Features

- for convience pass through callback result ([f6d5beb](https://github.com/medikoo/process-utils/commit/f6d5beb))

<a name="1.0.0"></a>

# 1.0.0 (2018-05-30)
