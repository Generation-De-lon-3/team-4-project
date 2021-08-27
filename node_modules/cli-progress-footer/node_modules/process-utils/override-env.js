"use strict";

const ensureIterable      = require("type/iterable/ensure")
    , isPlainFunction     = require("type/plain-function/is")
    , ensurePlainFunction = require("type/plain-function/ensure")
    , ensureObject        = require("type/object/ensure")
    , ensureString        = require("type/string/ensure")
    , isObject            = require("type/object/is")
    , processCallback     = require("./lib/private/process-callback")
    , createEnv           = require("./create-env");

const { hasOwnProperty: objHasOwnProperty } = Object.prototype;

module.exports = (options = {}, callback = null) => {
	ensurePlainFunction(callback, { isOptional: true });
	if (!callback && isPlainFunction(options)) {
		callback = options;
		options = {};
	} else if (!isObject(options)) {
		options = {};
	}
	if (
		["asCopy", "whitelist", "variables"].reduce(
			(count, optionName) => (options[optionName] ? count + 1 : count), 0
		) > 1
	) {
		throw new Error("Only one of `asCopy`, `whitelist` or `variables` options can be provided");
	}

	const whitelist = ensureIterable(options.whitelist, {
		isOptional: true,
		ensureItem: ensureString,
		errorMessage: "`whitelist` expected to be a string collection, got %v"
	});
	const original = process.env;
	const replacement = createEnv(ensureObject(options.variables, { isOptional: true }));
	if (options.asCopy) Object.assign(replacement, original);
	if (whitelist) {
		for (const varName of whitelist) {
			if (objHasOwnProperty.call(original, varName)) replacement[varName] = original[varName];
		}
	}
	process.env = replacement;
	const restore = () => (process.env = original);

	if (!callback) return { originalEnv: original, restoreEnv: restore };
	return processCallback(callback, original, restore);
};
