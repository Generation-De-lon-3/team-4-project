"use strict";

const ensureObject = require("type/object/ensure")
    , entries      = require("ext/object/entries");

module.exports = properties => {
	const env = new Proxy(
		{},
		{
			defineProperty(target, key, inputDescriptor) {
				return Object.defineProperty(target, key, {
					configurable: true,
					enumerable: true,
					value: `${ ensureObject(inputDescriptor).value }`,
					writable: true
				});
			}
		}
	);

	if (ensureObject(properties, { isOptional: true })) {
		for (const [key, value] of entries(properties)) env[key] = value;
	}
	return env;
};
