"use strict";

const test      = require("tape")
    , isObject  = require("type/object/is")
    , createEnv = require("../create-env");

test("createEnv", t => {
	let env = createEnv();

	t.equal(isObject(env), true, "Should create object");
	t.equal(Object.keys(env).length, 0, "Should create empty object");

	env.marko = null;
	t.equal(env.marko, "null", "Should automatically stringify set properties");
	Object.defineProperty(env, "hola", { value: 34 });
	t.deepEqual(
		Object.getOwnPropertyDescriptor(env, "hola"),
		{ configurable: true, enumerable: true, value: "34", writable: true },
		"Should override property definitions"
	);

	env = createEnv({ FOO: "bar" });

	t.equal(env.FOO, "bar", "Should support propeties input");

	t.end();
});
