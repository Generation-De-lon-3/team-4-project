"use strict";

const test        = require("tape")
    , overrideEnv = require("../../override-env");

test("overrideEnv", t => {
	const { env } = process;

	t.test("Bare", t => {
		env.FOO = "bar";
		try {
			const { restoreEnv, originalEnv } = overrideEnv(null);
			t.notEqual(process.env, env, "Should override env");
			t.equal(process.env.FOO, undefined, "Should not copy existing variables");
			t.equal(originalEnv, env, "Should expose original env at original env property");
			process.env.marko = null;
			t.equal(process.env.marko, "null", "Should automatically stringify set properties");
			Object.defineProperty(process.env, "hola", { value: 34 });
			t.deepEqual(
				Object.getOwnPropertyDescriptor(process.env, "hola"),
				{ configurable: true, enumerable: true, value: "34", writable: true },
				"Should override property definitions"
			);
			restoreEnv();
			t.equal(process.env, env, "Should restore original env after calling a callback");
		} finally {
			delete env.FOO;
		}
		t.end();
	});

	t.test("Callback", t => {
		env.FOO = "bar";
		try {
			overrideEnv(originalEnv => {
				t.notEqual(process.env, env, "Should override env");
				t.equal(process.env.FOO, undefined, "Should not copy existing variables");
				t.equal(originalEnv, env, "Should expose original env as an argument");
			});
			t.equal(process.env, env, "Should restore original env after calling a callback");
		} finally {
			delete env.FOO;
		}
		t.end();
	});

	t.test("Callback returning thenable", t => {
		let promise;
		overrideEnv(() => {
			promise = Promise.resolve();
			return promise;
		});
		t.notEqual(
			process.env, env, "Should wait with restoring original env until thenable resolves"
		);
		promise.then(() => {
			t.equal(process.env, env, "Should restore original env after calling a callback");
			t.end();
		});
	});
	t.end();
});
