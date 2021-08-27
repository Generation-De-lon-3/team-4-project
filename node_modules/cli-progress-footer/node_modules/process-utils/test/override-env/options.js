"use strict";

const test        = require("tape")
    , overrideEnv = require("../../override-env");

test("overrideEnv: options", t => {
	const { env } = process;

	t.test("`whitelist` option", t => {
		env.FOO = "bar";
		env.LOREM = "ispum";
		try {
			const { restoreEnv } = overrideEnv({ whitelist: ["FOO"] });
			t.equal(process.env.FOO, "bar", "Should expose whitelisted variables");
			t.equal(process.env.LOREM, undefined, "Should not expose not whitelisted variables");
			process.env.BAR = "elo";
			t.equal(env.BAR, undefined, "Should not leak new vers on cached env");

			restoreEnv();
		} finally {
			delete env.FOO;
			delete env.LOREM;
		}
		t.end();
	});

	t.test("`asCopy` option", t => {
		env.FOO = "bar";
		try {
			const { restoreEnv } = overrideEnv({ asCopy: true });
			t.equal(process.env.FOO, "bar", "Should copy existing variables");
			process.env.BAR = "elo";
			t.equal(env.BAR, undefined, "Should not leak new vers on cached env");

			restoreEnv();
		} finally {
			delete env.FOO;
		}
		t.end();
	});

	t.test("`variables` option", t => {
		env.FOO = "bar";
		try {
			const { restoreEnv } = overrideEnv({ variables: { MARKO: 12 } });
			t.equal(process.env.FOO, undefined, "Should not expose process.env vars");
			t.equal(process.env.MARKO, "12", "Should copy option.variables onto new process.env");

			restoreEnv();
		} finally {
			delete env.FOO;
		}
		t.end();
	});

	t.end();
});
