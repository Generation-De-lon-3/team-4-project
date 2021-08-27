"use strict";

const test        = require("tape")
    , overrideCwd = require("../override-cwd");

test("overrideCwd", t => {
	const cwd = process.cwd();

	overrideCwd(__dirname, original => {
		t.notEqual(process.cwd(), cwd, "Should override");
		t.equal(original, cwd, "Should expose original");
		t.deepEqual(process.cwd(), __dirname, "By default should strip argv to one item");
	});
	t.equal(process.cwd(), cwd, "Should restore original after calling a callback");

	t.test("No callback", t => {
		const { restoreCwd, originalCwd } = overrideCwd(__dirname);
		t.notEqual(process.cwd(), cwd, "Should override");
		t.equal(originalCwd, cwd, "Should expose original");
		t.deepEqual(process.cwd(), __dirname, "By default should strip argv to one item");
		restoreCwd();
		t.equal(process.cwd(), cwd, "Should restore original after calling a callback");
		t.end();
	});

	t.end();
});
