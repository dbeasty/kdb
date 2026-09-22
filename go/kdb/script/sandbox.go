package script

import (
	"github.com/dop251/goja"
)

// The deterministic sandbox.
//
// A procedure that resolves a conflict inside a merge decides part of that merge's content, and
// the merge's hash covers its content. Two nodes joining the same two heads must therefore get
// the same answer from it, or they build different merge commits and never converge. goja gives
// no access to the host by itself - there is no require, no process, no filesystem, no timers -
// but a plain JavaScript runtime still has three ways to differ between two runs: the clock,
// randomness, and anything locale- or platform-dependent.
//
// So ModePure removes them, rather than trusting procedure authors not to reach for them. A
// procedure that calls one gets an exception, which holds the merge; it does not get a fabricated
// value that would silently diverge.
//
// The transcendental Math functions go too. ECMAScript explicitly allows an implementation to
// return any value within an implementation-dependent approximation for sin, pow, exp and the
// rest, and Go's math and GraalVM's do differ in the last bit. Math.sqrt is exact under IEEE 754,
// so it stays, along with abs, floor, ceil, round, trunc, sign, min and max.
const purePrelude = `
(function () {
  function deny(name) {
    return function () {
      throw new Error("kdb: " + name + " is not available in a deterministic procedure");
    };
  }
  var approx = ["random", "sin", "cos", "tan", "asin", "acos", "atan", "atan2",
                "exp", "expm1", "log", "log1p", "log2", "log10", "pow", "cbrt",
                "hypot", "sinh", "cosh", "tanh", "asinh", "acosh", "atanh"];
  for (var i = 0; i < approx.length; i++) {
    Math[approx[i]] = deny("Math." + approx[i]);
  }
  var noDate = deny("Date");
  noDate.now = deny("Date.now");
  noDate.parse = deny("Date.parse");
  noDate.UTC = deny("Date.UTC");
  globalThis.Date = noDate;
  globalThis.Intl = undefined;
  String.prototype.localeCompare = deny("localeCompare");
  String.prototype.toLocaleUpperCase = deny("toLocaleUpperCase");
  String.prototype.toLocaleLowerCase = deny("toLocaleLowerCase");
  Number.prototype.toLocaleString = deny("toLocaleString");
  Object.prototype.toLocaleString = deny("toLocaleString");
})();
`

// invokePrelude installs the bridge the host calls. It captures JSON.parse and JSON.stringify
// before the procedure's own source runs, so a procedure that replaces JSON - deliberately or by
// accident - cannot change how its arguments are read or its result is written.
//
// It resolves main at call time, because the procedure's source has not run yet.
const invokePrelude = `
globalThis.__kdb_call = (function () {
  var parse = JSON.parse, stringify = JSON.stringify;
  return function (text) {
    if (typeof main !== "function") {
      throw new Error("kdb: procedure defines no main(args) function");
    }
    var out = main(parse(text));
    if (out === undefined) {
      throw new Error("kdb: main(args) returned undefined");
    }
    return stringify(out);
  };
})();
`

var (
	purePreludeProgram   = goja.MustCompile("kdb:pure-prelude", purePrelude, true)
	invokePreludeProgram = goja.MustCompile("kdb:invoke-prelude", invokePrelude, true)
)
