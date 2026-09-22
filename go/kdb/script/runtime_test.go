package script

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func call(t *testing.T, r *Runtime, src, args string) (Result, error) {
	t.Helper()
	return r.Call(context.Background(), src, ModePure, nil, args)
}

func TestCallReturnsJSON(t *testing.T) {
	r := New(Limits{})
	got, err := call(t, r, `function main(a) { return { sum: a.x + a.y }; }`, `{"x":2,"y":3}`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != `{"sum":5}` {
		t.Fatalf("got %q", got.Value)
	}
}

func TestCompileErrorIsReported(t *testing.T) {
	r := New(Limits{})
	if _, err := call(t, r, `function main( {`, `{}`); !errors.Is(err, ErrCompile) {
		t.Fatalf("got %v", err)
	}
}

func TestMissingMainIsReported(t *testing.T) {
	r := New(Limits{})
	_, err := call(t, r, `var notMain = 1;`, `{}`)
	if !errors.Is(err, ErrRuntime) || !strings.Contains(err.Error(), "defines no main") {
		t.Fatalf("got %v", err)
	}
}

func TestReturningUndefinedIsReported(t *testing.T) {
	r := New(Limits{})
	_, err := call(t, r, `function main() {}`, `{}`)
	if !errors.Is(err, ErrRuntime) || !strings.Contains(err.Error(), "returned undefined") {
		t.Fatalf("got %v", err)
	}
}

func TestInfiniteLoopTimesOut(t *testing.T) {
	r := New(Limits{WallClock: 100 * time.Millisecond})
	start := time.Now()
	_, err := call(t, r, `function main() { while (true) {} }`, `{}`)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("the interrupt took %v", elapsed)
	}
}

func TestLoopInTopLevelSourceTimesOut(t *testing.T) {
	// The watchdog covers the procedure's own top level too, not just main.
	r := New(Limits{WallClock: 100 * time.Millisecond})
	if _, err := call(t, r, `while (true) {} function main() { return {}; }`, `{}`); !errors.Is(err, ErrTimeout) {
		t.Fatalf("got %v", err)
	}
}

func TestCancelledContextStopsTheCall(t *testing.T) {
	r := New(Limits{WallClock: time.Minute})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, err := r.Call(ctx, `function main() { while (true) {} }`, ModePure, nil, `{}`)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestOutputSizeIsCapped(t *testing.T) {
	r := New(Limits{MaxOutputBytes: 64})
	_, err := call(t, r, `function main() { return { big: new Array(200).join("x") }; }`, `{}`)
	if !errors.Is(err, ErrOutput) {
		t.Fatalf("got %v", err)
	}
}

func TestCallsCannotShareState(t *testing.T) {
	// Each call gets a fresh VM, so a procedure that counts its invocations always sees 1.
	// Otherwise a document's resolution would depend on how many conflicts preceded it.
	r := New(Limits{})
	src := `if (typeof globalThis.n !== "number") { globalThis.n = 0; } function main() { globalThis.n++; return { n: globalThis.n }; }`
	for i := 0; i < 3; i++ {
		got, err := call(t, r, src, `{}`)
		if err != nil {
			t.Fatal(err)
		}
		if got.Value != `{"n":1}` {
			t.Fatalf("call %d saw %q", i, got.Value)
		}
	}
}

func TestRedefiningJSONDoesNotChangeTheBridge(t *testing.T) {
	r := New(Limits{})
	src := `JSON.stringify = function () { return "\"hijacked\""; }; function main(a) { return { x: a.x }; }`
	got, err := call(t, r, src, `{"x":1}`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != `{"x":1}` {
		t.Fatalf("got %q", got.Value)
	}
}

func TestRedefiningTheBridgeDoesNothing(t *testing.T) {
	r := New(Limits{})
	src := `globalThis.__kdb_call = function () { return "\"hijacked\""; }; function main() { return { ok: true }; }`
	got, err := call(t, r, src, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != `{"ok":true}` {
		t.Fatalf("got %q", got.Value)
	}
}

func TestNoHostReachableFromPureMode(t *testing.T) {
	r := New(Limits{})
	for _, expr := range []string{
		`typeof kdb`,
		`typeof require`,
		`typeof process`,
		`typeof setTimeout`,
		`typeof globalThis.Go`,
	} {
		got, err := call(t, r, `function main() { return { t: `+expr+` }; }`, `{}`)
		if err != nil {
			t.Fatal(err)
		}
		if got.Value != `{"t":"undefined"}` {
			t.Fatalf("%s is reachable: %s", expr, got.Value)
		}
	}
}

func TestFunctionConstructorCannotEscape(t *testing.T) {
	// The Function constructor compiles code, but in the same locked-down VM: the stubs the
	// prelude installed are what that code sees too.
	r := New(Limits{})
	_, err := call(t, r, `function main() { return (function(){}).constructor("return Date.now()")(); }`, `{}`)
	if err == nil || !strings.Contains(err.Error(), "Date.now is not available") {
		t.Fatalf("got %v", err)
	}
}

func TestDeniedDeterminismSources(t *testing.T) {
	r := New(Limits{})
	for _, expr := range []string{
		`new Date()`,
		`Date.now()`,
		`Math.random()`,
		`Math.pow(2, 3)`,
		`Math.sin(1)`,
		`"a".localeCompare("b")`,
		`(1234.5).toLocaleString()`,
	} {
		_, err := call(t, r, `function main() { return { v: `+expr+` }; }`, `{}`)
		if err == nil || !strings.Contains(err.Error(), "not available in a deterministic procedure") {
			t.Fatalf("%s was allowed: %v", expr, err)
		}
	}
}

func TestExactArithmeticStaysAvailable(t *testing.T) {
	// Only the implementation-dependent Math functions go; the exactly-specified ones stay,
	// because a procedure that rounds or clamps a number is ordinary and deterministic.
	r := New(Limits{})
	got, err := call(t, r, `function main() { return { v: Math.max(Math.floor(2.7), Math.abs(-1), Math.sqrt(9)) }; }`, `{}`)
	if err != nil {
		t.Fatal(err)
	}
	if got.Value != `{"v":3}` {
		t.Fatalf("got %q", got.Value)
	}
}
