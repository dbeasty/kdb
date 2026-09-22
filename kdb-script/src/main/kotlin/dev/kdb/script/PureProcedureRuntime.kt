package dev.kdb.script

import org.graalvm.polyglot.Context
import org.graalvm.polyglot.HostAccess
import org.graalvm.polyglot.PolyglotAccess
import org.graalvm.polyglot.PolyglotException
import org.graalvm.polyglot.ResourceLimits
import org.graalvm.polyglot.Value

/**
 * Runs a procedure with nothing but its arguments: no clock, no randomness, no locale, no
 * database. The counterpart of Go's `script.ModePure`, and the mode a conflict-resolving
 * procedure runs in.
 *
 * [GraalProcedureRuntime] exists to give a procedure the database; this exists to take everything
 * away. A procedure that decides part of a merge has its answer covered by the merge commit's
 * hash, so two nodes running it must get the same result - and a plain JavaScript runtime still
 * has three ways to differ between two runs: the clock, randomness, and anything locale- or
 * platform-dependent. Rather than trust procedure authors not to reach for them, they are
 * removed: reaching for one throws, which holds the merge, instead of producing a value that
 * would silently diverge.
 *
 * The transcendental Math functions go too. ECMAScript lets an implementation return any value
 * within an implementation-dependent approximation for sin, pow, exp and the rest, and GraalVM's
 * differ from Go's in the last bit. Math.sqrt is exact under IEEE 754, so it stays, along with
 * abs, floor, ceil, round, trunc, sign, min and max.
 *
 * This class needs no registry, storage or DAG, so an embedded caller can use it on its own.
 */
public class PureProcedureRuntime(
    private val limits: ProcLimits = ProcLimits.DEFAULT,
) {
    /** Compiles [source] and throws [ProcException.CompileError] if it will not run. */
    public fun compile(source: String) {
        buildContext().use { context ->
            try {
                context.eval("js", source)
            } catch (e: PolyglotException) {
                if (e.isSyntaxError) throw ProcException.CompileError(e.message ?: "syntax error", e)
                // A source file that throws while it loads is a runtime failure, not a syntax
                // one, but either way it will never produce a usable main.
                throw ProcException.CompileError(e.message ?: e.toString(), e)
            }
        }
    }

    /**
     * Runs `main(args)` with [argsJson] parsed as its argument and returns the JSON of what it
     * returned.
     */
    public fun call(
        source: String,
        argsJson: String,
    ): String {
        val context = buildContext()
        // Cancellation is not cooperative in a CPU-bound guest loop, so the wall clock is
        // enforced the only way it can be: another thread closes the context, which stops guest
        // execution at its next safepoint. Same mechanism as GraalProcedureRuntime's watchdog,
        // without the coroutine scope, because nothing here suspends.
        val watchdog =
            Thread {
                try {
                    Thread.sleep(limits.wallClockMillis)
                    runCatching { context.close(true) }
                } catch (_: InterruptedException) {
                    // Finished in time.
                }
            }
        watchdog.isDaemon = true
        watchdog.start()
        try {
            context.eval("js", PURE_PRELUDE)
            // Captured before the procedure's own source runs, so redefining __kdb_call does
            // nothing - the same order Go's runtime uses, and the reason both refuse to let a
            // procedure decide how its own result is read.
            val bridge: Value = context.getBindings("js").getMember("__kdb_call")
            context.eval("js", source)
            return bridge.execute(argsJson).asString()
        } catch (e: PolyglotException) {
            if (e.isCancelled || e.isInterrupted) throw ProcException.Timeout(limits.wallClockMillis)
            if (e.isResourceExhausted) throw ProcException.ResourceLimitExceeded(e.message ?: "resource limit exceeded")
            if (e.isSyntaxError) throw ProcException.CompileError(e.message ?: "syntax error", e)
            throw ProcException.ScriptRuntimeError(e.message ?: e.toString(), e)
        } finally {
            watchdog.interrupt()
            runCatching { context.close(true) }
        }
    }

    private fun buildContext(): Context =
        Context
            .newBuilder("js")
            .allowHostAccess(HostAccess.NONE)
            .allowHostClassLookup { false }
            .allowIO(false)
            .allowCreateProcess(false)
            .allowCreateThread(false)
            .allowNativeAccess(false)
            .allowPolyglotAccess(PolyglotAccess.NONE)
            .option("engine.WarnInterpreterOnly", "false")
            .resourceLimits(
                ResourceLimits
                    .newBuilder()
                    .statementLimit(limits.maxStatements, null)
                    .build(),
            ).build()

    private companion object {
        /**
         * Removes every source of non-determinism, then installs the bridge the host calls. The
         * bridge captures JSON.parse and JSON.stringify before the procedure's own source runs,
         * so a procedure that replaces JSON cannot change how its arguments are read or its
         * result written. Word for word the same messages as Go's, because the shared corpus
         * asserts on them.
         */
        const val PURE_PRELUDE = """
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
        """
    }
}
