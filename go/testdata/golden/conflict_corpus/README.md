# Shared conflict-procedure corpus

Every case here is run by both runtimes: Go's `kdb/script` (goja) and Kotlin's `kdb-script`
(GraalJS). They exist so that "the same procedure source gives the same answer on either
runtime" is a fact a test asserts, not an assumption - a merge resolved by a procedure on a Go
node and the same conflict examined on a Kotlin node must not disagree.

Each file is one case:

    {
      "name":    short identifier,
      "comment": what it pins down,
      "source":  the procedure, defining main(conflict),
      "input":   { docId, base, local, remote, origins: { local, remote } },
      "expected": one of
                   {"kind":"take",   "side": 0|1}
                   {"kind":"doc",    "doc": <object>}
                   {"kind":"fork",   "side": 0|1}
                   {"kind":"delete"}
                   {"kind":"defer"}
      "errorContains": present instead of "expected" when the call must fail
    }

`base`, `local` and `remote` are document bodies; `null` means the document is absent on that
side. A runner builds the procedure's argument from `input` itself, so the argument builder is
under test too.
