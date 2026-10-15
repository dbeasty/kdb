package sql

// CandidateEstimator is implemented by engines that can say, before a SELECT runs, how many
// documents its index access path would hand the executor - from index counts, reading nothing.
// Admission sizes a query from it: a query served by an index holds at most its candidates,
// however large the namespace (qtask production plan U2).
type CandidateEstimator interface {
	// EstimateCandidates is the number of documents the query's access path yields, or -1 when
	// that is not known without running it (no servable conjunct: a full scan).
	EstimateCandidates(q SelectQuery, ctx QueryContext) int
}

// EstimateCandidates implements CandidateEstimator.
func (e *defaultEngine) EstimateCandidates(q SelectQuery, ctx QueryContext) int {
	return e.executor.EstimateCandidates(q, ctx)
}

// EstimateCandidates is the executor's side of CandidateEstimator.
func (e *Executor) EstimateCandidates(q SelectQuery, ctx QueryContext) int {
	if q.Where == nil || e.IndexProvider == nil {
		if q.Where != nil {
			// Key lookups need no provider.
			env := newEvalEnv(ctx.Schema, ctx.Parameters, q.From)
			for _, c := range conjuncts(q.Where) {
				if ids, ok := keyLookup(c, env); ok {
					return len(ids)
				}
			}
		}
		return -1
	}
	env := newEvalEnv(ctx.Schema, ctx.Parameters, q.From)
	return e.estimatePredicate(q.Where, env, ctx)
}

// RetainedLimitError reports a query stopped because what it held while running passed the
// limit the server gave it (QueryContext.RetainedLimit) - the alternative being the process
// running out of memory. A client narrows the query, pages it, or selects fewer columns.
type RetainedLimitError struct {
	Limit int64
	Held  int64
}

func (e *RetainedLimitError) Error() string {
	return "query holds more memory than this server allows one query (" + itoa64(e.Held) + " of " + itoa64(e.Limit) +
		" bytes): page it, narrow the WHERE, or select fewer columns"
}

func itoa64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var b [20]byte
	i := len(b)
	for v > 0 {
		i--
		b[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}

// overRetainedLimit reports, as an error, a query holding more than ctx.RetainedLimit.
func (ctx QueryContext) overRetainedLimit() error {
	if ctx.RetainedLimit <= 0 || ctx.Stats == nil || ctx.Stats.RetainedBytes <= ctx.RetainedLimit {
		return nil
	}
	return &RetainedLimitError{Limit: ctx.RetainedLimit, Held: ctx.Stats.RetainedBytes}
}
