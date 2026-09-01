package client

import (
	"context"
	"errors"
	"fmt"

	"github.com/limidus/kdb/go/kdb/codec"
	"github.com/limidus/kdb/go/kdb/document"
)

// ErrPreconditionFailed is returned when a conditional write's assertion did not hold. It is a
// distinct sentinel from ErrConflict on purpose: a plain conflict means "re-read and retry",
// while a failed precondition may mean the caller's whole premise was wrong (the document it
// meant to create already exists). Retrying one blindly is correct; retrying the other is not.
var ErrPreconditionFailed = errors.New("kdb: precondition failed")

// PreconditionError carries which documents failed and what was actually there. ActualHash is
// the content hash the server found - the value a compare-and-set caller needs in order to
// re-derive from the state that beat it.
type PreconditionError struct {
	DocumentID string
	ActualHash string
}

func (e *PreconditionError) Error() string {
	if e.ActualHash == "" {
		return fmt.Sprintf("kdb: precondition failed on document %s (document absent)", e.DocumentID)
	}
	return fmt.Sprintf("kdb: precondition failed on document %s (actual content hash %s)", e.DocumentID, e.ActualHash)
}

func (e *PreconditionError) Unwrap() error { return ErrPreconditionFailed }

// PutIfAbsent creates docID with jsonBody only if no document exists there, returning the
// resulting commit hash. Returns a *PreconditionError (wrapping ErrPreconditionFailed) if the
// document already exists.
//
// This is the primitive for "create exactly once" that Upsert cannot express and that a plain
// PutJSON only approximates: PutJSON's optimistic check is content-addressed and per-document,
// so two clients creating the same id from the same empty base can both believe they won.
func (c *Client) PutIfAbsent(ctx context.Context, ns string, docID string, jsonBody []byte) (string, error) {
	return c.conditionalPut(ctx, ns, docID, jsonBody, document.Precondition{
		OpIndex: 0, Kind: document.ExpectAbsent,
	})
}

// ReplaceIf writes jsonBody at docID only if the document's current content hash is exactly
// expectedContentHash - a compare-and-set. Returns a *PreconditionError if it is not.
//
// The hash to pass is the one GetJSONWithHash returned for the value this write was derived
// from. Note that this check is literal: it fails on a hash mismatch even when jsonBody happens
// to equal what is stored, unlike ordinary conflict detection, which treats a content-identical
// write as a no-op.
func (c *Client) ReplaceIf(ctx context.Context, ns string, docID string, jsonBody []byte, expectedContentHash string) (string, error) {
	h, err := codec.HashFromHex(expectedContentHash)
	if err != nil {
		return "", fmt.Errorf("kdb: invalid expected content hash: %w", err)
	}
	return c.conditionalPut(ctx, ns, docID, jsonBody, document.Precondition{
		OpIndex: 0, Kind: document.ExpectContentHash, ContentHash: h,
	})
}

// ReplaceIfPresent writes jsonBody at docID only if some document exists there - an update that
// refuses to become a create.
func (c *Client) ReplaceIfPresent(ctx context.Context, ns string, docID string, jsonBody []byte) (string, error) {
	return c.conditionalPut(ctx, ns, docID, jsonBody, document.Precondition{
		OpIndex: 0, Kind: document.ExpectPresent,
	})
}

func (c *Client) conditionalPut(
	ctx context.Context, ns string, docID string, jsonBody []byte, pre document.Precondition,
) (string, error) {
	id, err := codec.UUIDFromString(docID)
	if err != nil {
		return "", fmt.Errorf("kdb: invalid docID: %w", err)
	}
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return "", err
	}
	txID, err := codec.RandomUUID()
	if err != nil {
		return "", err
	}
	st.mu.Lock()
	base := st.head
	st.mu.Unlock()
	tx := document.Transaction{
		ID:            txID,
		BaseVersion:   base,
		Operations:    []document.Op{document.WriteOp{DocID: id, Patch: string(jsonBody)}},
		Timestamp:     codec.TimestampNow(),
		AuthorNodeID:  c.authorNodeID,
		Preconditions: []document.Precondition{pre},
	}
	return c.commitTransaction(ctx, ns, st, tx)
}

// CompareAndSwap reads docID, applies update to its current JSON, and writes the result back
// under a compare-and-set, retrying up to maxAttempts times when another writer wins the race.
// It returns the commit hash of the successful write.
//
// update receives nil when the document does not exist, so a caller can seed it; returning a nil
// body from update means "leave it alone", and CompareAndSwap returns ErrAborted without
// writing. Every retry re-reads: a caller that recomputes from a stale value is exactly the lost
// update this exists to prevent, so the read cannot be hoisted out of the loop.
func (c *Client) CompareAndSwap(
	ctx context.Context,
	ns string,
	docID string,
	maxAttempts int,
	update func(current []byte) ([]byte, error),
) (string, error) {
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		current, hash, err := c.GetJSONWithHash(ctx, ns, docID)
		switch {
		case err == nil:
		case errors.Is(err, ErrNotFound):
			current, hash = nil, ""
		default:
			return "", err
		}

		next, err := update(current)
		if err != nil {
			return "", err
		}
		if next == nil {
			return "", ErrAborted
		}

		var commitHex string
		if hash == "" {
			commitHex, err = c.PutIfAbsent(ctx, ns, docID, next)
		} else {
			commitHex, err = c.ReplaceIf(ctx, ns, docID, next, hash)
		}
		if err == nil {
			return commitHex, nil
		}
		// Only a lost race is worth another attempt. Anything else - a schema violation, a
		// unique-constraint collision, a transport failure - will fail identically next time,
		// and retrying it just burns the caller's attempt budget and the server's write gate.
		if !errors.Is(err, ErrPreconditionFailed) && !errors.Is(err, ErrConflict) {
			return "", err
		}
		lastErr = err
	}
	return "", fmt.Errorf("kdb: compare-and-swap on %s gave up after %d attempts: %w", docID, maxAttempts, lastErr)
}

// ErrAborted reports that a CompareAndSwap update function chose not to write.
var ErrAborted = errors.New("kdb: operation aborted by caller")
