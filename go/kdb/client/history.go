package client

import (
	"context"
	"fmt"
	"time"

	"github.com/limidus/kdb/go/kdb/wire"
)

// History and undo over the wire (HISTORY_LIST 0x1F, REVERT 0x21).
//
// Sessionless, like GetJSON and Search: neither opens a session or touches
// transaction state. A revision is the same grammar the CLI and the SQL
// AT COMMIT clause accept - "head", "head~10", "head^", a commit hash,
// "<hash>~2", "tag:NAME", "branch:NAME" - resolved server-side, so there
// is one grammar rather than one per client.

// HistoryRequest asks for commits, newest first.
type HistoryRequest struct {
	Namespace string
	// From is the revision to start at. Empty means head.
	From string
	// Skip and Limit page the listing. A zero Limit takes the server's
	// default rather than returning nothing.
	Skip  int
	Limit int
}

// Commit is one commit as a listing describes it.
//
// Operations are deliberately absent - they are the full text of every
// document the commit wrote - so OperationCount is how many there were,
// or -1 when the server could not say without loading them.
type Commit struct {
	Hex            string
	ParentHexes    []string
	TransactionID  string
	Timestamp      time.Time
	AuthorNodeID   string
	TreeHex        string
	Message        string
	OperationCount int
	// Stubbed marks a commit archived in place; its contents are no longer
	// reachable.
	Stubbed bool
}

// HistoryResult is what History returns.
type HistoryResult struct {
	Commits []Commit
	// ResolvedHex is the commit the request's From resolved to, which is
	// also the first entry when there is one.
	ResolvedHex string
}

// History lists a namespace's commits, newest first.
func (c *Client) History(ctx context.Context, req HistoryRequest) (HistoryResult, error) {
	if req.Namespace == "" {
		return HistoryResult{}, fmt.Errorf("kdb: history: Namespace is required")
	}
	msg := wire.HistoryListMessage{
		H:         wire.Header{MessageType: wire.MsgHistoryList, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: req.Namespace,
		From:      req.From,
		Skip:      req.Skip,
		Limit:     req.Limit,
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return HistoryResult{}, err
	}
	result, ok := reply.(wire.HistoryResultMessage)
	if !ok {
		return HistoryResult{}, fmt.Errorf("kdb: expected HistoryResult, got %T", reply)
	}
	if result.Error != nil {
		return HistoryResult{}, classifiedError(*result.Error, result.ErrorCode, result.RetryAfterMs)
	}
	commits := make([]Commit, len(result.Commits))
	for i, e := range result.Commits {
		commits[i] = Commit{
			Hex: e.CommitHex, ParentHexes: e.ParentHexes, TransactionID: e.TransactionID,
			Timestamp:    time.UnixMicro(e.TimestampMicros).UTC(),
			AuthorNodeID: e.AuthorNodeID, TreeHex: e.TreeHex, Message: e.Message,
			OperationCount: e.OperationCount, Stubbed: e.Stubbed,
		}
	}
	return HistoryResult{Commits: commits, ResolvedHex: result.ResolvedHex}, nil
}

// RevertResult reports what a revert put back.
type RevertResult struct {
	// CommitHex is the *new* commit the revert wrote; TargetHex is the one
	// whose state it restored. History moves forward, so they are never
	// the same.
	CommitHex string
	TargetHex string
	Restored  int
	Removed   int
}

// Revert restores the state a namespace had at a revision, by writing a
// new commit whose document tree is that revision's.
//
// This is an undo, not a rewind. Nothing is deleted, the reverted-away
// state stays readable, and the revert is itself revertible - which is
// what makes it safe to offer at all. A commit's operations carry no
// pre-image, so history genuinely cannot be run backwards; going forward
// is the only durable undo there is.
func (c *Client) Revert(ctx context.Context, namespace, toRevision string) (RevertResult, error) {
	if namespace == "" {
		return RevertResult{}, fmt.Errorf("kdb: revert: namespace is required")
	}
	if toRevision == "" {
		return RevertResult{}, fmt.Errorf("kdb: revert: a revision to restore is required")
	}
	msg := wire.RevertMessage{
		H:         wire.Header{MessageType: wire.MsgRevert, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: namespace,
		To:        toRevision,
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return RevertResult{}, err
	}
	result, ok := reply.(wire.RevertResultMessage)
	if !ok {
		return RevertResult{}, fmt.Errorf("kdb: expected RevertResult, got %T", reply)
	}
	if result.Error != nil {
		return RevertResult{}, classifiedError(*result.Error, result.ErrorCode, result.RetryAfterMs)
	}
	return RevertResult{
		CommitHex: result.CommitHex, TargetHex: result.TargetHex,
		Restored: result.Restored, Removed: result.Removed,
	}, nil
}
