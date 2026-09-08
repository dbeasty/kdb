package cli

// Command is a parsed CLI command.
type Command interface {
	command()
}

type InitCmd struct{ Namespace string }
type PutCmd struct {
	Namespace string
	Payload   string
}
type GetCmd struct {
	Namespace string
	DocID     string
	// At reads the document as of a revision instead of head. Empty means
	// head. See dag.ParseRevision for what it accepts.
	At string
}
type QueryCmd struct {
	Namespace string
	SQL       string
}

// LogCmd lists commits newest first. Limit bounds how many; Oneline drops
// everything but the short hash and the message.
type LogCmd struct {
	Namespace string
	Limit     int
	Skip      int
	Oneline   bool
}

// ShowCmd describes one commit, named by any revision specification.
type ShowCmd struct {
	Namespace string
	Revision  string
}

// DiffCmd reports which documents differ between two revisions.
type DiffCmd struct {
	Namespace string
	From      string
	To        string
}

// RevertCmd restores the state at a revision by writing a new commit.
type RevertCmd struct {
	Namespace string
	Revision  string
}

// TagListCmd, TagCreateCmd and TagDeleteCmd manage the names that reach a
// commit without its hash.
type TagListCmd struct{ Namespace string }
type TagCreateCmd struct {
	Namespace string
	Name      string
	Revision  string
	Message   string
}
type TagDeleteCmd struct {
	Namespace string
	Name      string
}
type StatusCmd struct{ Namespace string }
type UnlockCmd struct{}
type BranchListCmd struct{ Namespace string }
type BranchCreateCmd struct {
	Namespace string
	Name      string
	FromHash  string
}
type BranchCheckoutCmd struct {
	Namespace string
	Name      string
}

func (InitCmd) command()           {}
func (PutCmd) command()            {}
func (GetCmd) command()            {}
func (QueryCmd) command()          {}
func (LogCmd) command()            {}
func (ShowCmd) command()           {}
func (DiffCmd) command()           {}
func (RevertCmd) command()         {}
func (TagListCmd) command()        {}
func (TagCreateCmd) command()      {}
func (TagDeleteCmd) command()      {}
func (StatusCmd) command()         {}
func (UnlockCmd) command()         {}
func (BranchListCmd) command()     {}
func (BranchCreateCmd) command()   {}
func (BranchCheckoutCmd) command() {}
