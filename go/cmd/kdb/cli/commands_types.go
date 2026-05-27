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
}
type QueryCmd struct {
	Namespace string
	SQL       string
}
type LogCmd struct{ Namespace string }
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
func (StatusCmd) command()         {}
func (UnlockCmd) command()         {}
func (BranchListCmd) command()     {}
func (BranchCreateCmd) command()   {}
func (BranchCheckoutCmd) command() {}
