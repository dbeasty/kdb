package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/limidus/kdb/go/kdb/embed"
)

// kdb file: a document's attachments from the command line.
//
//	kdb file ls <ns> <doc>                              list a document's attachments
//	kdb file put <ns> <doc> <path> [--name N] [--mime M] attach a file (replacing one of that name)
//	kdb file get <ns> <doc> <name|id> [-o path]          write an attachment out (stdout by default)
//	kdb file rm <ns> <doc> <name|id>                     remove an attachment
//	kdb file migrate <ns> [--dry-run] [--strip]          turn blobHash file records into attachments

// FileCmd is one kdb file subcommand.
type FileCmd struct {
	Op        string
	Namespace string
	Doc       string
	Arg       string // put: path; get, rm: name or id
	Name      string
	Mime      string
	Out       string
	DryRun    bool
	Strip     bool
}

func parseFileCommand(args []string) (Command, error) {
	usage := fmt.Errorf("usage: kdb file ls|put|get|rm|migrate ... (see kdb help)")
	if len(args) < 2 {
		return nil, usage
	}
	c := FileCmd{Op: args[0], Namespace: args[1]}
	rest := args[2:]
	need := map[string]int{"ls": 1, "put": 2, "get": 2, "rm": 2, "migrate": 0}
	n, ok := need[c.Op]
	if !ok || len(rest) < n {
		return nil, usage
	}
	if n >= 1 {
		c.Doc = rest[0]
	}
	if n == 2 {
		c.Arg = rest[1]
	}
	for i := n; i < len(rest); i++ {
		flag := rest[i]
		value := func() (string, error) {
			i++
			if i >= len(rest) {
				return "", fmt.Errorf("%s requires a value", flag)
			}
			return rest[i], nil
		}
		var err error
		switch {
		case flag == "--name" && c.Op == "put":
			c.Name, err = value()
		case flag == "--mime" && c.Op == "put":
			c.Mime, err = value()
		case (flag == "-o" || flag == "--out") && c.Op == "get":
			c.Out, err = value()
		case flag == "--dry-run" && c.Op == "migrate":
			c.DryRun = true
		case flag == "--strip" && c.Op == "migrate":
			c.Strip = true
		default:
			return nil, fmt.Errorf("unknown option for file %s: %s", c.Op, flag)
		}
		if err != nil {
			return nil, err
		}
	}
	return c, nil
}

func cmdFile(cfg Config, rt *embed.EmbeddedKdbRuntime, c FileCmd) int {
	fail := func(err error) int {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return 1
	}
	head, err := rt.DAG.Head()
	if err != nil {
		return fail(err)
	}
	if c.Op == "migrate" {
		res, err := rt.MigrateFileAttachments(c.Namespace, c.Strip, c.DryRun)
		if err != nil {
			return fail(err)
		}
		verb := "migrated"
		if c.DryRun {
			verb = "would migrate"
		}
		fmt.Printf("scanned %d documents, %s %d, %d already migrated\n", res.Scanned, verb, res.Migrated, res.Skipped)
		for _, id := range res.Missing {
			fmt.Printf("missing blob: %s\n", id)
		}
		if !c.DryRun && res.Migrated > 0 && !cfg.Quiet {
			fmt.Printf("commit %s\n", res.Commit.Hex())
		}
		return 0
	}
	commit, err := rt.DAG.GetCommitOrThrow(head)
	if err != nil {
		return fail(err)
	}
	doc, err := resolveDocSelector(c.Namespace, rt.Storage, commit.DocumentTreeHash, c.Doc)
	if err != nil {
		return fail(err)
	}
	sel := embed.AttachmentByName(c.Arg)
	if list, err := rt.ListAttachments(c.Namespace, doc, head); err == nil {
		for _, a := range list {
			if strings.EqualFold(a.ID.String(), c.Arg) {
				sel = embed.AttachmentByID(a.ID)
			}
		}
	}
	switch c.Op {
	case "ls":
		list, err := rt.ListAttachments(c.Namespace, doc, head)
		if err != nil {
			return fail(err)
		}
		for _, a := range list {
			fmt.Printf("%s  %12d  %-24s  %s  %s\n", a.ID, a.Size, a.Mime, a.ContentHash.Hex()[:12], a.Name)
		}
	case "put":
		f, err := os.Open(c.Arg)
		if err != nil {
			return fail(err)
		}
		defer f.Close()
		name := c.Name
		if name == "" {
			name = baseName(c.Arg)
		}
		hash, info, err := rt.PutAttachment(c.Namespace, doc, f, embed.PutAttachmentOptions{Name: name, Mime: c.Mime})
		if err != nil {
			return fail(err)
		}
		if !cfg.Quiet {
			fmt.Printf("%s %s %d bytes, commit %s\n", info.ID, info.Name, info.Size, hash.Hex())
		}
	case "get":
		r, _, err := rt.GetAttachment(c.Namespace, doc, sel, head)
		if err != nil {
			return fail(err)
		}
		defer r.Close()
		var w io.Writer = os.Stdout
		if c.Out != "" {
			f, err := os.Create(c.Out)
			if err != nil {
				return fail(err)
			}
			defer f.Close()
			w = f
		}
		if _, err := io.Copy(w, r); err != nil {
			return fail(err)
		}
	case "rm":
		hash, err := rt.DeleteAttachment(c.Namespace, doc, sel)
		if err != nil {
			return fail(err)
		}
		if !cfg.Quiet {
			fmt.Printf("commit %s\n", hash.Hex())
		}
	}
	return 0
}

func baseName(path string) string {
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		return path[i+1:]
	}
	return path
}

func (FileCmd) command() {}
