package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/limidus/kdb/go/kdb/auth"
	"github.com/limidus/kdb/go/kdb/embed"
)

// runUserCommand implements the RBAC bootstrap CLI (kdb-finish-up-plan Phase 2.7):
//
//	kdb-service user create --data-dir DIR --user NAME --password PW [--roles r1,r2]
//	kdb-service user role   --data-dir DIR --role NAME --grants "sql:app/*,sync:app/*"
//	kdb-service user assign --data-dir DIR --user NAME --role NAME
//	kdb-service user list   --data-dir DIR
//
// All subcommands open the durable registry exclusively (taking the data-dir lock), so they
// refuse to run while the service is up - stop the service first. Grants are "kind:pattern"
// strings, e.g. "sql:app/*" (see kdb/auth's PermissionMatchesPath for the pattern rules).
func runUserCommand(args []string) int {
	if len(args) == 0 {
		printUserUsage()
		return 2
	}
	sub, rest := args[0], args[1:]

	fs := flag.NewFlagSet("kdb-service user "+sub, flag.ExitOnError)
	var dataDir, user, password, roles, role, grants string
	fs.StringVar(&dataDir, "data-dir", "", "filesystem data root (same value the service runs with)")
	fs.StringVar(&user, "user", "", "user id")
	fs.StringVar(&password, "password", "", "user password (hashed with PBKDF2 before storage)")
	fs.StringVar(&roles, "roles", "", "comma-separated role names for the new user")
	fs.StringVar(&role, "role", "", "role name")
	fs.StringVar(&grants, "grants", "", "comma-separated kind:pattern grants, e.g. sql:app/*,sync:app/*")
	_ = fs.Parse(rest)

	if dataDir == "" {
		fmt.Fprintln(os.Stderr, "Error: --data-dir is required (the durable registry lives under it)")
		return 2
	}

	reg, err := embed.OpenFileAuthRegistryExclusive(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: open registry (is the service still running? stop it first): %v\n", err)
		return 1
	}
	defer reg.Close()

	switch sub {
	case "create":
		if user == "" || password == "" {
			fmt.Fprintln(os.Stderr, "Error: user create requires --user and --password")
			return 2
		}
		var roleList []string
		if roles != "" {
			roleList = splitTrimmed(roles)
		}
		if err := reg.Store.CreateUser(user, password, roleList); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		fmt.Printf("created user %q roles=%v\n", user, roleList)
	case "role":
		if role == "" || grants == "" {
			fmt.Fprintln(os.Stderr, "Error: user role requires --role and --grants")
			return 2
		}
		grantList := splitTrimmed(grants)
		for _, g := range grantList {
			if auth.PermissionKind(g) == "" {
				fmt.Fprintf(os.Stderr, "Error: malformed grant %q (want kind:pattern, e.g. sql:app/*)\n", g)
				return 2
			}
		}
		if existing, err := reg.Store.GetRole(role); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		} else if existing != nil {
			if err := reg.Store.UpdateGrants(role, grantList); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return 1
			}
			fmt.Printf("updated role %q grants=%v\n", role, grantList)
		} else {
			if err := reg.Store.CreateRole(role, grantList); err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return 1
			}
			fmt.Printf("created role %q grants=%v\n", role, grantList)
		}
	case "assign":
		if user == "" || role == "" {
			fmt.Fprintln(os.Stderr, "Error: user assign requires --user and --role")
			return 2
		}
		if err := reg.Store.AssignRole(user, role); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		fmt.Printf("assigned role %q to user %q\n", role, user)
	case "list":
		grantsByRole, err := reg.Store.GrantsByRole()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return 1
		}
		if len(grantsByRole) == 0 {
			fmt.Println("no roles defined")
		}
		for name, gs := range grantsByRole {
			fmt.Printf("role %q grants=%v\n", name, gs)
		}
	default:
		printUserUsage()
		return 2
	}
	return 0
}

func splitTrimmed(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func printUserUsage() {
	fmt.Fprintln(os.Stderr, `kdb-service user - bootstrap the durable RBAC registry (service must be stopped)

Subcommands:
  create --data-dir DIR --user NAME --password PW [--roles r1,r2]
  role   --data-dir DIR --role NAME --grants "sql:app/*,sync:app/*"   (create or update)
  assign --data-dir DIR --user NAME --role NAME
  list   --data-dir DIR`)
}
