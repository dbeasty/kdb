package driver

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"strings"
	"sync"
)

const (
	// URLPrefix is the DSN prefix accepted by this driver.
	URLPrefix = "kdb://"
)

var (
	driverOnce      sync.Once
	clearMemoryHook = func() {}
)

func init() {
	sql.Register("kdb", &Driver{})
	clearMemoryHook = func() { sharedMemory.clearAll() }
}

// Driver implements database/sql/driver for kdb:// URLs.
type Driver struct{}

func (d *Driver) Open(name string) (driver.Conn, error) {
	parsed, err := ParseDSN(name)
	if err != nil {
		return nil, err
	}
	return openConn(parsed)
}

// Open parses a full kdb:// URL and returns a *sql.DB.
func Open(raw string) (*sql.DB, error) {
	if err := AcceptsURL(raw); err != nil {
		return nil, err
	}
	dsn := strings.TrimPrefix(raw, URLPrefix)
	return sql.Open("kdb", dsn)
}

// AcceptsURL reports whether url is a kdb driver URL.
func AcceptsURL(raw string) error {
	if !strings.HasPrefix(raw, URLPrefix) {
		return fmt.Errorf("not a kdb URL: %s", raw)
	}
	return nil
}

// ClearMemoryRegistries drops shared in-memory runtimes (tests).
func ClearMemoryRegistries() {
	clearMemoryHook()
}

// EnsureRegistered forces driver registration (tests).
func EnsureRegistered() {
	driverOnce.Do(func() {})
}
