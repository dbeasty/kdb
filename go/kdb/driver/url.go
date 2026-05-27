package driver

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
)

// Mode is the connection transport mode.
type Mode int

const (
	ModeMemory Mode = iota
	ModeFile
)

// ParsedURL is a parsed kdb:// connection URL.
type ParsedURL struct {
	Mode         Mode
	Catalog      string
	NamespaceID  string
	ReadOnly     bool
	DataRoot     string
	MemoryParams map[string]string
}

// ParseURL parses a full kdb:// URL.
func ParseURL(raw string) (ParsedURL, error) {
	if !strings.HasPrefix(raw, URLPrefix) {
		return ParsedURL{}, fmt.Errorf("not a kdb URL: %s", raw)
	}
	return ParseDSN(strings.TrimPrefix(raw, URLPrefix))
}

// ParseDSN parses the DSN body passed to database/sql (without kdb:// prefix).
func ParseDSN(dsn string) (ParsedURL, error) {
	query := ""
	if i := strings.IndexByte(dsn, '?'); i >= 0 {
		query = dsn[i+1:]
		dsn = dsn[:i]
	}
	q, _ := url.ParseQuery(query)
	readOnly := q.Get("readOnly") == "true" || q.Get("read_only") == "true"

	switch {
	case strings.HasPrefix(dsn, "memory:") || strings.HasPrefix(dsn, "memory/"):
		body := strings.TrimPrefix(dsn, "memory:")
		body = strings.TrimPrefix(body, "memory/")
		return parseMemoryBody(body, readOnly, q)
	case strings.HasPrefix(dsn, "file:") || strings.HasPrefix(dsn, "file/"):
		body := strings.TrimPrefix(dsn, "file:")
		body = strings.TrimPrefix(body, "file/")
		return parseFileBody(body, readOnly)
	default:
		return ParsedURL{}, fmt.Errorf("unsupported kdb DSN %q (want memory/ or file/)", dsn)
	}
}

func parseMemoryBody(body string, readOnly bool, q url.Values) (ParsedURL, error) {
	body = strings.TrimPrefix(body, "//")
	if semi := strings.IndexByte(body, ';'); semi >= 0 {
		for k, v := range parseSemicolonParams(body[semi+1:]) {
			if q.Get(k) == "" {
				q.Set(k, v)
			}
		}
		body = body[:semi]
	}
	body = strings.Trim(body, "/")
	catalog := "default"
	namespace := "default/main"
	if body != "" {
		if i := strings.IndexByte(body, '/'); i >= 0 {
			catalog = body[:i]
			if catalog == "" {
				catalog = "default"
			}
			tail := body[i+1:]
			if strings.Contains(tail, "/") {
				namespace = tail
			} else {
				namespace = catalog + "/" + tail
			}
		} else {
			catalog = body
			namespace = catalog + "/main"
		}
	}
	params := map[string]string{}
	for k := range q {
		params[k] = q.Get(k)
	}
	return ParsedURL{
		Mode:         ModeMemory,
		Catalog:      catalog,
		NamespaceID:  namespace,
		ReadOnly:     readOnly,
		MemoryParams: params,
	}, nil
}

func parseFileBody(body string, readOnly bool) (ParsedURL, error) {
	body = strings.TrimPrefix(body, "//")
	if body == "" {
		return ParsedURL{}, fmt.Errorf("file kdb URL missing data directory path")
	}
	clean := filepath.Clean(body)
	parts := strings.Split(strings.Trim(clean, string(filepath.Separator)), string(filepath.Separator))
	if len(parts) == 0 {
		return ParsedURL{}, fmt.Errorf("file kdb URL missing data directory path")
	}
	var dataRoot, catalog, namespace string
	switch {
	case len(parts) >= 2:
		catalog = parts[len(parts)-2]
		nsLeaf := parts[len(parts)-1]
		namespace = catalog + "/" + nsLeaf
		if len(parts) > 2 {
			dataRoot = filepath.Join(parts[:len(parts)-2]...)
		} else {
			dataRoot = string(filepath.Separator)
		}
	default:
		catalog = parts[len(parts)-1]
		dataRoot = filepath.Dir(clean)
		if dataRoot == "." {
			dataRoot = string(filepath.Separator)
		}
		namespace = catalog + "/main"
	}
	return ParsedURL{
		Mode:        ModeFile,
		Catalog:     catalog,
		NamespaceID: namespace,
		ReadOnly:    readOnly,
		DataRoot:    dataRoot,
	}, nil
}

func parseSemicolonParams(segment string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(segment, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if eq := strings.IndexByte(part, '='); eq < 0 {
			out[part] = "true"
		} else {
			out[strings.TrimSpace(part[:eq])] = strings.TrimSpace(part[eq+1:])
		}
	}
	return out
}
