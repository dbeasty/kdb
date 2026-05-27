package json

import (
	"strconv"
	"sync"

	kdberr "github.com/limidus/kdb/go/kdb/error"
)

type pathSeg interface {
	isPathSeg()
}

type rootSeg struct{}

func (rootSeg) isPathSeg() {}

type fieldSeg struct{ name string }

func (fieldSeg) isPathSeg() {}

type idxSeg struct{ index int }

func (idxSeg) isPathSeg() {}

type wildcardElemSeg struct{}

func (wildcardElemSeg) isPathSeg() {}

type wildcardFieldSeg struct{}

func (wildcardFieldSeg) isPathSeg() {}

// Path is a compiled JSONPath ($.field, $.a[0], wildcards for GetAll only).
type Path struct {
	Expression string
	segments   []pathSeg
}

var (
	pathCacheMu sync.Mutex
	pathCache   = make(map[string]*Path)
)

// CompilePath parses and caches a JSONPath expression.
func CompilePath(expression string) (*Path, error) {
	pathCacheMu.Lock()
	defer pathCacheMu.Unlock()
	if p, ok := pathCache[expression]; ok {
		return p, nil
	}
	segs, err := parsePath(expression)
	if err != nil {
		return nil, err
	}
	p := &Path{Expression: expression, segments: segs}
	if len(pathCache) >= 256 {
		for k := range pathCache {
			delete(pathCache, k)
			break
		}
	}
	pathCache[expression] = p
	return p, nil
}

// CompilePathOrNil returns nil on parse failure.
func CompilePathOrNil(expression string) *Path {
	p, _ := CompilePath(expression)
	return p
}

func (p *Path) HasWildcards() bool {
	for _, s := range p.segments {
		switch s.(type) {
		case wildcardElemSeg, wildcardFieldSeg:
			return true
		}
	}
	return false
}

func parsePath(expr string) ([]pathSeg, error) {
	if expr == "" || expr[0] != '$' {
		return nil, kdberr.NewJsonPathError("path must start with $", expr, nil)
	}
	if len(expr) == 1 {
		return []pathSeg{rootSeg{}}, nil
	}
	if expr[1] != '.' {
		return nil, kdberr.NewJsonPathError("expected . after $", expr, nil)
	}
	out := []pathSeg{rootSeg{}}
	i := 2

	parseNameOrStar := func() error {
		if i < len(expr) && expr[i] == '*' {
			i++
			out = append(out, wildcardFieldSeg{})
			return nil
		}
		startI := i
		for i < len(expr) && expr[i] != '.' && expr[i] != '[' {
			i++
		}
		if i == startI {
			return kdberr.NewJsonPathError("field name expected", expr, nil)
		}
		out = append(out, fieldSeg{name: expr[startI:i]})
		return nil
	}

	parseBracket := func() error {
		i++
		if i < len(expr) && expr[i] == '*' {
			i++
			if i >= len(expr) || expr[i] != ']' {
				return kdberr.NewJsonPathError("expected ] after *", expr, nil)
			}
			i++
			out = append(out, wildcardElemSeg{})
			return nil
		}
		neg := false
		if i < len(expr) && expr[i] == '-' {
			neg = true
			i++
		}
		ds := i
		for i < len(expr) && expr[i] >= '0' && expr[i] <= '9' {
			i++
		}
		if i == ds {
			return kdberr.NewJsonPathError("index expected", expr, nil)
		}
		idx, _ := strconv.Atoi(expr[ds:i])
		if neg {
			idx = -idx
		}
		if i >= len(expr) || expr[i] != ']' {
			return kdberr.NewJsonPathError("expected ]", expr, nil)
		}
		i++
		out = append(out, idxSeg{index: idx})
		return nil
	}

	if err := parseNameOrStar(); err != nil {
		return nil, err
	}
	for i < len(expr) {
		switch expr[i] {
		case '.':
			i++
			if err := parseNameOrStar(); err != nil {
				return nil, err
			}
		case '[':
			if err := parseBracket(); err != nil {
				return nil, err
			}
		default:
			return nil, kdberr.NewJsonPathError("unexpected '"+string(expr[i])+"'", expr, nil)
		}
	}
	return out, nil
}
