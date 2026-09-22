package peersync

import (
	"sort"
	"strings"
)

// MatchNamespace reports whether ns matches pattern. Namespaces are '/'-separated paths;
// in a pattern "*" matches exactly one segment and "**" matches any number of segments,
// including none. Anything else must match its segment literally.
func MatchNamespace(pattern, ns string) bool {
	return matchSegments(strings.Split(pattern, "/"), strings.Split(ns, "/"))
}

func matchSegments(pat, segs []string) bool {
	for len(pat) > 0 {
		switch pat[0] {
		case "**":
			for i := 0; i <= len(segs); i++ {
				if matchSegments(pat[1:], segs[i:]) {
					return true
				}
			}
			return false
		case "*":
			if len(segs) == 0 {
				return false
			}
		default:
			if len(segs) == 0 || segs[0] != pat[0] {
				return false
			}
		}
		pat, segs = pat[1:], segs[1:]
	}
	return len(segs) == 0
}

// SelectNamespaces returns the namespaces in available that match at least one of patterns
// and none of the exclusions (patterns prefixed "!"), sorted. A pattern with no wildcard names
// one namespace, which is selected only if it is available.
func SelectNamespaces(patterns, available []string) []string {
	var include, exclude []string
	for _, p := range patterns {
		if strings.HasPrefix(p, "!") {
			exclude = append(exclude, p[1:])
		} else {
			include = append(include, p)
		}
	}
	var out []string
	for _, ns := range available {
		matched := false
		for _, p := range include {
			if MatchNamespace(p, ns) {
				matched = true
				break
			}
		}
		for _, p := range exclude {
			if matched && MatchNamespace(p, ns) {
				matched = false
			}
		}
		if matched {
			out = append(out, ns)
		}
	}
	sort.Strings(out)
	return out
}
