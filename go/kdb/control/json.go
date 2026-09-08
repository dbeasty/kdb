package control

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"
)

var (
	errMissingCredentials     = errors.New("no Authorization header; the control plane authenticates every request")
	errMalformedAuthorization = errors.New("malformed Authorization header")
	errUnsupportedScheme      = errors.New("unsupported Authorization scheme; use Bearer or Basic")
)

// errorBody is the one error shape every endpoint returns, so a client never has to guess.
type errorBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// The control plane serves data an operator is looking at right now; a cached settings page
	// showing last hour's configuration would be actively misleading.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var body errorBody
	body.Error.Code = code
	body.Error.Message = message
	writeJSON(w, status, body)
}

// intParam reads a bounded integer query parameter. A value that will not parse is an error
// rather than a silent fallback: a caller who asked for limit=abc has a bug, and answering with
// the default hides it.
func intParam(r *http.Request, name string, def, min, max int) (int, error) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, errors.New(name + " must be an integer")
	}
	if n < min {
		return 0, errors.New(name + " must be at least " + strconv.Itoa(min))
	}
	if n > max {
		// Clamping rather than refusing: an operator asking for more rows than the cap allows
		// wants as many as they can have, and the response says how many that was.
		return max, nil
	}
	return n, nil
}

// shortHash is the display form of a commit hash - the same 8 hex digits the CLI's log uses, and
// the minimum LookupHashPrefix accepts.
func shortHash(hex string) string {
	if len(hex) <= 8 {
		return hex
	}
	return hex[:8]
}

func millisToRFC3339(epochMillis int64) string {
	if epochMillis == 0 {
		return ""
	}
	return time.UnixMilli(epochMillis).UTC().Format(time.RFC3339Nano)
}
