package embed

import (
	"fmt"

	"github.com/limidus/kdb/go/kdb/storage"
	"github.com/limidus/kdb/go/kdb/storage/engine"
)

// HistoryFeatureUnavailableError reports an operation that only means
// something on a namespace that keeps its history. Detect with errors.As.
//
// The alternative - letting these succeed and quietly not work - is worse
// than it sounds. A branch or a tag is a *retention root*: it says "this
// commit must stay reachable". Under storage.HistoryModeNone nothing
// consults them before reclaiming, because the retention window is the
// only thing that decides, so a branch created here would silently become
// a name for a commit whose documents can no longer be produced. Refusing
// is the honest answer, and it names the mode so the reason is not a
// mystery.
type HistoryFeatureUnavailableError struct {
	NamespaceID string
	Feature     string
}

func (e *HistoryFeatureUnavailableError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q runs with history=none, which reclaims commits on a retention window, "+
			"so %s is not available: it would name a commit that may stop being readable. "+
			"Convert the namespace with kdb-inspect migrate-history --history-mode full to use it",
		e.NamespaceID, e.Feature)
}

// HistoryMode reports how long this namespace keeps the past. Full for a
// runtime with no engine behind it, which is the honest answer: nothing
// there reclaims anything.
func (rt *EmbeddedKdbRuntime) HistoryMode() storage.HistoryMode {
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return storage.HistoryModeFull
	}
	return eng.HistoryMode()
}

// RetentionWindow reports how much of the past this namespace keeps.
// Meaningless under HistoryModeFull, which keeps all of it.
func (rt *EmbeddedKdbRuntime) RetentionWindow() storage.RetentionWindow {
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return storage.RetentionWindow{}
	}
	return eng.RetentionWindow()
}

// SetRetentionWindow changes how much of the past this namespace keeps, on
// a running runtime, and reports whether the change reached an engine that
// can honour it.
//
// The change is in force from the next maintenance pass onwards. It is not
// persisted anywhere: a restart returns the namespace to whatever its
// configuration says, and the control plane reports that as drift rather
// than hiding it.
//
// Refused on a read-only runtime, which reclaims nothing, and under
// HistoryModeFull, where the window governs nothing and accepting it would
// leave an operator believing they had configured something that will never
// be consulted.
func (rt *EmbeddedKdbRuntime) SetRetentionWindow(w storage.RetentionWindow) error {
	if err := rt.AssertWritable(); err != nil {
		return err
	}
	eng, ok := rt.Storage.(*engine.ServerEngine)
	if !ok {
		return fmt.Errorf("kdb: this runtime has no engine that keeps a retention window")
	}
	if eng.HistoryMode() != storage.HistoryModeNone {
		return fmt.Errorf(
			"kdb: namespace %s is history=full, which keeps everything; a retention window "+
				"would govern nothing until it is switched to history=none", rt.DefaultNamespace)
	}
	eng.SetRetentionWindow(w)
	return nil
}

// AssertRetainsHistory refuses an operation that depends on history being
// kept. feature is named in the error, so it reads as a sentence.
func (rt *EmbeddedKdbRuntime) AssertRetainsHistory(namespaceID, feature string) error {
	if rt.HistoryMode() != storage.HistoryModeNone {
		return nil
	}
	return &HistoryFeatureUnavailableError{NamespaceID: namespaceID, Feature: feature}
}

// HistoryNotRetainedError reports a revision that this namespace once had
// and has since reclaimed.
//
// Distinct from "no such commit" because the remedy is different and only
// one of them is a configuration change: this one is fixed by a longer
// retention window, and by nothing else. Wraps the underlying resolution
// failure, so errors.As still finds that too.
type HistoryNotRetainedError struct {
	NamespaceID string
	Spec        string
	Window      storage.RetentionWindow
	Err         error
}

func (e *HistoryNotRetainedError) Error() string {
	return fmt.Sprintf(
		"kdb: namespace %q does not retain %q: it runs with history=none and a retention window of %s, "+
			"so anything older has been reclaimed (%v)",
		e.NamespaceID, e.Spec, e.Window, e.Err)
}

func (e *HistoryNotRetainedError) Unwrap() error { return e.Err }

// explainHistoryFailure attributes a failed history operation to the
// retention window when that is what caused it.
//
// Under history=full it changes nothing: the original error is already the
// whole truth, and blaming retention there would be a lie about a
// namespace that reclaims nothing.
func explainHistoryFailure(rt *EmbeddedKdbRuntime, namespaceID, spec string, err error) error {
	if err == nil || rt.HistoryMode() != storage.HistoryModeNone {
		return err
	}
	return &HistoryNotRetainedError{
		NamespaceID: namespaceID, Spec: spec, Window: rt.RetentionWindow(), Err: err,
	}
}
