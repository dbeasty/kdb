package sql_test

import "testing"

// U8: a field stored as null and a field absent both read NULL; IS MISSING / EXISTS tell them
// apart, and ALL_VALUES projects every value a path through an array reaches.
func TestMissingExistsAndAllValues(t *testing.T) {
	f := newCorpusFixture(t, "app/conf-missing")
	// delta stores priority null; every other document has a number or nothing.
	f.expect(`SELECT title FROM tasks WHERE priority IS NULL ORDER BY title`, "delta")
	f.expect(`SELECT title FROM tasks WHERE priority IS MISSING ORDER BY title`)
	f.expect(`SELECT title FROM tasks WHERE score IS MISSING ORDER BY title`, "charlie")
	f.expect(`SELECT title FROM tasks WHERE projectIds IS NOT MISSING AND score IS NOT MISSING ORDER BY title`, "Echo", "alpha", "bravo")
	f.expect(`SELECT title, EXISTS(priority), EXISTS(flag) FROM tasks WHERE title = 'delta'`, "delta|true|false")
	f.expect(`SELECT title FROM tasks WHERE EXISTS(collaborators.userId) ORDER BY title`, "alpha", "bravo")
	// A path through an array: the bare column is its first value (spec §3.5), ALL_VALUES is all.
	f.expect(`SELECT collaborators.userId, ALL_VALUES(collaborators.userId) FROM tasks WHERE title = 'alpha'`, `u1|["u1","u2"]`)
	f.expect(`SELECT ALL_VALUES(steps.text) FROM tasks WHERE title = 'alpha'`, `["plan","deploy staging"]`)
	f.expect(`SELECT ALL_VALUES(collaborators.userId) FROM tasks WHERE title = 'delta'`, "NULL")
	f.expectPlanningError(`SELECT EXISTS(1) FROM tasks`, "path")
	f.expectPlanningError(`SELECT ALL_VALUES(title, status) FROM tasks`, "one path")
}
