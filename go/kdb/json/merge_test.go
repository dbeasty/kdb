package json

import "testing"

// TestMergeOrdersNewKeysAsThePatchDoes: keys the base lacks follow its own in the patch's order -
// the order Kotlin's merge gives them - every time. They followed Go's map iteration order, so
// a patch adding several keys could store different text (and a different content hash) on
// every run.
func TestMergeOrdersNewKeysAsThePatchDoes(t *testing.T) {
	for i := 0; i < 50; i++ {
		got, err := Merge(`{"b":1,"a":2}`, `{"z":3,"a":4,"m":5,"c":6,"y":7}`)
		if err != nil {
			t.Fatal(err)
		}
		if want := `{"b":1,"a":4,"z":3,"m":5,"c":6,"y":7}`; got != want {
			t.Fatalf("Merge = %s, want %s", got, want)
		}
	}
}

func TestMergeKeepsBaseOrderAndReplacesInPlace(t *testing.T) {
	cases := []struct{ base, patch, want string }{
		{`{}`, `{}`, `{}`},
		{`{"a":1}`, `{}`, `{"a":1}`},
		{`{}`, `{"a":1}`, `{"a":1}`},
		{`{"a":1,"b":{"x":1}}`, `{"b":{"y":2}}`, `{"a":1,"b":{"y":2}}`},
		{` { "a" : [1, 2] , "b":null } `, `{"b":"s","c":true}`, `{"a":[1,2],"b":"s","c":true}`},
		{`{"a":1}`, `{"a":2,"a":3}`, `{"a":3}`},
	}
	for _, c := range cases {
		got, err := Merge(c.base, c.patch)
		if err != nil {
			t.Fatalf("Merge(%s, %s): %v", c.base, c.patch, err)
		}
		if got != c.want {
			t.Errorf("Merge(%s, %s) = %s, want %s", c.base, c.patch, got, c.want)
		}
	}
	for _, bad := range [][2]string{{`[1]`, `{}`}, {`{}`, `[1]`}, {`{`, `{}`}, {`{}`, `{`}} {
		if _, err := Merge(bad[0], bad[1]); err == nil {
			t.Errorf("Merge(%s, %s) succeeded", bad[0], bad[1])
		}
	}
}
