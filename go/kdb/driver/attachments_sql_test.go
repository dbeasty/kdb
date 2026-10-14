package driver_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// Attachments through SQL: written by INSERT ... WITH ATTACHMENT and UPDATE ... SET ATTACHMENT,
// read by ATTACHMENT(), _attachments and ATTACHMENT_COUNT(), renamed and dropped.
func TestSQLAttachments(t *testing.T) {
	db := memDB(t, "files/docs")
	mustExec(t, db, "INSERT INTO docs (title) VALUES ('report') WITH ATTACHMENT 'a.txt' = X'68656c6c6f' MIME 'text/plain', ATTACHMENT 'b.bin' = ?", []byte("second"))

	var a, b []byte
	if err := db.QueryRow("SELECT ATTACHMENT('a.txt'), ATTACHMENT('b.bin') FROM docs WHERE title = 'report'").Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if string(a) != "hello" || string(b) != "second" {
		t.Fatalf("read %q %q", a, b)
	}
	if n := count(t, db, "SELECT ATTACHMENT_COUNT() FROM docs WHERE title = 'report'"); n != 2 {
		t.Fatalf("count %d", n)
	}

	var listing string
	if err := db.QueryRow("SELECT _attachments FROM docs").Scan(&listing); err != nil {
		t.Fatal(err)
	}
	var items []struct {
		ID, Name, Mime, Hash string
		Size                 int64
	}
	if err := json.Unmarshal([]byte(listing), &items); err != nil {
		t.Fatalf("%s: %v", listing, err)
	}
	if len(items) != 2 {
		t.Fatalf("listing %s", listing)
	}
	var aID string
	for _, it := range items {
		if it.Name == "a.txt" {
			aID = it.ID
			if it.Size != 5 || it.Mime != "text/plain" || len(it.Hash) != 64 {
				t.Fatalf("a.txt listed as %+v", it)
			}
		}
	}

	// Replace by name keeps the ID and the type; a new name creates one; a big value is a parameter.
	big := bytes.Repeat([]byte("0123456789abcdef"), 200_000) // 3.2 MB: several chunks
	res := mustExec(t, db, "UPDATE docs SET ATTACHMENT 'a.txt' = 'HELLO', ATTACHMENT 'c.dat' = ? WHERE title = 'report'", big)
	if n, _ := res.RowsAffected(); n != 1 {
		t.Fatalf("rows affected %d", n)
	}
	var c []byte
	var mime string
	if err := db.QueryRow("SELECT ATTACHMENT('a.txt'), ATTACHMENT('c.dat'), ATTACHMENT_MIME('a.txt') FROM docs").Scan(&a, &c, &mime); err != nil {
		t.Fatal(err)
	}
	if string(a) != "HELLO" || !bytes.Equal(c, big) || mime != "text/plain" {
		t.Fatalf("after update: %q, %d bytes, %q", a, len(c), mime)
	}
	if err := db.QueryRow("SELECT ATTACHMENT(?) FROM docs", aID).Scan(&a); err != nil || string(a) != "HELLO" {
		t.Fatalf("by id: %q %v", a, err)
	}

	// Rename, then drop by ID; the document and its JSON are untouched throughout.
	mustExec(t, db, "UPDATE docs RENAME ATTACHMENT 'a.txt' TO 'greeting.txt', DROP ATTACHMENT ID ? WHERE title = 'report'", aID)
	if n := count(t, db, "SELECT ATTACHMENT_COUNT() FROM docs"); n != 2 {
		t.Fatalf("count after drop %d", n)
	}
	if err := db.QueryRow("SELECT _attachments FROM docs").Scan(&listing); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(listing, "a.txt") || strings.Contains(listing, "greeting.txt") || !strings.Contains(listing, "c.dat") {
		t.Fatalf("listing after rename+drop: %s", listing)
	}
	var title string
	if err := db.QueryRow("SELECT title FROM docs").Scan(&title); err != nil || title != "report" {
		t.Fatalf("title %q %v", title, err)
	}

	// Errors: a duplicate name, NULL bytes, an unknown function arity.
	if _, err := db.Exec("INSERT INTO docs (title) VALUES ('x') WITH ATTACHMENT 'n' = 'a', ATTACHMENT 'n' = 'b'"); err == nil {
		t.Fatal("duplicate attachment names in one INSERT accepted")
	}
	if _, err := db.Exec("UPDATE docs SET ATTACHMENT 'n' = NULL"); err == nil {
		t.Fatal("NULL attachment accepted")
	}
	if _, err := db.Query("SELECT ATTACHMENT_COUNT(1) FROM docs"); err == nil {
		t.Fatal("ATTACHMENT_COUNT(1) accepted")
	}
	// Deleting the document takes its attachments with it.
	mustExec(t, db, "DELETE FROM docs WHERE title = 'report'")
	if n := count(t, db, "SELECT COUNT(*) FROM docs"); n != 0 {
		t.Fatalf("docs left: %d", n)
	}
}

func TestSQLAttachmentsTooBigForACell(t *testing.T) {
	db := memDB(t, "files/big")
	big := bytes.Repeat([]byte{7}, 17<<20)
	mustExec(t, db, "INSERT INTO big (k) VALUES (1) WITH ATTACHMENT 'x' = ?", big)
	if _, err := db.Query("SELECT ATTACHMENT('x') FROM big"); err == nil {
		// the error may surface on iteration
		rows, _ := db.Query("SELECT ATTACHMENT('x') FROM big")
		if rows != nil {
			for rows.Next() {
			}
			if rows.Err() == nil {
				t.Fatal("a 17 MiB attachment read into a cell")
			}
		}
	}
	if n := count(t, db, "SELECT ATTACHMENT_SIZE('x') FROM big"); n != 17<<20 {
		t.Fatalf("size %d", n)
	}
}
