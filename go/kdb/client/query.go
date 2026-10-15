package client

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/limidus/kdb/go/kdb/wire"
)

// Query runs one SQL SELECT against ns and decodes each result row into dest, which must be a
// pointer to a slice of a caller-defined struct - mirrors mongo-driver's cur.All(ctx, &out)
// idiom. Each result column is matched to a struct field by case-insensitive name.
func (c *Client) Query(ctx context.Context, ns string, sqlText string, args []any, dest any) error {
	result, err := c.execSql(ctx, ns, sqlText, args)
	if err != nil {
		return err
	}
	return decodeRows(result.Columns, result.Rows, dest)
}

// QueryRaw runs one SQL SELECT and returns the raw column names and string-typed rows, for
// callers (CLIs, test tooling) that don't have a struct shape to decode into - columns like
// "_doc" can't map to an exported struct field name, so Query's reflection path can't reach
// them.
func (c *Client) QueryRaw(ctx context.Context, ns string, sqlText string, args []any) (columns []string, rows [][]string, err error) {
	result, err := c.execSql(ctx, ns, sqlText, args)
	if err != nil {
		return nil, nil, err
	}
	return result.Columns, result.Rows, nil
}

// QueryValues runs one SQL SELECT and returns each cell as a typed Go value: nil for NULL, int64,
// float64, bool, string, or json.RawMessage for a JSON document or value. Unlike QueryRaw it
// tells a NULL from an empty string and a number from text that looks like one.
//
// The types come from the server (wire.SqlResultMessage.CellKinds). A server that predates them
// sends none, and then every cell is returned as a string - NULL included, as "".
func (c *Client) QueryValues(ctx context.Context, ns string, sqlText string, args []any) (columns []string, rows [][]any, err error) {
	result, err := c.execSql(ctx, ns, sqlText, args)
	if err != nil {
		return nil, nil, err
	}
	rows, err = typedRows(result.Rows, result.cellKinds)
	return result.Columns, rows, err
}

// typedRows converts text cells to the values their kinds name. kinds is row by row, one per
// cell; nil means the server sent none and every cell stays a string.
func typedRows(rows [][]string, kinds []wire.CellKind) ([][]any, error) {
	n := 0
	for _, row := range rows {
		n += len(row)
	}
	if kinds != nil && len(kinds) != n {
		return nil, fmt.Errorf("kdb: result has %d cells but %d cell kinds", n, len(kinds))
	}
	values := make([]any, n)
	out := make([][]any, len(rows))
	k := 0
	for i, row := range rows {
		start := k
		for _, cell := range row {
			kind := wire.CellKindString
			if kinds != nil {
				kind = kinds[k]
			}
			v, err := typedCell(cell, kind)
			if err != nil {
				return nil, err
			}
			values[k] = v
			k++
		}
		out[i] = values[start:k:k]
	}
	return out, nil
}

func typedCell(cell string, kind wire.CellKind) (any, error) {
	switch kind {
	case wire.CellKindNull:
		return nil, nil
	case wire.CellKindLong:
		return strconv.ParseInt(cell, 10, 64)
	case wire.CellKindDouble:
		return strconv.ParseFloat(cell, 64)
	case wire.CellKindBool:
		return cell == "1" || cell == "true", nil // Go servers write 1/0, the Kotlin one true/false
	case wire.CellKindJSON:
		return json.RawMessage(cell), nil
	case wire.CellKindBytes:
		return base64.StdEncoding.DecodeString(cell)
	default:
		// A kind this build does not know yet is still text.
		return cell, nil
	}
}

// Exec runs one non-SELECT SQL statement (schema/DDL). Most of Zolik's write paths should
// prefer PutJSON/Upsert/Commit; Exec exists for DDL and the occasional write better expressed as
// SQL. An INSERT is auto-committed immediately (this call is one client-visible unit of work,
// not two).
//
// It returns the number of rows the statement affected, so a DELETE that matched nothing is
// distinguishable from one that removed a row. Concurrent calls on one namespace are safe.
func (c *Client) Exec(ctx context.Context, ns string, sqlText string, args []any) (int, error) {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return 0, err
	}
	st.dml.Lock()
	defer st.dml.Unlock()
	result, err := c.execSqlOnSession(ctx, ns, st.sessionID, sqlText, args)
	if err != nil {
		return 0, err
	}
	if !result.needsCommit {
		return result.rowsAffected, nil
	}
	commitMsg := wire.TxCommitMessage{
		H:         wire.Header{MessageType: wire.MsgTxCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: ns,
		SessionID: st.sessionID,
	}
	reply, err := c.request(ctx, commitMsg)
	if err != nil {
		return 0, err
	}
	switch r := reply.(type) {
	case wire.ConflictReportMessage:
		return 0, decodeConflictError(r.ReportBytes, r.RetryAfterMs)
	case wire.SqlResultMessage:
		if r.Error != nil {
			return 0, classifiedError(*r.Error, r.ErrorCode, r.RetryAfterMs)
		}
		// The statement's count, not the commit's: the commit reports operations applied.
		return result.rowsAffected, nil
	default:
		return 0, fmt.Errorf("kdb: unexpected commit response %T", reply)
	}
}

// QueryRawPages runs a SELECT in pages of at most pageSize rows and hands each page to fn, in
// order, until the result is exhausted or fn returns an error. Unlike QueryRaw it is not limited
// by the server's --max-result-rows: only a page has to fit in one reply. Every page reads the
// same snapshot (the commit the first page read at), so writes made meanwhile are not seen. The
// server re-runs the query for each page, so a query with ORDER BY pays its sort per page and a
// deep page of an unordered one re-reads the rows before it; prefer a selective WHERE where you
// can. pageSize <= 0 uses the server's maximum.
func (c *Client) QueryRawPages(ctx context.Context, ns string, sqlText string, args []any, pageSize int, fn func(columns []string, rows [][]string) error) error {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return err
	}
	if pageSize <= 0 {
		pageSize = 1 << 30 // clamped by the server to its result bound
	}
	token := ""
	for {
		result, err := c.execSqlPaged(ctx, ns, st.sessionID, sqlText, args, pageSize, token)
		if err != nil {
			return err
		}
		if err := fn(result.Columns, result.Rows); err != nil {
			return err
		}
		if result.continuation == "" {
			return nil
		}
		token = result.continuation
	}
}

// QueryValuesPages is QueryRawPages with typed cells, as QueryValues returns them: a SELECT read
// in pages of at most pageSize rows, every page from the same snapshot, each handed to fn with
// NULLs, numbers, booleans and JSON decoded.
func (c *Client) QueryValuesPages(ctx context.Context, ns string, sqlText string, args []any, pageSize int, fn func(columns []string, rows [][]any) error) error {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return err
	}
	if pageSize <= 0 {
		pageSize = 1 << 30 // clamped by the server to its result bound
	}
	token := ""
	for {
		result, err := c.execSqlPaged(ctx, ns, st.sessionID, sqlText, args, pageSize, token)
		if err != nil {
			return err
		}
		rows, err := typedRows(result.Rows, result.cellKinds)
		if err != nil {
			return err
		}
		if err := fn(result.Columns, rows); err != nil {
			return err
		}
		if result.continuation == "" {
			return nil
		}
		token = result.continuation
	}
}

type sqlExecResult struct {
	continuation string
	Columns      []string
	Rows         [][]string
	cellKinds    []wire.CellKind
	needsCommit  bool
	rowsAffected int
	resolvedHex  string
}

func (c *Client) execSql(ctx context.Context, ns string, sqlText string, args []any) (sqlExecResult, error) {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return sqlExecResult{}, err
	}
	return c.execSqlOnSession(ctx, ns, st.sessionID, sqlText, args)
}

// execSqlOnSession sends one SQL_EXEC on a specific session.
func (c *Client) execSqlOnSession(ctx context.Context, ns, sessionID string, sqlText string, args []any) (sqlExecResult, error) {
	return c.execSqlPaged(ctx, ns, sessionID, sqlText, args, 0, "")
}

func (c *Client) execSqlPaged(ctx context.Context, ns, sessionID string, sqlText string, args []any, pageSize int, continuation string) (sqlExecResult, error) {
	var paramsJSON *string
	if len(args) > 0 {
		b, err := json.Marshal(args)
		if err != nil {
			return sqlExecResult{}, err
		}
		s := string(b)
		paramsJSON = &s
	}
	msg := wire.SqlExecMessage{
		H:              wire.Header{MessageType: wire.MsgSqlExec, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace:      ns,
		SessionID:      sessionID,
		SQL:            sqlText,
		ParametersJSON: paramsJSON,
		PageSize:       pageSize,
		Continuation:   continuation,
	}
	reply, err := c.request(ctx, msg)
	if err != nil {
		return sqlExecResult{}, err
	}
	result, ok := reply.(wire.SqlResultMessage)
	if !ok {
		return sqlExecResult{}, fmt.Errorf("kdb: expected SqlResult, got %T", reply)
	}
	if result.Error != nil {
		return sqlExecResult{}, classifiedError(*result.Error, result.ErrorCode, result.RetryAfterMs)
	}
	return sqlExecResult{Columns: result.Columns, Rows: result.Rows, cellKinds: result.CellKinds, needsCommit: !result.ReadOnly,
		rowsAffected: result.RowsAffected, resolvedHex: result.ResolvedCommitHex,
		continuation: result.ContinuationToken}, nil
}

// decodeRows fills dest (a *[]T for a struct type T) from columns/rows, matching each column to
// a struct field by case-insensitive name. Unmatched columns and unexported fields are skipped.
func decodeRows(columns []string, rows [][]string, dest any) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("kdb: dest must be a non-nil pointer to a slice, got %T", dest)
	}
	sliceVal := v.Elem()
	elemType := sliceVal.Type().Elem()
	if elemType.Kind() != reflect.Struct {
		return fmt.Errorf("kdb: dest slice element must be a struct, got %s", elemType.Kind())
	}

	fieldIndexByColumn := make(map[string]int, elemType.NumField())
	for i := 0; i < elemType.NumField(); i++ {
		f := elemType.Field(i)
		if !f.IsExported() {
			continue
		}
		fieldIndexByColumn[strings.ToLower(f.Name)] = i
	}

	out := reflect.MakeSlice(sliceVal.Type(), 0, len(rows))
	for _, row := range rows {
		elem := reflect.New(elemType).Elem()
		for i, col := range columns {
			if i >= len(row) {
				continue
			}
			fieldIdx, ok := fieldIndexByColumn[strings.ToLower(col)]
			if !ok {
				continue
			}
			if err := setFieldFromString(elem.Field(fieldIdx), row[i]); err != nil {
				return fmt.Errorf("kdb: column %q into field %q: %w", col, elemType.Field(fieldIdx).Name, err)
			}
		}
		out = reflect.Append(out, elem)
	}
	sliceVal.Set(out)
	return nil
}

func setFieldFromString(field reflect.Value, s string) error {
	switch field.Kind() {
	case reflect.String:
		field.SetString(s)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if s == "" {
			return nil
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		field.SetInt(n)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if s == "" {
			return nil
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		field.SetUint(n)
	case reflect.Float32, reflect.Float64:
		if s == "" {
			return nil
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return err
		}
		field.SetFloat(f)
	case reflect.Bool:
		if s == "" {
			return nil
		}
		b, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		field.SetBool(b)
	default:
		return fmt.Errorf("unsupported field kind %s", field.Kind())
	}
	return nil
}
