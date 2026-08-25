package client

import (
	"context"
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

// Exec runs one non-SELECT SQL statement (schema/DDL). Most of Zolik's write paths should
// prefer PutJSON/Upsert/Commit; Exec exists for DDL and the occasional write better expressed as
// SQL. An INSERT is auto-committed immediately (this call is one client-visible unit of work,
// not two).
func (c *Client) Exec(ctx context.Context, ns string, sqlText string, args []any) error {
	result, err := c.execSql(ctx, ns, sqlText, args)
	if err != nil {
		return err
	}
	if !result.needsCommit {
		return nil
	}
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return err
	}
	commitMsg := wire.TxCommitMessage{
		H:         wire.Header{MessageType: wire.MsgTxCommit, ProtocolVersion: wire.KdbWireProtocolVersion, CorrelationID: c.nextCorrelation()},
		Namespace: ns,
		SessionID: st.sessionID,
	}
	reply, err := c.request(ctx, commitMsg)
	if err != nil {
		return err
	}
	switch r := reply.(type) {
	case wire.ConflictReportMessage:
		return decodeConflictError(r.ReportBytes)
	case wire.SqlResultMessage:
		if r.Error != nil {
			return fmt.Errorf("kdb: %s", *r.Error)
		}
		return nil
	default:
		return fmt.Errorf("kdb: unexpected commit response %T", reply)
	}
}

type sqlExecResult struct {
	Columns     []string
	Rows        [][]string
	needsCommit bool
}

func (c *Client) execSql(ctx context.Context, ns string, sqlText string, args []any) (sqlExecResult, error) {
	st, err := c.ensureNamespace(ctx, ns)
	if err != nil {
		return sqlExecResult{}, err
	}
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
		SessionID:      st.sessionID,
		SQL:            sqlText,
		ParametersJSON: paramsJSON,
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
		return sqlExecResult{}, fmt.Errorf("kdb: %s", *result.Error)
	}
	return sqlExecResult{Columns: result.Columns, Rows: result.Rows, needsCommit: !result.ReadOnly}, nil
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
