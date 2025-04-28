package zeronulls

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// Wrap a pgx.RowTo* function such that NULL values set the destination to its zero value.
// e.g. pgx.CollectRows(zeronulls.WrapRowTo(pgx.RowToStruct[Article]))
func WrapRowTo[T any](fn pgx.RowToFunc[T]) pgx.RowToFunc[T] {
	// Scan plans are stored across calls to the wrapped function to reduce the number of PlanScan calls
	var (
		scanPlans []pgtype.ScanPlan
		scanTypes []reflect.Type
		tm        *pgtype.Map
	)

	// The resulting call chain is (usually):
	// pgx.Collect* -> this wrapper -> wrapped fn -> wrappedRow.Scan -> pgx.baseRow.Scan -> rowScanner.ScanRow
	return func(row pgx.CollectableRow) (T, error) {
		return fn(wrappedRow{
			CollectableRow: row,
			scanPlans:      &scanPlans,
			scanTypes:      &scanTypes,
			tm:             &tm,
		})
	}
}

type wrappedRow struct {
	pgx.CollectableRow
	scanPlans *[]pgtype.ScanPlan
	scanTypes *[]reflect.Type
	tm        **pgtype.Map
}

var _ pgx.CollectableRow = wrappedRow{}

// We intercept the Scan call from the wrapped RowTo* function to replace the destination slice with a wrapper that
// implements pgx.RowScanner
func (r wrappedRow) Scan(dest ...any) error {
	return r.CollectableRow.Scan(rowScanner{
		dest:      dest,
		scanPlans: r.scanPlans,
		scanTypes: r.scanTypes,
		tm:        r.tm,
	})
}

type rowScanner struct {
	dest      []any
	scanPlans *[]pgtype.ScanPlan
	scanTypes *[]reflect.Type
	tm        **pgtype.Map
}

var _ pgx.RowScanner = rowScanner{}

// We implement the pgx.RowScanner interface to obtain the original Row and scan it's raw values to the destination
// slice using a customized scanner that handles NULLs by setting the destination to its zero value.
func (s rowScanner) ScanRow(rows pgx.Rows) error {
	var tm *pgtype.Map
	switch {
	case rows.Conn() != nil:
		tm = rows.Conn().TypeMap()
	case *s.tm == nil:
		*s.tm = pgtype.NewMap()
		fallthrough
	default:
		tm = *s.tm
	}

	// This impl is kept as close to the original pgx.baseRows.Scan as possible
	fieldDescriptions := rows.FieldDescriptions()
	dest := s.dest
	values := rows.RawValues()

	if len(dest) == 1 {
		if rc, ok := dest[0].(pgx.RowScanner); ok {
			err := rc.ScanRow(rows)
			return err
		}
	}

	if len(fieldDescriptions) != len(dest) {
		err := fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(fieldDescriptions), len(dest))
		return err
	}

	if *s.scanPlans == nil {
		*s.scanPlans = make([]pgtype.ScanPlan, len(values))
		*s.scanTypes = make([]reflect.Type, len(values))
		for i := range dest {
			(*s.scanPlans)[i] = tm.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			(*s.scanTypes)[i] = reflect.TypeOf(dest[i])
		}
	}

	for i, dst := range dest {
		if dst == nil {
			continue
		}

		if (*s.scanTypes)[i] != reflect.TypeOf(dst) {
			(*s.scanPlans)[i] = tm.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			(*s.scanTypes)[i] = reflect.TypeOf(dest[i])
		}

		// This is where the NULL handling happens
		if values[i] == nil && (*s.scanTypes)[i].Kind() == reflect.Ptr {
			reflect.ValueOf(dst).Elem().Set(reflect.Zero((*s.scanTypes)[i].Elem()))
			continue
		}

		err := (*s.scanPlans)[i].Scan(values[i], dst)
		if err != nil {
			err = pgx.ScanArgError{ColumnIndex: i, FieldName: fieldDescriptions[i].Name, Err: err}
			return err
		}
	}

	return nil
}
