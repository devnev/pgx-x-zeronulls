package zeronulls

import (
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

func WrapRows(rows pgx.Rows) pgx.Rows {
	return &wrappedRows{Rows: rows}
}

type wrappedRows struct {
	pgx.Rows

	err       error
	tm        *pgtype.Map
	scanPlans []pgtype.ScanPlan
	scanTypes []reflect.Type
}

func (r *wrappedRows) Scan(dest ...any) error {
	var m *pgtype.Map
	switch {
	case r.Conn() != nil:
		m = r.Conn().TypeMap()
	case r.tm == nil:
		r.tm = pgtype.NewMap()
		fallthrough
	default:
		m = r.tm
	}

	// This impl is kept as close to the original pgx.baseRows.Scan as possible
	fieldDescriptions := r.FieldDescriptions()
	values := r.RawValues()

	if len(fieldDescriptions) != len(values) {
		err := fmt.Errorf("number of field descriptions must equal number of values, got %d and %d", len(fieldDescriptions), len(values))
		r.fatal(err)
		return err
	}

	if len(dest) == 1 {
		if rc, ok := dest[0].(pgx.RowScanner); ok {
			err := rc.ScanRow(r)
			return err
		}
	}

	if len(fieldDescriptions) != len(dest) {
		err := fmt.Errorf("number of field descriptions must equal number of destinations, got %d and %d", len(fieldDescriptions), len(dest))
		return err
	}

	if r.scanPlans == nil {
		r.scanPlans = make([]pgtype.ScanPlan, len(values))
		r.scanTypes = make([]reflect.Type, len(values))
		for i := range dest {
			(r.scanPlans)[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			(r.scanTypes)[i] = reflect.TypeOf(dest[i])
		}
	}

	for i, dst := range dest {
		if dst == nil {
			continue
		}

		if (r.scanTypes)[i] != reflect.TypeOf(dst) {
			(r.scanPlans)[i] = m.PlanScan(fieldDescriptions[i].DataTypeOID, fieldDescriptions[i].Format, dest[i])
			(r.scanTypes)[i] = reflect.TypeOf(dest[i])
		}

		// This is where the NULL handling happens
		if values[i] == nil {
			reflect.ValueOf(dst).Elem().Set(reflect.Zero((r.scanTypes)[i]))
		}

		err := (r.scanPlans)[i].Scan(values[i], dst)
		if err != nil {
			err = pgx.ScanArgError{ColumnIndex: i, FieldName: fieldDescriptions[i].Name, Err: err}
			return err
		}
	}

	return nil
}

func (r *wrappedRows) Err() error {
	if r.Rows.Err() != nil {
		r.err = r.Rows.Err()
	}
	return r.err
}

func (r *wrappedRows) fatal(err error) {
	if r.err != nil {
		return
	}
	r.err = err
	r.Close()
}
