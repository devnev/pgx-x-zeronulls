package zeronulls_test

import (
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type testRows struct {
	fds     []pgconn.FieldDescription
	data    [][][]byte
	started bool
}

var _ pgx.Rows = (*testRows)(nil)

func (r *testRows) Close() {
	r.data = nil
}

func (r *testRows) Err() error {
	return nil
}

func (r *testRows) CommandTag() pgconn.CommandTag {
	return pgconn.CommandTag{}
}

func (r *testRows) FieldDescriptions() []pgconn.FieldDescription {
	return r.fds
}

func (r *testRows) Next() bool {
	if len(r.data) == 0 {
		return false
	}
	if r.started {
		r.data = r.data[1:]
	} else {
		r.started = true
	}
	return len(r.data) > 0
}

func (r *testRows) Scan(dest ...any) error {
	if len(dest) == 1 {
		if rs, _ := dest[0].(pgx.RowScanner); rs != nil {
			return rs.ScanRow(r)
		}
	}
	panic("unimplemented")
}

func (r *testRows) Values() ([]any, error) {
	panic("unimplemented")
}

func (r *testRows) RawValues() [][]byte {
	return r.data[0]
}

func (r *testRows) Conn() *pgx.Conn {
	return nil
}
