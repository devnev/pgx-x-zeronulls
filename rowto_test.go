package zeronulls_test

import (
	"testing"

	zeronulls "github.com/devnev/pgx-rowtozeronulls"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestWrapRowTo(t *testing.T) {
	const testValue int32 = 123
	p := pgtype.Int4Codec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, testValue)
	b, _ := p.Encode(testValue, nil)
	rows := &testRows{
		fds: []pgconn.FieldDescription{
			{Name: "num", DataTypeOID: pgtype.Int4OID, Format: pgtype.BinaryFormatCode},
		},
		data: [][][]byte{{b}},
	}
	ts, err := pgx.CollectOneRow(rows, zeronulls.WrapRowTo(pgx.RowToStructByName[testStruct]))
	if err != nil {
		t.Fatal(err)
	}
	if ts.Num != testValue {
		t.Fatalf("unexpected read value, expected %d, got %d", testValue, ts.Num)
	}
}

type testStruct struct {
	Num int32
}

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
