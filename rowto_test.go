package zeronulls_test

import (
	"testing"

	zeronulls "github.com/devnev/pgx-x-zeronulls"
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
		data: [][][]byte{
			{b},
			{nil},
		},
	}

	ts, err := pgx.CollectRows(rows, zeronulls.WrapRowTo(pgx.RowToStructByName[testStruct]))
	if err != nil {
		t.Fatal(err)
	}

	if len(ts) != 2 {
		t.Fatalf("unexpected number of rows, expected 2, got %d", len(ts))
	}
	if ts[0].Num != testValue {
		t.Fatalf("unexpected read value, expected %d, got %d", testValue, ts[0].Num)
	}
	if ts[1].Num != 0 {
		t.Fatalf("unexpected read value, expected 0, got %d", ts[1].Num)
	}
}

type testStruct struct {
	Num int32
}
