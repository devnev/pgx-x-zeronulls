package zeronulls_test

import (
	"testing"

	zeronulls "github.com/devnev/pgx-x-zeronulls"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestWrapRows(t *testing.T) {
	const testValue int32 = 123

	p := pgtype.Int4Codec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, testValue)
	b, _ := p.Encode(testValue, nil)
	rows := &testRows{
		fds: []pgconn.FieldDescription{
			{Name: "num", DataTypeOID: pgtype.Int4OID, Format: pgtype.BinaryFormatCode},
		},
		data: [][][]byte{{b}},
	}

	ts, err := pgx.CollectOneRow(zeronulls.WrapRows(rows), pgx.RowToStructByName[testStruct])
	if err != nil {
		t.Fatal(err)
	}

	if ts.Num != testValue {
		t.Fatalf("unexpected read value, expected %d, got %d", testValue, ts.Num)
	}
}
