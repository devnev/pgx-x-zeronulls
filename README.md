# pgx-x-zeronulls

Use `pgx.Collect*` and `pgx.RowTo*` with NULLs being treated as zero values.

```go
type Article struct {
	ID        int64
	Title     string
	Content   string
	Published bool
}

func Get(db *pgx.Conn, id int64) (*Article, error) {
	rows, err := db.Query("SELECT id, title, content, published FROM articles WHERE id = $1", id)
	if err != nil {
		return nil, err
	}

	return pgx.CollectExactlyOneRow(zeronulls.WrapRows(rows), pgx.RowToAddrOfStructByName[Article])
	// or
	return pgx.CollectExactlyOneRow(rows, zeronulls.WrapRowTo(pgx.RowToAddrOfStructByName[Article]))
}
```
