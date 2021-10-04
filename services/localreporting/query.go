// Query holds the results of a database query operation
type Query struct {
	ID   uint64
	Rows *sql.Rows
}