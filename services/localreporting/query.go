package localreporting

import "database/sql"

// Query holds the results of a database query operation
type Query struct {
	ID   uint64
	Rows *sql.Rows
}

// QueryCategoriesOptions stores the query options for CATEGORY type reports
type QueryCategoriesOptions struct {
	GroupColumn         string `json:"groupColumn"`
	AggregationFunction string `json:"aggregationFunction"`
	AggregationValue    string `json:"aggregationValue"`
	Limit               int    `json:"limit"`
	OrderByColumn       int    `json:"orderByColumn"`
	OrderAsc            bool   `json:"orderAsc"`
}

// QueryTextOptions stores the query options for TEXT type reports
type QueryTextOptions struct {
	Columns []string `json:"columns"`
}

// QuerySeriesOptions stores the query options for SERIES type reports
type QuerySeriesOptions struct {
	Columns             []string `json:"columns"`
	TimeIntervalSeconds int      `json:"timeIntervalSeconds"`
}

// QueryEventsOptions stores the query options for EVENTS type reports
type QueryEventsOptions struct {
	OrderByColumn string `json:"orderByColumn"`
	OrderAsc      bool   `json:"orderAsc"`
	Limit         int    `json:"limit"`
}
