package localreporting

// ReportEntry is a report entry as defined in the JSON schema
type ReportEntry struct {
	UniqueID             string                       `json:"uniqueId"`
	Name                 string                       `json:"name"`
	Category             string                       `json:"category"`
	Description          string                       `json:"description"`
	DisplayOrder         int                          `json:"displayOrder"`
	ReadOnly             bool                         `json:"readOnly"`
	Type                 string                       `json:"type"`
	Table                string                       `json:"table"`
	Conditions           []ReportCondition            `json:"conditions"`
	ColumnDisambiguation []ReportColumnDisambiguation `json:"columnDisambiguation"`
	UserConditions       []ReportCondition            `json:"userConditions"`
	QueryCategories      QueryCategoriesOptions       `json:"queryCategories"`
	QueryText            QueryTextOptions             `json:"queryText"`
	QuerySeries          QuerySeriesOptions           `json:"querySeries"`
	QueryEvents          QueryEventsOptions           `json:"queryEvents"`
}

// ReportCondition holds a SQL reporting condition (ie client = 1.2.3.4)
type ReportCondition struct {
	Column   string      `json:"column"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

// ReportColumnDisambiguation remove and ambigious column names (used for joins)
type ReportColumnDisambiguation struct {
	ColumnName    string `json:"columnName"`
	NewColumnName string `json:"newColumnName"`
}
