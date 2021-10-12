package localreporting

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/untangle/golang-shared/services/logger"
	"github.com/untangle/golang-shared/services/overseer"
)

// The queries map tracks async database requests from the admin interface. A call
// is made to CreateQuery, the results are fetched via one or more calls to
// GetData, followed by a final call to CloseQuery for cleanup.
var queriesMap = make(map[uint64]*Query)
var queriesLock sync.RWMutex
var queryID uint64

var preparedStatements = map[string]*sql.Stmt{}
var preparedStatementsMutex = sync.RWMutex{}

// CreateQuery submits a database query and returns the results
func CreateQuery(reportEntryStr string) (*Query, error) {
	var clean bool
	var err error
	reportEntry := &ReportEntry{}

	err = unmarshall(string(reportEntryStr), reportEntry)
	if err != nil {
		logger.Err("json.Unmarshal error: %s\n", err)
		return nil, err
	}

	mergeConditions(reportEntry)
	err = addOrUpdateTimestampConditions(reportEntry)
	if err != nil {
		logger.Err("Timestamp condition error: %s\n", err)
		return nil, err
	}

	var rows *sql.Rows
	var sqlStmt *sql.Stmt

	sqlStmt, clean, err = getPreparedStatement(reportEntry)
	if err != nil {
		logger.Warn("Failed to get prepared SQL: %v\n", err)
		return nil, err
	}
	values := conditionValues(reportEntry.Conditions)

	rows, err = sqlStmt.Query(values...)

	// If the prepared statment was not cached the clean flag will be true which
	// means we have to close the statement so the memory can be released.
	// Check and do statement cleanup first to make the code a little cleaner
	// rather than doing it both in and following the Query error handler.
	if clean {
		sqlStmt.Close()
	}

	// now check for any error returned from sqlStmt.Query
	if err != nil {
		logger.Err("sqlStmt.Query error: %s\n", err)
		return nil, err
	}

	q := new(Query)
	q.ID = atomic.AddUint64(&queryID, 1)
	q.Rows = rows

	queriesLock.Lock()
	queriesMap[q.ID] = q
	queriesLock.Unlock()

	// I believe this is here to cleanup stray queries that may be locking the database?
	go func() {
		time.Sleep(60 * time.Second)
		cleanupQuery(q)
	}()
	return q, nil
}

// getPreparedStatement retrieves the prepared statements from the prepared statements map
// and creates it if it does not exist. This largely takes from the ideas here:
// https://thenotexpert.com/golang-sql-recipe/
// MFW-1056 added logic to detect and not cache queries that will always be unique. This
// happens with series type queries where timestamp and other values are passed inline
// rather than as placeholders that reference argumented values passed into the query.
func getPreparedStatement(reportEntry *ReportEntry) (*sql.Stmt, bool, error) {
	var stmt *sql.Stmt
	var present bool

	query, err := makeSQLString(reportEntry)
	if err != nil {
		logger.Warn("Failed to make SQL: %v\n", err)
		return nil, false, err
	}

	preparedStatementsMutex.RLock()
	if stmt, present = preparedStatements[query]; present {
		preparedStatementsMutex.RUnlock()
		return stmt, false, nil
	}

	// If not present, let's create.
	preparedStatementsMutex.RUnlock()
	// Locking for both reading and writing now.
	preparedStatementsMutex.Lock()
	defer preparedStatementsMutex.Unlock()

	// There is a tiny possibility that one goroutine creates a statement but another one gets here as well.
	// Then the latter will receive the prepared statement instead of recreating it.
	if stmt, present = preparedStatements[query]; present {
		return stmt, false, nil
	}

	stmt, err = DBMain.Prepare(query)
	if err != nil {
		return nil, false, err
	}

	// Complex UI series queries have embedded timestamps and such that make them unique so
	// we return without adding to our cache and tell the caller to do statement cleanup.
	if reportEntry.Type == "SERIES" || reportEntry.Type == "CATEGORIES_SERIES" {
		return stmt, true, nil
	}

	overseer.AddCounter("reports_prepared_statement_cache", 1)
	preparedStatements[query] = stmt
	return stmt, false, nil
}

// GetData returns the data for the provided QueryID
func GetData(queryID uint64) (string, error) {
	queriesLock.RLock()
	q := queriesMap[queryID]
	queriesLock.RUnlock()
	if q == nil {
		logger.Warn("Query not found: %d\n", queryID)
		return "", errors.New("Query ID not found")
	}
	result, err := getRows(q.Rows, 1000)
	if err != nil {
		return "", err
	}
	jsonData, err := json.Marshal(result)
	if err != nil {
		return "", err
	}

	return string(jsonData), nil
}

// CloseQuery closes the query now
func CloseQuery(queryID uint64) (string, error) {
	queriesLock.RLock()
	q := queriesMap[queryID]
	queriesLock.RUnlock()
	if q == nil {
		logger.Warn("Query not found: %d\n", queryID)
		return "", errors.New("Query ID not found")
	}
	cleanupQuery(q)
	return "Success", nil
}

// cleanupQuery deletes the passed in query from the query map and closes any rows the query has open
func cleanupQuery(query *Query) {
	logger.Debug("cleanupQuery(%d)\n", query.ID)
	queriesLock.Lock()
	defer queriesLock.Unlock()
	delete(queriesMap, query.ID)
	if query.Rows != nil {
		query.Rows.Close()
		query.Rows = nil
	}
	logger.Debug("cleanupQuery(%d) finished\n", query.ID)
}

// getRows gets the actual table data
func getRows(rows *sql.Rows, limit int) ([]map[string]interface{}, error) {
	if rows == nil {
		return nil, errors.New("Invalid argument")
	}
	if limit < 1 {
		return nil, errors.New("Invalid limit")
	}

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	columnCount := len(columns)

	tableData := make([]map[string]interface{}, 0)
	values := make([]interface{}, columnCount)
	valuePtrs := make([]interface{}, columnCount)

	for i := 0; i < limit && rows.Next(); i++ {
		for i := 0; i < columnCount; i++ {
			valuePtrs[i] = &values[i]
		}
		rows.Scan(valuePtrs...)
		entry := make(map[string]interface{})
		for i, col := range columns {
			var v interface{}
			val := values[i]
			b, ok := val.([]byte)
			if ok {
				v = string(b)
			} else {
				v = val
			}
			entry[col] = v
		}
		tableData = append(tableData, entry)
	}

	return tableData, nil
}

// addDefaultTimestampConditions adds time_stamp > X and time_stamp < Y
// to userConditions if they are not already present
func addOrUpdateTimestampConditions(reportEntry *ReportEntry) error {
	var err error
	err = addOrUpdateTimestampCondition(reportEntry, "GT", time.Now().Add(-1*time.Duration(100)*time.Hour))
	if err != nil {
		return err
	}

	err = addOrUpdateTimestampCondition(reportEntry, "LT", time.Now().Add(time.Duration(1)*time.Minute))
	if err != nil {
		return err
	}

	return nil
}

func addOrUpdateTimestampCondition(reportEntry *ReportEntry, operator string, defaultTime time.Time) error {
	var err error

	for i, cond := range reportEntry.Conditions {
		if cond.Column == "time_stamp" && cond.Operator == operator {
			var condition = &reportEntry.Conditions[i]

			// if a condition is found, set the condition value to a time.Time
			// check if its a string or int
			var timeEpochSec int64
			jsonNumber, ok := condition.Value.(json.Number)
			if ok {
				// time is specified in milliseconds, lets just use seconds
				timeEpochMillisecond, err := jsonNumber.Int64()
				timeEpochSec = timeEpochMillisecond / 1000
				if err != nil {
					logger.Warn("Invalid JSON number for time_stamp condition: %v\n", condition.Value)
					return err
				}
			} else {
				valueStr, ok := condition.Value.(string)
				if ok {
					// otherwise just convert the epoch value to a time.Time
					timeEpochSec, err = strconv.ParseInt(valueStr, 10, 64)
					if err != nil {
						logger.Warn("Invalid JSON number for time_stamp condition: %v\n", condition.Value)
						return err
					}
				}
			}

			// update value to actual Time value expected by sqlite3
			condition.Value = dateFormat(time.Unix(timeEpochSec, 0))
			return nil
		}
	}

	// if no time found, set defaultTime
	newCondition := ReportCondition{Column: "time_stamp", Operator: operator, Value: dateFormat(defaultTime)}
	reportEntry.Conditions = append(reportEntry.Conditions, newCondition)

	return nil
}

// mergeConditions moves any user specified conditions in UserConditions
// to Conditions
func mergeConditions(reportEntry *ReportEntry) {
	if len(reportEntry.UserConditions) > 0 {
		reportEntry.Conditions = append(reportEntry.Conditions, reportEntry.UserConditions...)
	}
	reportEntry.UserConditions = []ReportCondition{}
}

func unmarshall(reportEntryStr string, reportEntry *ReportEntry) error {
	decoder := json.NewDecoder(strings.NewReader(reportEntryStr))
	decoder.UseNumber()
	err := decoder.Decode(reportEntry)
	// err := json.Unmarshal(reportEntryBytes, reportEntry)
	if err != nil {
		return err
	}
	return nil
}
