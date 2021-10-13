package localreporting

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"syscall"
	"time"

	"github.com/mattn/go-sqlite3"
	"github.com/untangle/golang-shared/services/logger"
	"github.com/untangle/reportd/services/monitor"
)

// the main database connection
var DBMain *sql.DB

// setupDatabase is a routine that sets up the database and spawns writers and readers to handle requests
// THIS IS A ROUTINE
func setupDatabase(relations monitor.RoutineContextGroup) {
	var rtName = "setup_database"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	// Currently these are static, but could be pulled from settings?
	dbPath := "/tmp"
	dbFileName := "reports.db"

	// Determine database size limit and send to the cleaner routine
	dbSizeLimit := getSizeLimit(dbPath, 0.40)

	dbConnection, err := buildConnection(dbPath, dbFileName, dbSizeLimit)
	if err != nil {
		logger.Err("Unable to build database connection: %s\n", err)
		monitor.RoutineError(rtName)
		return
	}
	DBMain = dbConnection

	// enable auto vaccuum = FULL, this will clean up empty pages by moving them
	// to the end of the DB file. This will reclaim data from data that has been
	// removed from the database.
	runSQL(dbConnection, "PRAGMA auto_vacuum = FULL")

	createTables(dbConnection)

	// Startup cleaner before writer queues
	go tableCleaner(relations.Contexts["table_cleaner"], dbConnection, dbSizeLimit)

	intfStatsStmt, sessStatsStmt, threatPrevStatsStmt, err := createPreparedStatements(dbConnection)

	if err != nil {
		logger.Err("Unable to build prepared statements: %s\n", err)
		monitor.RoutineError(rtName)
		return
	}

	// Startup interface stats queue writer
	go intfStatsWriter(relations.Contexts["interface_stats_processor"], intfStatsStmt)

	// Startup sess stats queue writer
	go sessStatsWriter(relations.Contexts["session_stats_processor"], sessStatsStmt)

	// Startup session queue writer
	go sessWriter(relations.Contexts["session_processor"], dbConnection)

	// Start up threat prevention stats writer
	go threatPreventionStatsWriter(relations.Contexts["threat_prevention_stats_processor"], threatPrevStatsStmt)
}

// getSizeLimit uses the dbfilename and path to determine filesystem stats
// @param path - the path of the reports.db file
// @param percentage - the max percentage of DB space that should be allowed
// @return - the db size limit that should be enforced
func getSizeLimit(dbPath string, maxPercent float64) int64 {
	var stat syscall.Statfs_t

	// get the file system stats for the path where the database will be stored
	syscall.Statfs(dbPath, &stat)

	// set the database size limit to 60 percent of the total space available
	return int64(float64(stat.Bsize) * float64(stat.Blocks) * maxPercent)
}

// buildConnection will handle building the sql driver and setting up any connection strings
func buildConnection(dbPath string, dbFileName string, dbSizeLimit int64) (*sql.DB, error) {
	// register a custom driver with a connect hook where we can set our pragma's for
	// all connections that get created. This is needed because pragma's are applied
	// per connection. Since the sql package does connection pooling and management,
	// the hook lets us set the right pragma's for each and every connection.
	sql.Register("sqlite3_custom", &sqlite3.SQLiteDriver{ConnectHook: setPragmaSettings})

	dbVersion, _, _ := sqlite3.Version()
	dsn := fmt.Sprintf("file:%s/%s?mode=rwc", dbPath, dbFileName)
	dbMain, err := sql.Open("sqlite3_custom", dsn)

	if err != nil {
		logger.Err("Failed to open database: %s\n", err.Error())
		return nil, err
	}

	logger.Info("SQLite3 Database Version:%s  File:%s/%s  Limit:%d MB\n", dbVersion, dbPath, dbFileName, dbSizeLimit/(1024*1024))

	dbMain.SetMaxOpenConns(4)
	dbMain.SetMaxIdleConns(2)

	return dbMain, nil
}

// setPragmaSettings is used set the parameters we need for every database connection
func setPragmaSettings(conn *sqlite3.SQLiteConn) error {
	// turn off sync to disk after every transaction for improved performance
	if _, err := conn.Exec("PRAGMA synchronous = OFF", nil); err != nil {
		logger.Warn("Error setting synchronous: %v\n", err)
	}

	// store the rollback journal in memory for improved performance
	if _, err := conn.Exec("PRAGMA journal_mode = MEMORY", nil); err != nil {
		logger.Warn("Error setting journal_mode: %v\n", err)
	}

	// setting a busy timeout will allow the driver to retry for the specified
	// number of milliseconds instead of immediately returning SQLITE_BUSY when
	// a table is locked
	if _, err := conn.Exec("PRAGMA busy_timeout = 10000", nil); err != nil {
		logger.Warn("Error setting busy_timeout: %v\n", err)
	}

	return nil
}

func createPreparedStatements(dbConn *sql.DB) (*sql.Stmt, *sql.Stmt, *sql.Stmt, error) {

	// prepare the SQL used for interface_stats INSERT
	interfaceStatsStatement, err := dbConn.Prepare(GetInterfaceStatsInsertQuery())
	if err != nil {
		logger.Err("Failed to prepare interface_stats database statement: %s\n", err.Error())
		return nil, nil, nil, err
	}

	// prepare the SQL used for session_stats INSERT
	sessionStatsStatement, err := dbConn.Prepare(GetSessionStatsInsertQuery())
	if err != nil {
		logger.Err("Failed to prepare session_stats database statement: %s\n", err.Error())
		return nil, nil, nil, err
	}

	// prepare the SQL used for threat_prevention_stats INSERT
	threatPreventionStatsStatement, err := dbConn.Prepare(GetThreatPreventionStatsInsertQuery())
	if err != nil {
		logger.Err("Failed to prepare threat_prevention_stats database statement: %s\n", err.Error())
		return nil, nil, nil, err
	}

	return interfaceStatsStatement, sessionStatsStatement, threatPreventionStatsStatement, err
}

// makeSQLString makes a SQL string from a ReportEntry
func makeSQLString(reportEntry *ReportEntry) (string, error) {
	if reportEntry.Table == "" {
		return "", errors.New("Missing required attribute Table")
	}

	switch reportEntry.Type {
	case "TEXT":
		return makeTextSQLString(reportEntry)
	case "EVENTS":
		return makeEventsSQLString(reportEntry)
	case "CATEGORIES":
		return makeCategoriesSQLString(reportEntry)
	case "SERIES":
		return makeSeriesSQLString(reportEntry)
	case "CATEGORIES_SERIES":
		return makeCategoriesSeriesSQLString(reportEntry)
	}

	return "", errors.New("Unsupported reportEntry type")
}

// makeTextSQLString makes a SQL string from a TEXT type ReportEntry
func makeTextSQLString(reportEntry *ReportEntry) (string, error) {
	if reportEntry.QueryText.Columns == nil {
		return "", errors.New("Missing required attribute Columns")
	}

	sqlStr := "SELECT"
	for i, column := range reportEntry.QueryText.Columns {
		if column == "" {
			return "", errors.New("Missing column name")
		}
		if i == 0 {
			sqlStr += " " + column
		} else {
			sqlStr += ", " + column
		}
	}
	sqlStr += " FROM"
	sqlStr += " " + escape(reportEntry.Table)
	sqlStr += " WHERE"
	for i, condition := range reportEntry.Conditions {
		if i != 0 {
			sqlStr += " AND"
		}
		newStr, err := getConditionSQL(reportEntry, &condition)
		if err != nil {
			logger.Warn("Invalid condition: %v %v\n", condition, err)
			return "", err
		}
		sqlStr += newStr
	}
	return sqlStr, nil
}

// makeEventsSQLString makes a SQL string from a EVENTS type ReportEntry
func makeEventsSQLString(reportEntry *ReportEntry) (string, error) {
	var orderByColumn = "time_stamp"
	var order = "DESC"
	if reportEntry.QueryEvents.OrderByColumn != "" {
		orderByColumn = reportEntry.QueryEvents.OrderByColumn
	}
	if reportEntry.QueryEvents.OrderAsc {
		order = "ASC"
	}

	sqlStr := "SELECT * FROM"
	sqlStr += " " + escape(reportEntry.Table)
	sqlStr += " WHERE"
	for i, condition := range reportEntry.Conditions {
		if i != 0 {
			sqlStr += " AND"
		}
		newStr, err := getConditionSQL(reportEntry, &condition)
		if err != nil {
			logger.Warn("Invalid condition: %v %v\n", condition, err)
			return "", err
		}
		sqlStr += newStr
	}

	sqlStr += fmt.Sprintf(" ORDER BY %s %s", orderByColumn, order)

	if reportEntry.QueryEvents.Limit != 0 {
		sqlStr += fmt.Sprintf(" LIMIT %d", reportEntry.QueryEvents.Limit)
	}

	logger.Debug("Events SQL: %v\n", sqlStr)
	return sqlStr, nil
}

// makeCategoriesSQLString makes a SQL string from a CATEGORY type ReportEntry
func makeCategoriesSQLString(reportEntry *ReportEntry) (string, error) {
	if reportEntry.QueryCategories.GroupColumn == "" {
		return "", errors.New("Missing required attribute GroupColumn")
	}
	if reportEntry.QueryCategories.AggregationFunction == "" {
		return "", errors.New("Missing required attribute AggregationFunction")
	}
	if reportEntry.QueryCategories.AggregationValue == "" {
		return "", errors.New("Missing required attribute AggregationValue")
	}
	var orderByColumn = 2
	if reportEntry.QueryCategories.OrderByColumn < 0 || reportEntry.QueryCategories.OrderByColumn > 2 {
		return "", errors.New("Illegal value for OrderByColumn")
	}
	if reportEntry.QueryCategories.OrderByColumn != 0 {
		orderByColumn = reportEntry.QueryCategories.OrderByColumn
	}
	var order = "DESC"
	if reportEntry.QueryCategories.OrderAsc {
		order = "ASC"
	}

	sqlStr := "SELECT"
	sqlStr += " " + reportEntry.QueryCategories.GroupColumn
	sqlStr += ", " + reportEntry.QueryCategories.AggregationFunction + "(" + reportEntry.QueryCategories.AggregationValue + ")"
	sqlStr += " as value"
	sqlStr += " FROM " + escape(reportEntry.Table)
	sqlStr += " WHERE"
	for i, condition := range reportEntry.Conditions {
		if i != 0 {
			sqlStr += " AND"
		}
		newStr, err := getConditionSQL(reportEntry, &condition)
		if err != nil {
			logger.Warn("Invalid condition: %v %v\n", condition, err)
			return "", err
		}
		sqlStr += newStr
	}
	sqlStr += " GROUP BY " + reportEntry.QueryCategories.GroupColumn

	// remove "0" values
	sqlStr += " HAVING value > 0"

	sqlStr += fmt.Sprintf(" ORDER BY %d %s", orderByColumn, order)

	if reportEntry.QueryCategories.Limit != 0 {
		sqlStr += fmt.Sprintf(" LIMIT %d", reportEntry.QueryCategories.Limit)
	}

	logger.Debug("Categories SQL: %v\n", sqlStr)
	return sqlStr, nil
}

// makeSeriesSQLString makes a SQL string from a SERIES type ReportEntry
func makeSeriesSQLString(reportEntry *ReportEntry) (string, error) {
	if reportEntry.QuerySeries.Columns == nil {
		return "", errors.New("Missing required attribute Columns")
	}

	var timeIntervalSec = reportEntry.QuerySeries.TimeIntervalSeconds
	if timeIntervalSec == 0 {
		timeIntervalSec = 60
	}
	var timeIntervalMilli = int64(timeIntervalSec) * 1000

	startTime, err := findStartTime(*reportEntry)
	if err != nil {
		logger.Warn("start time condition not found: %v\n", reportEntry.Conditions)
		return "", err
	}

	endTime, err := findEndTime(*reportEntry)
	if err != nil {
		logger.Warn("end time condition not found\n")
		return "", err
	}

	tStr, err := makeTimelineSQLString(startTime, endTime, int64(timeIntervalSec))
	if err != nil {
		return "", err
	}

	qStr := "SELECT"
	qStr += fmt.Sprintf(" (%s/%d*%d) as time_trunc", getColumnName(reportEntry, "time_stamp"), timeIntervalMilli, timeIntervalMilli)
	for _, column := range reportEntry.QuerySeries.Columns {
		if column == "" {
			return "", errors.New("Missing column name")
		}
		qStr += ", " + column
	}
	qStr += " FROM " + escape(reportEntry.Table)
	qStr += " WHERE"
	for i, condition := range reportEntry.Conditions {
		if i != 0 {
			qStr += " AND"
		}
		newStr, err := getConditionSQL(reportEntry, &condition)
		if err != nil {
			logger.Warn("Invalid condition: %v %v\n", condition, err)
			return "", err
		}
		qStr += newStr
	}
	qStr += " GROUP BY time_trunc"

	sqlStr := "SELECT * FROM "
	sqlStr += " ( " + tStr + " ) as t1 "
	sqlStr += "LEFT JOIN "
	sqlStr += " ( " + qStr + " ) as t2 "
	sqlStr += " USING (time_trunc) "
	sqlStr += " ORDER BY time_trunc ASC "

	logger.Debug("Series Query SQL: %v\n", qStr)
	logger.Debug("Series SQL: %v\n", sqlStr)
	return sqlStr, nil
}

// makeCategoriesSeriesSQLString makes a SQL string from a CATEGORIES_SERIES type ReportEntry
func makeCategoriesSeriesSQLString(reportEntry *ReportEntry) (string, error) {
	if reportEntry.QueryCategories.Limit == 0 {
		return "", errors.New("Missing required attribute Limit")
	}

	distinctValues, err := getDistinctValues(reportEntry)
	logger.Debug("Distinct Values: %v\n", distinctValues)
	if err != nil {
		return "", err
	}

	var columns []string
	aggFunc := reportEntry.QueryCategories.AggregationFunction
	aggValue := reportEntry.QueryCategories.AggregationValue
	for _, column := range distinctValues {
		columnStr := aggFunc + "("
		columnStr += "CASE WHEN " + reportEntry.QueryCategories.GroupColumn + " = '" + escapeSingleTick(column) + "'"
		columnStr += " THEN " + aggValue + " END)"
		columnStr += " AS '" + escapeSingleTick(column) + "'"
		columns = append(columns, columnStr)
	}

	// MFW-967 - If the distinct values query didn't return any data we need to
	// create a dummy query that will generate an empty result set while also
	// consuming all of the query arguments that will be passed to the Query
	// function to prevent sql: expected x arguments, got y errors.
	if len(columns) == 0 {
		emptyQuery := "SELECT null"
		for i := range reportEntry.Conditions {
			if i == 0 {
				emptyQuery += " WHERE null != ?"
			} else {
				emptyQuery += " AND null != ?"
			}
		}
		emptyQuery += ";"
		return emptyQuery, nil
	}

	reportEntry.QuerySeries.Columns = columns

	sqlStr, err := makeSeriesSQLString(reportEntry)
	if sqlStr != "" {
		logger.Debug("Categories Series SQL: %v\n", sqlStr)
	}
	return sqlStr, err
}

//makeTimelineSQLString makes a SQL query string to provide the timeline to left join
//on time-based series reports to provide all datapoints
func makeTimelineSQLString(startTime string, endTime string, intervalSec int64) (string, error) {
	divisor := strconv.FormatInt(intervalSec*1000, 10)

	sqlStr := "SELECT DISTINCT (("
	sqlStr += "(" + startTime + "/" + divisor + ")"
	sqlStr += "+a*10000+b*1000+c*100+d*10+e" + ")*" + divisor + ") AS time_trunc FROM"
	sqlStr += " (" + makeSeqSQLString("a", 9) + "), "
	sqlStr += " (" + makeSeqSQLString("b", 10) + "), "
	sqlStr += " (" + makeSeqSQLString("c", 10) + "), "
	sqlStr += " (" + makeSeqSQLString("d", 10) + "), "
	sqlStr += " (" + makeSeqSQLString("e", 10) + ") "
	sqlStr += "WHERE time_trunc < " + endTime

	logger.Debug("Timeline SQL: %v\n", sqlStr)
	return sqlStr, nil
}

//makeSeriesSQLString makes a SQL string to get the sequence 0 to max-1
//example: maxSeriesSQLString("a",5)
//SELECT 0 as a UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
// 0, 1, 2, 3, 4
func makeSeqSQLString(columnName string, max int) string {
	if max < 0 {
		return ""
	}
	sqlStr := fmt.Sprintf("SELECT 0 as %s", columnName)
	for i := 1; i < max; i++ {
		sqlStr += fmt.Sprintf(" UNION SELECT %d", i)
	}
	return sqlStr
}

//dateFormat returns the proper sql string for the corresponding time
func dateFormat(t time.Time) string {
	//return t.Format(time.RFC3339)
	return strconv.FormatInt(t.UnixNano()/1e6, 10)
}

// getMapValue gets the value for the row for CATEGORIES reports
func getMapValue(m map[string]interface{}) string {
	// We don't care about the value
	// Delete it so we can find the value of the remaining entry
	delete(m, "value")
	// Get the value for the only remaining entry and return it
	for _, v := range m {
		str := fmt.Sprintf("%v", v)
		return str
	}
	return ""
}

// getDistinctValues returns the distinct values to be used
// in a CATEGORIES_SERIES report
func getDistinctValues(reportEntry *ReportEntry) ([]string, error) {
	categoriesSQLStr, err := makeCategoriesSQLString(reportEntry)
	if err != nil {
		return nil, err
	}
	rows, err := DBMain.Query(categoriesSQLStr, conditionValues(reportEntry.Conditions)...)
	if err != nil {
		logger.Warn("Failed to get Distinct values: %v\n", err)
		return nil, err
	}

	// rows is valid so make sure it gets closed
	defer rows.Close()

	categories, err := getRows(rows, reportEntry.QueryCategories.Limit)
	if err != nil {
		return nil, err
	}

	var values []string

	for _, v := range categories {
		str := getMapValue(v)
		if str != "" {
			values = append(values, str)
		}
	}

	return values, nil
}

// escape escapes quotes in as string
// this is a really gross way to handle SQL safety
// https://github.com/golang/go/issues/18478
func escape(source string) string {
	var j int
	if len(source) == 0 {
		return ""
	}
	tempStr := source[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\r':
			flag = true
			escape = '\r'
			break
		case '\n':
			flag = true
			escape = '\n'
			break
		case '\\':
			flag = true
			escape = '\\'
			break
		case '\'':
			flag = true
			escape = '\''
			break
		case '"':
			flag = true
			escape = '"'
			break
		case '\032':
			flag = true
			escape = 'Z'
			break
		default:
		}
		if flag {
			desc[j] = '\\'
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}
	return string(desc[0:j])
}

// escapeSingleTick will replace single ticks with a SQLite safe single tick escape sequence
// hopefully the golang sqlite driver will support better escape characters in the future
//
// this is a really gross way to handle SQL safety
// https://github.com/golang/go/issues/18478
// @param source - the string to fix the single quotes in
func escapeSingleTick(source string) string {
	var j int
	if len(source) == 0 {
		return ""
	}
	tempStr := source[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\'':
			flag = true
			escape = '\''
			break
		default:
		}
		if flag {
			desc[j] = escape
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}
	return string(desc[0:j])
}

// operatorSQL returns the sql equivalent of a condition operator
func operatorSQL(operator string) (string, error) {
	switch operator {
	case "EQ":
		return "=", nil
	case "NE":
		return "!=", nil
	case "GT":
		return ">", nil
	case "LT":
		return "<", nil
	case "GE":
		return ">=", nil
	case "LE":
		return "<=", nil
	case "LIKE":
		return "like", nil
	case "NOT_LIKE":
		return "not like", nil
	case "IS":
		return "is", nil
	case "IS_NOT":
		return "is not", nil
	case "IN":
		return "in", nil
	case "NOT_IN":
		return "not in", nil
	default:
		return "", errors.New("Invalid condition operator" + operator)
	}
}

// conditionValues returns a slice of the condition values of slice of ReportConditions
func conditionValues(conditions []ReportCondition) []interface{} {
	values := make([]interface{}, len(conditions))
	for i, condition := range conditions {
		values[i] = condition.Value
	}
	return values
}

// findStartTime returns the time value for the time_stamp > (GT) condition
func findStartTime(reportEntry ReportEntry) (string, error) {
	return findTime(reportEntry, "GT")
}

// findEndTime returns the time value for the time_stamp < (LT) condition
func findEndTime(reportEntry ReportEntry) (string, error) {
	return findTime(reportEntry, "LT")
}

// findTime returns the time value for the time_stamp operator condition
func findTime(reportEntry ReportEntry, operator string) (string, error) {
	for _, cond := range reportEntry.Conditions {
		if cond.Column == "time_stamp" && cond.Operator == operator {
			t, ok := cond.Value.(string)
			if ok {
				return t, nil
			}
		}
	}

	return "", errors.New("time not found")
}

// getConditionSQL returns the SQL for a given condition
func getConditionSQL(reportEntry *ReportEntry, condition *ReportCondition) (string, error) {
	opStr, err := operatorSQL(condition.Operator)
	if err != nil {
		return "", err
	}
	columnName := getColumnName(reportEntry, condition.Column)
	return " " + columnName + " " + opStr + " ?", nil
}

// getColumnName returns the proper column name providing the name
// this does a lookup in the disambiguation table and updates the column name if necessary
// to remove ambiguation of duplicate column names when doing joins
func getColumnName(reportEntry *ReportEntry, columnName string) string {
	if reportEntry.ColumnDisambiguation != nil {
		for _, disambi := range reportEntry.ColumnDisambiguation {
			if columnName == disambi.ColumnName {
				return disambi.NewColumnName
			}
		}
	}
	return columnName
}
