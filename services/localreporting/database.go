package localreporting

import (
	"database/sql"
	"fmt"
	"syscall"

	"github.com/jsommerville-untangle/golang-shared/services/logger"
	"github.com/mattn/go-sqlite3"
	"github.com/untangle/reportd/services/monitor"
)

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

	// enable auto vaccuum = FULL, this will clean up empty pages by moving them
	// to the end of the DB file. This will reclaim data from data that has been
	// removed from the database.
	runSQL(dbConnection, "PRAGMA auto_vacuum = FULL")

	createTables(dbConnection)

	// Startup cleaner before writer queues
	go tableCleaner(relations.Contexts["table_cleaner"], dbConnection, dbSizeLimit)

	intfStatsStmt, sessStatsStmt, err := createPreparedStatements(dbConnection)

	if err != nil {
		logger.Err("Unable to build prepared statements: %s\n", err)
		monitor.RoutineError(rtName)
		return
	}

	// Startup interface stats queue writer
	go queueWriter(relations.Contexts["interface_stats_processor"], "interface_stats_processor", intfStatsStmt, interfaceStatsChannel)

	// Startup sess stats queue writer
	go queueWriter(relations.Contexts["session_stats_processor"], "session_stats_processor", sessStatsStmt, sessionStatsChannel)

	// Startup session queue writer
	go sessWriter(relations.Contexts["session_processor"], dbConnection)
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

func createPreparedStatements(dbConn *sql.DB) (*sql.Stmt, *sql.Stmt, error) {

	// prepare the SQL used for interface_stats INSERT
	interfaceStatsStatement, err := dbConn.Prepare(GetInterfaceStatsInsertQuery())
	if err != nil {
		logger.Err("Failed to prepare interface_stats database statement: %s\n", err.Error())
		return nil, nil, err
	}

	// prepare the SQL used for session_stats INSERT
	sessionStatsStatement, err := dbConn.Prepare(GetSessionStatsInsertQuery())
	if err != nil {
		logger.Err("Failed to prepare session_stats database statement: %s\n", err.Error())
		return nil, nil, err
	}

	return interfaceStatsStatement, sessionStatsStatement, err
}
