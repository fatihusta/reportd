package localreporting

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/untangle/golang-shared/services/logger"
	"github.com/untangle/reportd/services/monitor"
)

/*
These functions provide the low level framework for capturing session and
interface stats in the system database. This data is used to display
the dashboard and other session details in the management interface.

The GetxxxColumnList functions return the list of columns in the corresponding
table in the order they appear in the prepared INSERT statment. When logging these
events to the database, the values MUST be appended to the values interface array
in this order so the correct values are written to the corresponding columns.

The GetxxxInsertQuery functions return a string generated using the GetxxxColumnList
function that is used to create the prepared statement for inserting each event
type in the database.
*/

// GetInterfaceStatsColumnList returns the columns in the interface_stats database table
func GetInterfaceStatsColumnList() []string {
	return []string{
		"time_stamp",
		"interface_id",
		"interface_name",
		"device_name",
		"is_wan",
		"latency_1",
		"latency_5",
		"latency_15",
		"latency_variance",
		"passive_latency_1",
		"passive_latency_5",
		"passive_latency_15",
		"passive_latency_variance",
		"active_latency_1",
		"active_latency_5",
		"active_latency_15",
		"active_latency_variance",
		"jitter_1",
		"jitter_5",
		"jitter_15",
		"jitter_variance",
		"ping_timeout",
		"ping_timeout_rate",
		"rx_bytes",
		"rx_bytes_rate",
		"rx_packets",
		"rx_packets_rate",
		"rx_errs",
		"rx_errs_rate",
		"rx_drop",
		"rx_drop_rate",
		"rx_fifo",
		"rx_fifo_rate",
		"rx_frame",
		"rx_frame_rate",
		"rx_compressed",
		"rx_compressed_rate",
		"rx_multicast",
		"rx_multicast_rate",
		"tx_bytes",
		"tx_bytes_rate",
		"tx_packets",
		"tx_packets_rate",
		"tx_errs",
		"tx_errs_rate",
		"tx_drop",
		"tx_drop_rate",
		"tx_fifo",
		"tx_fifo_rate",
		"tx_colls",
		"tx_colls_rate",
		"tx_carrier",
		"tx_carrier_rate",
		"tx_compressed",
		"tx_compressed_rate",
	}
}

// GetSessionStatsColumnList returns the list of columns in the session_stats table
func GetSessionStatsColumnList() []string {
	return []string{
		"time_stamp",
		"session_id",
		"bytes",
		"byte_rate",
		"client_bytes",
		"client_byte_rate",
		"server_bytes",
		"server_byte_rate",
		"packets",
		"packet_rate",
		"client_packets",
		"client_packet_rate",
		"server_packets",
		"server_packet_rate",
	}
}

// GetInterfaceStatsInsertQuery generates the SQL for creating the prepared INSERT statment for the interface_stats table
func GetInterfaceStatsInsertQuery() string {
	colList := GetInterfaceStatsColumnList()
	sqlStr := "INSERT INTO interface_stats ("
	valStr := "("

	for x := 0; x < len(colList); x++ {
		if x != 0 {
			sqlStr += ","
			valStr += ","
		}
		sqlStr += colList[x]
		valStr += "?"
	}

	sqlStr += ")"
	valStr += ")"
	return (sqlStr + " VALUES " + valStr)
}

// GetSessionStatsInsertQuery generates the SQL for creating the prepared INSERT statement for the session_stats table
func GetSessionStatsInsertQuery() string {
	colList := GetSessionStatsColumnList()
	sqlStr := "INSERT INTO session_stats ("
	valStr := "("

	for x := 0; x < len(colList); x++ {
		if x != 0 {
			sqlStr += ","
			valStr += ","
		}
		sqlStr += colList[x]
		valStr += "?"
	}

	sqlStr += ")"
	valStr += ")"
	return (sqlStr + " VALUES " + valStr)
}

// createTables builds the reports.db tables and indexes
func createTables(dbConn *sql.DB) {
	var err error

	_, err = dbConn.Exec(
		`CREATE TABLE IF NOT EXISTS sessions (
			session_id int8 PRIMARY KEY NOT NULL,
			time_stamp bigint NOT NULL,
			end_time bigint,
			family int1,
			ip_protocol int,
			hostname text,
			username text,
			client_interface_id int default 0,
			server_interface_id int default 0,
			client_interface_type int1 default 0,
			server_interface_type int1 default 0,
			local_address  text,
			remote_address text,
			client_address text,
			server_address text,
			client_port int2,
			server_port int2,
			client_address_new text,
			server_address_new text,
			server_port_new int2,
			client_port_new int2,
			client_country text,
			client_latitude real,
			client_longitude real,
			server_country text,
			server_latitude real,
			server_longitude real,
			application_id text,
			application_name text,
			application_protochain text,
			application_category text,
			application_blocked boolean,
			application_flagged boolean,
			application_confidence integer,
			application_productivity integer,
			application_risk integer,
			application_detail text,
			application_id_inferred text,
			application_name_inferred text,
			application_confidence_inferred integer,
			application_protochain_inferred text,
			application_productivity_inferred integer,
			application_risk_inferred text,
			application_category_inferred text,
			certificate_subject_cn text,
			certificate_subject_o text,
			ssl_sni text,
			wan_rule_chain string,
			wan_rule_id integer,
			wan_policy_id integer,
			client_hops integer,
			server_hops integer,
			client_dns_hint text,
			server_dns_hint text)`)

	if err != nil {
		logger.Err("Failed to create table: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_sessions_time_stamp ON sessions (time_stamp DESC)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_sessions_id_time_stamp ON sessions (session_id, time_stamp DESC)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_sessions_wan_interface_time_stamp ON sessions (wan_rule_chain, server_interface_type, time_stamp DESC)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	// FIXME add domain (SNI + dns_prediction + cert_prediction)
	// We need a singular "domain" field that takes all the various domain determination methods into account and chooses the best one
	// I think the preference order is:
	//    ssl_sni (preferred because the client specified exactly the domain it is seeking)
	//    server_dns_hint (use a dns hint if no other method is known)
	//    certificate_subject_cn (preferred next as its specified by the server, but not exact, this same field is used by both certsniff and certfetch)

	// FIXME add domain_category
	// We need to add domain level categorization

	_, err = dbConn.Exec(
		`CREATE TABLE IF NOT EXISTS session_stats (
			session_id int8 NOT NULL,
			time_stamp bigint NOT NULL,
			bytes int8,
			client_bytes int8,
			server_bytes int8,
			byte_rate int8,
			client_byte_rate int8,
			server_byte_rate int8,
			packets int8,
			client_packets int8,
			server_packets int8,
			packet_rate int8,
			client_packet_rate int8,
			server_packet_rate int8)`)

	if err != nil {
		logger.Err("Failed to create table: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_session_stats_time_stamp ON session_stats (time_stamp DESC)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_session_stats_session_id_time_stamp ON session_stats (session_id, time_stamp DESC)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(
		`CREATE TABLE IF NOT EXISTS interface_stats (
			time_stamp bigint NOT NULL,
			interface_id int1,
			interface_name text,
			device_name text,
			is_wan boolean,
			latency_1 real,
			latency_5 real,
			latency_15 real,
			latency_variance real,
			passive_latency_1 real,
			passive_latency_5 real,
			passive_latency_15 real,
			passive_latency_variance real,
			active_latency_1 real,
			active_latency_5 real,
			active_latency_15 real,
			active_latency_variance real,
			jitter_1 real,
			jitter_5 real,
			jitter_15 real,
			jitter_variance real,
			ping_timeout int8,
			ping_timeout_rate int8,
			rx_bytes int8,
			rx_bytes_rate int8,
			rx_packets int8,
			rx_packets_rate int8,
			rx_errs int8,
			rx_errs_rate int8,
			rx_drop int8,
			rx_drop_rate int8,
			rx_fifo int8,
			rx_fifo_rate int8,
			rx_frame int8,
			rx_frame_rate int8,
			rx_compressed int8,
			rx_compressed_rate int8,
			rx_multicast int8,
			rx_multicast_rate int8,
			tx_bytes int8,
			tx_bytes_rate int8,
			tx_packets int8,
			tx_packets_rate int8,
			tx_errs int8,
			tx_errs_rate int8,
			tx_drop int8,
			tx_drop_rate int8,
			tx_fifo int8,
			tx_fifo_rate int8,
			tx_colls int8,
			tx_colls_rate int8,
			tx_carrier int8,
			tx_carrier_rate int8,
			tx_compressed,
			tx_compressed_rate int8)`)

	if err != nil {
		logger.Err("Failed to create table: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE TABLE IF NOT EXISTS threatprevention_stats (
		time_stamp bigint NOT NULL,
		blocked_address text,
		client_address text,
		threat_level int)`)

	if err != nil {
		logger.Err("Failed to create table: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_time_stamp ON interface_stats (time_stamp DESC)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_id_time_stamp ON interface_stats (interface_id, time_stamp DESC)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_id_ts_pt ON interface_stats (interface_id, time_stamp DESC, ping_timeout)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_id_ts_rb ON interface_stats (interface_id, time_stamp DESC, rx_bytes)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_id_ts_jit ON interface_stats (interface_id, time_stamp DESC, jitter_1)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_id_ts_lat ON interface_stats (interface_id, time_stamp DESC, latency_1)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_id_ts_al ON interface_stats (interface_id, time_stamp DESC, active_latency_1)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}

	_, err = dbConn.Exec(`CREATE INDEX IF NOT EXISTS idx_iface_stats_id_ts_pl ON interface_stats (interface_id, time_stamp DESC, passive_latency_1)`)
	if err != nil {
		logger.Err("Failed to create index: %s\n", err.Error())
	}
}

/**
 * tableCleaner monitors the size of the sqlite database. Once the database file grows to
 * the size limit, we begin deleting the oldest data to keep the file from growing too
 * large. Since deleting rows doesn't reduce the file size, we use the number of free
 * pages to decide when to trim the oldest data. We no longer perform a vacuum operation
 * since it is very expensive, and we don't believe it provides commensurate benefit in
 * this environment. The database is relatively small, most of the records are of similar
 * size, and it's typically stored in a memory-based filesystem, so we don't believe
 * fragmentation is a significant concern.
 THIS IS A ROUTINE
 @param dbConn (*sql.DB) - the db connection
 @param sizeLimit (int64) - the db size limit
**/
func tableCleaner(ctx context.Context, dbConn *sql.DB, sizeLimit int64) {
	var rtName = "table_cleaner"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)
	const minFreePageSpace int64 = 32768
	const megaByte = 1024 * 1024

	ch := make(chan bool, 1)
	ch <- true

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping table cleaner\n")
			return
		case <-ch:
		case <-time.After(60 * time.Second):
		}

		currentSize, pageSize, pageCount, maxPageCount, freeCount, err := loadDbStats(dbConn)

		if err != nil {
			logger.Crit("Unable to load DB Stats: %s\n", err.Error())
			monitor.RoutineError(rtName)
			continue
		}

		logger.Info("Database Size:%v MB  Limit:%v MB  Free Pages:%v Page Size: %v Page Count: %v Max Page Count: %v \n", currentSize/megaByte, sizeLimit/megaByte, freeCount, pageSize, pageCount, maxPageCount)

		// if we haven't reached the size limit just continue
		if currentSize < sizeLimit {
			continue
		}

		// if we haven't dropped below the minimum free page limit just continue
		if freeCount >= (minFreePageSpace / pageSize) {
			continue
		}

		// database is getting full so clean out some of the oldest data
		logger.Info("Database starting trim operation\n")

		tx, err := dbConn.Begin()
		if err != nil {
			logger.Warn("Failed to begin transaction: %s\n", err.Error())
			monitor.RoutineError(rtName)
			continue
		}

		err = trimPercent("sessions", .10, tx)
		if err != nil {
			logger.Warn("Failed to trim sessions: %s\n", err.Error())
			monitor.RoutineError(rtName)
			continue
		}

		err = trimPercent("session_stats", .10, tx)
		if err != nil {
			logger.Warn("Failed to trim session_stats: %s\n", err.Error())
			monitor.RoutineError(rtName)
			continue
		}

		err = trimPercent("interface_stats", .10, tx)
		if err != nil {
			logger.Warn("Failed to trim interface_stats: %s\n", err.Error())
			monitor.RoutineError(rtName)
			continue
		}

		err = trimPercent("threatprevention_stats", .10, tx)
		if err != nil {
			logger.Warn("Failed to trim threatprevention_stats: %s\n", err.Error())
			monitor.RoutineError(rtName)
			continue
		}

		logger.Info("Committing database trim...\n")

		// end transaction
		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			logger.Warn("Failed to commit transaction: %s\n", err.Error())
			monitor.RoutineError(rtName)
			continue

		}
		logger.Info("Database trim operation completed\n")

		//also run optimize
		runSQL(dbConn, "PRAGMA optimize")

		logger.Info("Database trim operation completed\n")

		currentSize, pageSize, pageCount, maxPageCount, freeCount, err = loadDbStats(dbConn)
		if err != nil {
			logger.Crit("Unable to load DB Stats POST TRIM: %s\n", err.Error())
			//monitor.RoutineError(rtName)
			continue
		}

		logger.Info("POST TRIM Database Size:%v MB  Limit:%v MB  Free Pages:%v Page Size: %v Page Count: %v Max Page Count: %v \n", currentSize/megaByte, sizeLimit/megaByte, freeCount, pageSize, pageCount, maxPageCount)
		// re-run and check size with no delay
		ch <- true

	}
}

// trimPercent trims the specified table by the specified percent (by time)
// example: trimPercent("sessions",.1) will drop the oldest 10% of events in sessions by time
func trimPercent(table string, percent float32, tx *sql.Tx) error {
	logger.Info("Trimming %s by %.1f%% percent...\n", table, percent*100.0)
	sqlStr := fmt.Sprintf("DELETE FROM %s WHERE time_stamp < (SELECT min(time_stamp)+cast((max(time_stamp)-min(time_stamp))*%f as int) from %s)", table, percent, table)
	logger.Debug("Trimming DB statement:\n %s \n", sqlStr)
	res, err := tx.Exec(sqlStr)
	if err != nil {
		logger.Warn("Failed to execute transaction: %s %s\n", err.Error(), sqlStr)
		return err
	}
	logger.Debug("Log trim result: %v\n", res)
	return nil
}

// loadDbStats gets the page size, page count, free list size, and current DB size from the database
// param dbConn (*sql.DB) - the DB connection
// returns currentSize (int64) - The DB Size in bytes
// returns pageSize (int64) - The current page size of each DB page
// returns pageCount (int64) - The current number of pages in the database
// returns maxPageCount (int64) - The maximum number of pages the database can hold
// returns freeCount (int64) - The number of free pages in the DB file
func loadDbStats(dbConn *sql.DB) (currentSize int64, pageSize int64, pageCount int64, maxPageCount int64, freeCount int64, err error) {
	// we get the page size, page count, and free list size for our limit calculations
	pageSize, err = strconv.ParseInt(runSQL(dbConn, "PRAGMA page_size"), 10, 64)
	if err != nil || pageSize == 0 {
		logger.Crit("Unable to parse database page_size: %v\n", err)
	}

	pageCount, err = strconv.ParseInt(runSQL(dbConn, "PRAGMA page_count"), 10, 64)
	if err != nil {
		logger.Crit("Unable to parse database page_count: %v\n", err)
	}

	maxPageCount, err = strconv.ParseInt(runSQL(dbConn, "PRAGMA max_page_count"), 10, 64)
	if err != nil {
		logger.Crit("Unable to parse database page_count: %v\n", err)
	}

	freeCount, err = strconv.ParseInt(runSQL(dbConn, "PRAGMA freelist_count"), 10, 64)
	if err != nil {
		logger.Crit("Unable to parse database freelist_count: %v\n", err)
	}

	currentSize = (pageSize * pageCount)

	return
}

// runSQL runs the specified SQL and returns the result which may be nothing
// mainly used for the PRAGMA commands used to get information about the database
func runSQL(dbConn *sql.DB, sqlStr string) string {
	var stmt *sql.Stmt
	var rows *sql.Rows
	var err error
	var result string = ""

	logger.Debug("SQL: %s\n", sqlStr)

	stmt, err = dbConn.Prepare(sqlStr)
	if err != nil {
		logger.Warn("Failed to Prepare statement: %s %s\n", err.Error(), sqlStr)
		return result
	}

	defer stmt.Close()

	rows, err = stmt.Query()
	if err != nil {
		logger.Warn("Failed to Query statement: %s %s\n", err.Error(), sqlStr)
		return result
	}

	defer rows.Close()

	// we only look at the first row returned
	if rows.Next() {
		rows.Scan(&result)
	}

	return result
}

// GetThreatpreventionStatsColumnList returns the list of columns in the threatprevention_stats table
func GetThreatPreventionStatsColumnList() []string {
	return []string{
		"time_stamp",
		"blocked_address",
		"client_address",
		"threat_level",
	}
}

// GetThreatPreventionStatsInsertQuery generates the SQL for creating the prepared INSERT statement for the threatprevention_stats table
func GetThreatPreventionStatsInsertQuery() string {
	colList := GetThreatPreventionStatsColumnList()
	sqlStr := "INSERT INTO threatprevention_stats ("
	valStr := "("

	for x := 0; x < len(colList); x++ {
		if x != 0 {
			sqlStr += ","
			valStr += ","
		}
		sqlStr += colList[x]
		valStr += "?"
	}

	sqlStr += ")"
	valStr += ")"
	return (sqlStr + " VALUES " + valStr)
}
