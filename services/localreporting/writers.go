package localreporting

import (
	"database/sql"
	"net"
	"time"

	"github.com/jsommerville-untangle/golang-shared/services/logger"
	pbe "github.com/jsommerville-untangle/golang-shared/structs/ProtoBuffEvent"
	"github.com/untangle/reportd/services/monitor"
	spb "google.golang.org/protobuf/types/known/structpb"
)

// queueWriter reads a queue channel and writes data using the sql statement pointer passed in
func queueWriter(queueType string, stmt *sql.Stmt, queue chan *[]interface{}) {
	var rtName string = queueType + "_queue_routine"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
		case <-serviceShutdown:
			logger.Info("Stopping queue writer %s\n", rtName)
			return
		case queueItem := <-queue:
			if logger.IsTraceEnabled() {
				logger.Trace("%s: %v\n", queueType, queueItem)
			}

			stmt.Exec(*queueItem...)
		}
	}
}

// sessWriter reads the Session event queue and writes the appropriate data
func sessWriter(dbConn *sql.DB) {
	var sessionBatch []pbe.ProtoBuffEvent
	var lastInsert time.Time
	var retryBatch = make(chan bool, 1)
	const eventLoggerInterval = 10 * time.Second
	const eventBatchSize = 1000
	const waitTime = 60.0
	var rtName string = "session_write_queue_routine"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
		case <-serviceShutdown:
			logger.Info("Shutting down session writer\n")
			return
		// read data out of the eventQueue into the eventBatch
		case sess := <-sessionsChannel:
			sessionBatch = append(sessionBatch, *sess)
			retry := false

			// when the batch is larger than the configured batch insert size OR we haven't inserted anything in one minute, we need to insert some stuff
			batchCount := len(sessionBatch)
			if batchCount >= eventBatchSize || time.Since(lastInsert).Seconds() > waitTime {
				logger.Info("Starting a batch...\n")
				sessionBatch, lastInsert, retry = batchTransaction(dbConn, sessionBatch, batchCount)
				if retry {
					retryBatch <- true
				}
			}
		// If the channel hasn't had any data in eventLoggerInterval, commit any remaining batch items to DB
		case <-time.After(eventLoggerInterval):
			logger.Debug("No events seen for eventLogger\n")
			retry := false

			if sessionBatch != nil {
				logger.Info("Starting a batch with some remaining stuff..\n")

				batchCount := len(sessionBatch)
				sessionBatch, lastInsert, retry = batchTransaction(dbConn, sessionBatch, batchCount)
				if retry {
					retryBatch <- true
				}
			}
		case <-retryBatch:
			logger.Info("Retrying a batch...")

			retry := false
			batchCount := len(sessionBatch)
			sessionBatch, lastInsert, retry = batchTransaction(dbConn, sessionBatch, batchCount)
			if retry {
				retryBatch <- true
			}
		}
	}

}

// batchTransaction will accept a batch and complete the transaction to the DB
// param eventBatch ([]Event) - events to commit to DB
// param batchCount (int) - numbers of events being commited to DB
// return ([]Event, time.Time) - return a nil eventBatch and the current time
func batchTransaction(dbConn *sql.DB, eventBatch []pbe.ProtoBuffEvent, batchCount int) ([]pbe.ProtoBuffEvent, time.Time, bool) {
	logger.Debug("%v Items ready for batch, starting transaction at %v...\n", batchCount, time.Now())

	tx, err := dbConn.Begin()
	if err != nil {
		logger.Warn("Failed to begin transaction: %s\n", err.Error())
		return eventBatch, time.Now(), true

	}

	//iterate events in the batch and send them into the db transaction
	for _, event := range eventBatch {
		err = eventToTransaction(event, tx)
		if err != nil {
			tx.Rollback()
			logger.Warn("Unable to convert an event in this batch to transaction: %s\n", err)
			return eventBatch, time.Now(), true
		}
	}

	// end transaction
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		logger.Warn("Failed to commit transaction: %s\n", err.Error())
		return eventBatch, time.Now(), true

	}

	logger.Debug("Transaction completed, %v items processed at %v .\n", batchCount, time.Now())

	return nil, time.Now(), false
}

// eventToTransaction converts the ProtoBuffEvent object into a Sql Transaction and appends it into the current transaction context
// param event (ProtoBuffEvent) - the protocol buffer event to process
// param tx (*sql.Tx) - the transaction context
func eventToTransaction(event pbe.ProtoBuffEvent, tx *sql.Tx) error {
	var sqlStr string
	var values []interface{}
	var first = true

	// sqlOP 1 is an INSERT
	if event.SQLOp == 1 {
		sqlStr = "INSERT INTO " + event.Table + "("
		var valueStr = "("
		for k, v := range DecodeToMap(event.GetColumns()) {
			if !first {
				sqlStr += ","
				valueStr += ","
			}
			sqlStr += k
			valueStr += "?"
			first = false
			values = append(values, prepareEventValues(v))
		}
		sqlStr += ")"
		valueStr += ")"
		sqlStr += " VALUES " + valueStr
	}

	// sqlOP 2 is an UPDATE
	if event.SQLOp == 2 {
		sqlStr = "UPDATE " + event.Table + " SET"
		for k, v := range DecodeToMap(event.GetModifiedColumns()) {
			if !first {
				sqlStr += ","
			}

			sqlStr += " " + k + " = ?"
			values = append(values, prepareEventValues(v))
			first = false
		}

		sqlStr += " WHERE "
		first = true
		for k, v := range DecodeToMap(event.GetColumns()) {
			if !first {
				sqlStr += " AND "
			}

			sqlStr += " " + k + " = ?"
			values = append(values, prepareEventValues(v))
			first = false
		}
	}

	res, err := tx.Exec(sqlStr, values...)
	if err != nil {
		logger.Warn("Failed to execute transaction: %s %s\n", err.Error(), sqlStr)
		return err
	}

	rowCount, _ := res.RowsAffected()
	logger.Debug("SQL:%s ROWS:%d\n", sqlStr, rowCount)
	return nil
}

// decodeValue is used to decode a protocol buffer as needed
// These helpers are referenced from:
// https://gist.github.com/unstppbl/31c86d97332798c22d26c1c67f3c6ca1#file-struct-go
//
func decodeValue(v *spb.Value) interface{} {
	switch k := v.Kind.(type) {
	case *spb.Value_NullValue:
		return nil
	case *spb.Value_NumberValue:
		return k.NumberValue
	case *spb.Value_StringValue:
		return k.StringValue
	case *spb.Value_BoolValue:
		return k.BoolValue
	case *spb.Value_StructValue:
		return DecodeToMap(k.StructValue)
	case *spb.Value_ListValue:
		s := make([]interface{}, len(k.ListValue.Values))
		for i, e := range k.ListValue.Values {
			s[i] = decodeValue(e)
		}
		return s
	default:
		logger.Warn("Unknown type for this buffer, returning null.")
		return v.GetNullValue()
	}
}

// DecodeToMap converts a pb.Struct to a map from strings to Go types.
// DecodeToMap panics if s is invalid.
func DecodeToMap(s *spb.Struct) map[string]interface{} {
	if s == nil {
		return nil
	}
	m := map[string]interface{}{}
	for k, v := range s.Fields {
		m[k] = decodeValue(v)
	}
	return m
}

// prepareEventValues prepares data that should be modified when being inserted into SQLite
func prepareEventValues(data interface{}) interface{} {
	switch data.(type) {
	//IP Addresses should be converted to string types before stored in database
	case net.IP:
		return data.(net.IP).String()

	// Special handle time.Time
	// We want to log these as milliseconds since epoch
	case time.Time:
		return data.(time.Time).UnixNano() / 1e6
	default:
		return data
	}
}
