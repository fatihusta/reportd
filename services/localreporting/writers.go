package localreporting

import (
	"context"
	"database/sql"
	"net"
	"time"

	"github.com/untangle/golang-shared/services/logger"
	pbe "github.com/untangle/golang-shared/structs/protocolbuffers/SessionEvent"
	"github.com/untangle/reportd/services/monitor"
	spb "google.golang.org/protobuf/types/known/structpb"
)

// intfStatsWriter reads the interfacestatschannel and writes entries to the database using the prepared sql statement
func intfStatsWriter(ctx context.Context, stmt *sql.Stmt) {
	rtName := "interface_stats_processor"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
		case intfStats := <-interfaceStatsChannel:

			_, err := stmt.Exec(
				intfStats.TimeStamp,
				intfStats.InterfaceID,
				intfStats.InterfaceName,
				intfStats.DeviceName,
				intfStats.IsWan,
				intfStats.Latency1,
				intfStats.Latency5,
				intfStats.Latency15,
				intfStats.LatencyVariance,
				intfStats.PassiveLatency1,
				intfStats.PassiveLatency5,
				intfStats.PassiveLatency15,
				intfStats.PassiveLatencyVariance,
				intfStats.ActiveLatency1,
				intfStats.ActiveLatency5,
				intfStats.ActiveLatency15,
				intfStats.ActiveLatencyVariance,
				intfStats.Jitter1,
				intfStats.Jitter5,
				intfStats.Jitter15,
				intfStats.JitterVariance,
				intfStats.PingTimeout,
				intfStats.PingTimeoutRate,
				intfStats.RxBytes,
				intfStats.RxBytesRate,
				intfStats.RxPackets,
				intfStats.RxPacketsRate,
				intfStats.RxErrs,
				intfStats.RxErrsRate,
				intfStats.RxDrop,
				intfStats.RxDropRate,
				intfStats.RxFifo,
				intfStats.RxFifoRate,
				intfStats.RxFrame,
				intfStats.RxFrameRate,
				intfStats.RxCompressed,
				intfStats.RxCompressedRate,
				intfStats.RxMulticast,
				intfStats.RxMulticastRate,
				intfStats.TxBytes,
				intfStats.TxBytesRate,
				intfStats.TxPackets,
				intfStats.TxPacketsRate,
				intfStats.TxErrs,
				intfStats.TxErrsRate,
				intfStats.TxDrop,
				intfStats.TxDropRate,
				intfStats.TxFifo,
				intfStats.TxFifoRate,
				intfStats.TxColls,
				intfStats.TxCollsRate,
				intfStats.TxCarrier,
				intfStats.TxCarrierRate,
				intfStats.TxCompressed,
				intfStats.TxCompressedRate,
			)
			if err != nil {
				logger.Err("Error while executing %s insert: %s\n", rtName, err)
				continue
			}
		case <-ctx.Done():
			logger.Info("Shutting down %s\n", rtName)
			return
		}
	}
}

// sessStatsWriter reads the interfacestatschannel and writes entries to the database using the prepared sql statement
func sessStatsWriter(ctx context.Context, stmt *sql.Stmt) {
	rtName := "session_stats_processor"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
		case sessStats := <-sessionStatsChannel:

			_, err := stmt.Exec(
				sessStats.TimeStamp,
				sessStats.SessionID,
				sessStats.Bytes,
				sessStats.ByteRate,
				sessStats.ClientBytes,
				sessStats.ClientByteRate,
				sessStats.ServerBytes,
				sessStats.ServerByteRate,
				sessStats.Packets,
				sessStats.PacketRate,
				sessStats.ClientPackets,
				sessStats.ClientPacketRate,
				sessStats.ServerPackets,
				sessStats.ServerPacketRate,
			)
			if err != nil {
				logger.Err("Error while executing %s insert: %s\n", rtName, err)
				continue
			}
		case <-ctx.Done():
			logger.Info("Shutting down %s\n", rtName)
			return
		}
	}
}

func threatPreventionStatsWriter(ctx context.Context, stmt *sql.Stmt) {
	rtName := "threat_prevention_stats_processor"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
		case threatPreventionStats := <-threatPreventionStatsChannel:
			if logger.IsTraceEnabled() {
				logger.Trace("THREATPREVENTION_STATS: %v\n", threatPreventionStats)
			}
			_, err := stmt.Exec(
				threatPreventionStats.TimeStamp,
				threatPreventionStats.BlockedAddress,
				threatPreventionStats.ClientAddress,
				threatPreventionStats.ThreatLevel,
			)
			if err != nil {
				logger.Err("Error while executing %s insert: %s\n", rtName, err)
				continue
			}
		case <-ctx.Done():
			logger.Info("Shutting down %s\n", rtName)
			return
		}
	}
}

// sessWriter reads the Session event queue and writes the appropriate data
func sessWriter(ctx context.Context, dbConn *sql.DB) {
	var sessionBatch []pbe.SessionEvent
	var lastInsert time.Time
	var retryBatch = make(chan bool, 1)
	const eventLoggerInterval = 10 * time.Second
	const eventBatchSize = 1000
	const waitTime = 60.0
	var rtName string = "session_processor"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
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
		case <-ctx.Done():
			logger.Info("Stopping queue writer: %s\n", rtName)
			return
		}
	}

}

// batchTransaction will accept a batch and complete the transaction to the DB
// param eventBatch ([]Event) - events to commit to DB
// param batchCount (int) - numbers of events being commited to DB
// return ([]Event, time.Time) - return a nil eventBatch and the current time
func batchTransaction(dbConn *sql.DB, eventBatch []pbe.SessionEvent, batchCount int) ([]pbe.SessionEvent, time.Time, bool) {

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

// eventToTransaction converts the SessionEvent object into a Sql Transaction and appends it into the current transaction context
// param event (SessionEvent) - the protocol buffer event to process
// param tx (*sql.Tx) - the transaction context
func eventToTransaction(event pbe.SessionEvent, tx *sql.Tx) error {
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
