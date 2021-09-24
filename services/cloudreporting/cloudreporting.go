package cloudreporting

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/untangle/golang-shared/services/logger"
	"github.com/untangle/golang-shared/services/settings"
	ise "github.com/untangle/golang-shared/structs/protocolbuffers/InterfaceStatsEvent"
	"github.com/untangle/reportd/services/monitor"
)

var cloudIntfStatsChannel = make(chan *ise.InterfaceStatsEvent, 1000)
var cloudReportingRelation = monitor.RoutineContextGroup{}
var uid = "00000000-0000-0000-0000-000000000000"

// CloudEvent is a struct formatted for what the cloud side message queues are expecting
type CloudEvent struct {
	// Name - A human readable name for this event. (ie "session_new" is a new session event)
	Name string `json:"name"`
	// Table - the DB table that this event modifies (or nil)
	Table string `json:"table"`
	// SQLOp - the SQL operation needed to serialize the event to the DB
	// 1 - INSERT // 2 - UPDATE
	SQLOp int `json:"sqlOp"`
	// The columns in the DB this inserts for INSERTS or qualifies if matches for UPDATES
	Columns map[string]interface{} `json:"columns"`
	// The columns to modify for UPDATE events
	ModifiedColumns map[string]interface{} `json:"modifiedColumns"`
}

// Startup is called to build anthing related to this package
func Startup() {
	cloudIntfStatsChannel = make(chan *ise.InterfaceStatsEvent, 1000)
	cloudReportingRelation = monitor.CreateRoutineContextRelation(context.Background(), "cloudreporting", []string{"interface_stats_sender"})
	uid, err := settings.GetUID()
	if err != nil {
		logger.Warn("Unable to read UID: %s - Using all zeros\n", err.Error())
	}

	go intfStatsSender(cloudReportingRelation.Contexts["interface_stats_sender"], uid)

}

// Shutdown is called to tear down go routines and remove anything related to this package
func Shutdown() {
	logger.Info("Shutting down cloudreporting service...\n")
	monitor.CancelContexts(cloudReportingRelation)
}

// AddToInterfaceStatsChannel will add the item pointer to the interface stats channel
func AddToInterfaceStatsChannel(item *ise.InterfaceStatsEvent) {
	cloudIntfStatsChannel <- item
}

func buildMfwEventsClient(uid string) (*http.Client, string) {
	transport := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
	client := &http.Client{Transport: transport, Timeout: time.Duration(5 * time.Second)}
	target := fmt.Sprintf("https://database.untangle.com/v1/put?source=%s&type=db&queueName=mfw_events", uid)

	return client, target
}

func intfStatsSender(ctx context.Context, uid string) {
	rtName := "interface_stats_sender"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	client, target := buildMfwEventsClient(uid)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping %s routine\n", rtName)
			return
		case evt := <-cloudIntfStatsChannel:

			// we need to parse this into the old event format for the cloud
			cloudEvent := formatForCloud(evt)

			message, err := json.Marshal(cloudEvent)
			if err != nil {
				logger.Warn("Error calling json.Marshal: %s\n", err.Error())
				continue
			}

			request, err := http.NewRequest("POST", target, bytes.NewBuffer(message))
			if err != nil {
				logger.Warn("Error calling http.NewRequest: %s\n", err.Error())
				continue
			}

			request.Header.Set("AuthRequest", "93BE7735-E9F2-487A-9DD4-9D05B95640F5")

			response, err := client.Do(request)
			if err != nil {
				logger.Warn("Error calling client.Do: %s\n", err.Error())
				continue
			}

			_, err = ioutil.ReadAll(response.Body)
			response.Body.Close()

			if err != nil {
				logger.Warn("Error calling ioutil.ReadAll: %s\n", err.Error())
			}

			if logger.IsDebugEnabled() {
				logger.Debug("CloudURL:%s CloudRequest:%s CloudResponse: [%d] %s %s\n", target, string(message), response.StatusCode, response.Proto, response.Status)
			}
		}
	}
}

// formatForCloud formats the intfStatEvt as a CloudEvent for the cloud events API
// TODO: maybe we can switch the cloud consumer to accept both formats? (structured version and unstructured)
//
func formatForCloud(intfStats *ise.InterfaceStatsEvent) *CloudEvent {

	cols := make(map[string]interface{})

	cols["time_stamp"] = intfStats.TimeStamp
	cols["interface_id"] = intfStats.InterfaceID
	cols["interface_name"] = intfStats.InterfaceName
	cols["device_name"] = intfStats.DeviceName
	cols["is_wan"] = intfStats.IsWan
	cols["latency_1"] = intfStats.Latency1
	cols["latency_5"] = intfStats.Latency5
	cols["latency_15"] = intfStats.Latency15
	cols["latency_variance"] = intfStats.LatencyVariance
	cols["passive_latency_1"] = intfStats.PassiveLatency1
	cols["passive_latency_5"] = intfStats.PassiveLatency5
	cols["passive_latency_15"] = intfStats.PassiveLatency15
	cols["passive_latency_variance"] = intfStats.PassiveLatencyVariance
	cols["active_latency_1"] = intfStats.ActiveLatency1
	cols["active_latency_5"] = intfStats.ActiveLatency5
	cols["active_latency_15"] = intfStats.ActiveLatency15
	cols["active_latency_variance"] = intfStats.ActiveLatencyVariance
	cols["jitter_1"] = intfStats.Jitter1
	cols["jitter_5"] = intfStats.Jitter5
	cols["jitter_15"] = intfStats.Jitter15
	cols["jitter_variance"] = intfStats.JitterVariance
	cols["ping_timeout"] = intfStats.PingTimeout
	cols["ping_timeout_rate"] = intfStats.PingTimeoutRate
	cols["rx_bytes"] = intfStats.RxBytes
	cols["rx_bytes_rate"] = intfStats.RxBytesRate
	cols["rx_packets"] = intfStats.RxPackets
	cols["rx_packets_rate"] = intfStats.RxPacketsRate
	cols["rx_errs"] = intfStats.RxErrs
	cols["rx_errs_rate"] = intfStats.RxErrsRate
	cols["rx_drop"] = intfStats.RxDrop
	cols["rx_drop_rate"] = intfStats.RxDropRate
	cols["rx_fifo"] = intfStats.RxFifo
	cols["rx_fifo_rate"] = intfStats.RxFifoRate
	cols["rx_frame"] = intfStats.RxFrame
	cols["rx_frame_rate"] = intfStats.RxFrameRate
	cols["rx_compressed"] = intfStats.RxCompressed
	cols["rx_compressed_rate"] = intfStats.RxCompressedRate
	cols["rx_multicast"] = intfStats.RxMulticast
	cols["rx_multicast_rate"] = intfStats.RxMulticastRate
	cols["tx_bytes"] = intfStats.TxBytes
	cols["tx_bytes_rate"] = intfStats.TxBytesRate
	cols["tx_packets"] = intfStats.TxPackets
	cols["tx_packets_rate"] = intfStats.TxPacketsRate
	cols["tx_errs"] = intfStats.TxErrs
	cols["tx_errs_rate"] = intfStats.TxErrsRate
	cols["tx_drop"] = intfStats.TxDrop
	cols["tx_drop_rate"] = intfStats.TxDropRate
	cols["tx_fifo"] = intfStats.TxFifo
	cols["tx_fifo_rate"] = intfStats.TxFifoRate
	cols["tx_colls"] = intfStats.TxColls
	cols["tx_colls_rate"] = intfStats.TxCollsRate
	cols["tx_carrier"] = intfStats.TxCarrier
	cols["tx_carrier_rate"] = intfStats.TxCarrierRate
	cols["tx_compressed"] = intfStats.TxCompressed
	cols["tx_compressed_rate"] = intfStats.TxCompressedRate

	returnEvt := &CloudEvent{Name: "interface_stats", Table: "interface_stats", SQLOp: 1, Columns: cols, ModifiedColumns: nil}

	return returnEvt
}
