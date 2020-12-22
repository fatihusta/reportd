package localreporting

import (
	"context"

	"github.com/jsommerville-untangle/golang-shared/services/logger"
	pbe "github.com/jsommerville-untangle/golang-shared/structs/ProtoBuffEvent"
	"github.com/untangle/reportd/services/monitor"
)

var interfaceStatsChannel = make(chan *[]interface{}, 1000)
var sessionStatsChannel = make(chan *[]interface{}, 5000)
var sessionsChannel = make(chan *pbe.ProtoBuffEvent, 10000)
var contextRelation = monitor.RoutineContextGroup{}

// Startup is used to startup the localreporting service
func Startup() {

	createReceiverChannels()
	var relatedRoutines = []string{"table_cleaner", "interface_stats_processor", "session_stats_processor", "session_processor"}
	contextRelation = monitor.CreateRoutineContextRelation(context.Background(), "localreporting", relatedRoutines)

	go setupDatabase(contextRelation)

}

// Shutdown is used to shutdown the reports service
func Shutdown() {
	logger.Info("Shutting down local reporting service...\n")
	monitor.CancelContexts(contextRelation)
}

// createReceiverChannels builds (or rebuilds) the channels used for receiving and writing event data
func createReceiverChannels() {
	interfaceStatsChannel = make(chan *[]interface{}, 1000)
	sessionStatsChannel = make(chan *[]interface{}, 5000)
	sessionsChannel = make(chan *pbe.ProtoBuffEvent, 10000)
}

// AddToSessionChannel will add the item pointer into the sessions channel
func AddToSessionChannel(item *pbe.ProtoBuffEvent) {
	sessionsChannel <- item
}
