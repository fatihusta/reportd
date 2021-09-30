package localreporting

import (
	"context"

	"github.com/untangle/golang-shared/services/logger"
	ise "github.com/untangle/golang-shared/structs/protocolbuffers/InterfaceStatsEvent"
	se "github.com/untangle/golang-shared/structs/protocolbuffers/SessionEvent"
	sse "github.com/untangle/golang-shared/structs/protocolbuffers/SessionStatsEvent"
	tpse "github.com/untangle/golang-shared/structs/protocolbuffers/ThreatPreventionStatsEvent"
	"github.com/untangle/reportd/services/monitor"
)

var interfaceStatsChannel = make(chan *ise.InterfaceStatsEvent, 1000)
var sessionStatsChannel = make(chan *sse.SessionStatsEvent, 5000)
var sessionsChannel = make(chan *se.SessionEvent, 10000)
var threatPreventionStatsChannel = make(chan *tpse.ThreatPreventionStatsEvent, 1000)
var contextRelation = monitor.RoutineContextGroup{}

// Startup is used to startup the localreporting service
func Startup() {

	createReceiverChannels()
	var relatedRoutines = []string{"table_cleaner", "interface_stats_processor", "session_stats_processor", "session_processor", "threat_prevention_stats_processor"}
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
	interfaceStatsChannel = make(chan *ise.InterfaceStatsEvent, 1000)
	sessionStatsChannel = make(chan *sse.SessionStatsEvent, 5000)
	sessionsChannel = make(chan *se.SessionEvent, 10000)
	threatPreventionStatsChannel = make(chan *tpse.ThreatPreventionStatsEvent, 1000)
}

// AddToSessionChannel will add the item pointer into the sessions channel
func AddToSessionChannel(item *se.SessionEvent) {
	sessionsChannel <- item
}

// AddToSessionStatsChannel will add the item pointer to the session stats channel
func AddToSessionStatsChannel(item *sse.SessionStatsEvent) {
	sessionStatsChannel <- item
}

// AddToInterfaceStatsChannel will add the item pointer to the interface stats channel
func AddToInterfaceStatsChannel(item *ise.InterfaceStatsEvent) {
	interfaceStatsChannel <- item
}

// AddToThreatPreventionStatsChannel will add the item pointer to the threat prevention stats channel
func AddToThreatPreventionStatsChannel(item *tpse.ThreatPreventionStatsEvent) {
	threatPreventionStatsChannel <- item
}
