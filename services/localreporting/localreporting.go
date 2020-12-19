package localreporting

import (
	"github.com/jsommerville-untangle/golang-shared/services/logger"
	pbe "github.com/jsommerville-untangle/golang-shared/structs/ProtoBuffEvent"
)

// Channel to signal all report go routines to stop
var serviceShutdown = make(chan bool, 1)
var interfaceStatsChannel = make(chan *[]interface{}, 1000)
var sessionStatsChannel = make(chan *[]interface{}, 5000)
var sessionsChannel = make(chan *pbe.ProtoBuffEvent, 10000)

// Startup is used to startup the localreporting service
func Startup() {

	createReceiverChannels()

	go setupDatabase()

}

// Shutdown is used to shutdown the reports service
func Shutdown() {
	logger.Info("Sending service shutdown channel to report routines...")
	serviceShutdown <- true

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
