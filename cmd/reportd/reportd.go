package main

import (
	"os"
	"time"

	"github.com/untangle/packetd/services/logger"
)

var routineWatcher = make(chan int)

// main is the entrypoint of reportd
func main() {
	logger.Info("Starting up reportd...\n")

	go eventListener()

	go eventLogger()

	logger.Info("Waiting for all routines to finish...\n")

	numGoroutines := 0
	for diff := range routineWatcher {
		numGoroutines += diff
		logger.Info("Running routines: %v\n", numGoroutines)
		if numGoroutines == 0 {
			logger.Info("Shutting down reportd...\n")
			os.Exit(0)
		}
	}
}

// eventListener is used to listen for ZMQ events being published
func eventListener() {
	routineWatcher <- +1
	logger.Info("Starting up a theoretical goroutine for event listening...\n")
	time.Sleep(30 * time.Second)
	go finishRoutine()
}

// eventLogger is used to log events to the database
func eventLogger() {
	routineWatcher <- +1

	logger.Info("Starting up a theoretical goroutine for event logging...\n")
	time.Sleep(30 * time.Second)
	go finishRoutine()
}

// finishRoutine is called at the end of a running go routine to empty the channel watcher
func finishRoutine() {
	routineWatcher <- -1
}
