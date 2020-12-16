package monitor

import (
	"os"

	"github.com/jsommerville-untangle/golang-shared/services/logger"
)

var routineWatcher = make(chan int)

// Startup is called to startup the monitor service
func Startup() {
	routineWatcher = make(chan int)
}

// StartRoutine is used when initializing a new goroutine and adding monitoring to that routine
func StartRoutine() {
	routineWatcher <- +1
}

// StartFinishRoutineThread is a function to simplify how we can defer calling finishRoutine() at the top of a function,
// instead of having to always call it at the end of a routine
func StartFinishRoutineThread() {
	go finishRoutine()
}

// finishRoutine is called at the end of a running go routine to empty the channel watcher
func finishRoutine() {
	routineWatcher <- -1
}

// Shutdown is called to close all running routines
func Shutdown() {
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
