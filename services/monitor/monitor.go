package monitor

import (
	"sync"
	"time"

	"github.com/jsommerville-untangle/golang-shared/services/logger"
)

var routineInfoWatcher = make(chan *routineInfo)
var serviceShutdown = make(chan bool, 1)
var activeRoutines = make(map[string]bool)
var activeRoutinesMutex = &sync.RWMutex{}

// routineInfo is a struct used to signal routine events
type routineInfo struct {
	Name   string
	Action routineAction
}

// routineAction is a constant that represents specific routine actions that occur
type routineAction int

const (
	start routineAction = iota
	err
	end
)

// Startup is called to startup the monitor service
func Startup() {
	logger.Info("Starting routine monitor service...\n")
	routineInfoWatcher = make(chan *routineInfo)
	activeRoutines = make(map[string]bool)

	go monitorRoutineEvents()

}

// RoutineStarted is used when initializing a new goroutine and adding monitoring to that routine
func RoutineStarted(routineName string) {
	logger.Info("Start Routine called: %s \n", routineName)
	activeRoutinesMutex.Lock()
	//routineInfoWatcher <- &routineInfo{Name: routineName, Action: start}
	defer activeRoutinesMutex.Unlock()
	activeRoutines[routineName] = true
}

// RoutineEnd is a function to simplify how we can defer calling finishRoutine() at the top of a function,
// instead of having to always call it at the end of a routine
func RoutineEnd(routineName string) {
	logger.Info("Finish Routine called: %s \n", routineName)
	activeRoutinesMutex.Lock()
	//routineInfoWatcher <- &routineInfo{Name: routineName, Action: end}
	defer activeRoutinesMutex.Unlock()
	_, ok := activeRoutines[routineName]
	if ok {
		delete(activeRoutines, routineName)
	}

}

// RoutineError signals a routine error to the routineInfoWatcher channel
func RoutineError(routineName string) {
	logger.Info("Error Routine called: %s \n", routineName)
	activeRoutinesMutex.Lock()
	//routineInfoWatcher <- &routineInfo{Name: routineName, Action: err}
	defer activeRoutinesMutex.Unlock()
	_, ok := activeRoutines[routineName]
	if ok {
		delete(activeRoutines, routineName)
	}
}

// Shutdown is called to close the routine monitor
func Shutdown() {
	// Set shutdown channel for this service
	logger.Info("Sending service shutdown channel to monitor routines...")

	serviceShutdown <- true

}

// montitorRoutineEvents is a routine that monitors the routineInfoWatcher queue for any routine events to act on
// THIS IS A ROUTINE FUNCTION
func monitorRoutineEvents() {

	// Read the routineInfoWatcher channel for any Error types
	select {
	case <-serviceShutdown:
		logger.Info("Stopping routine monitor\n")
		return
	case rtEvt := <-routineInfoWatcher:
		if rtEvt.Action == err {
			logger.Info("Acting on this event: %v\n", rtEvt)
			handleRoutineWatcherEvents()
		}
	case <-time.Tick(60 * time.Second):
		logger.Info("There are %v monitored routines.\n", len(activeRoutines))
	}

}

// handleRoutineWatcherEvents is used to signal specific patterns to other routines
// IE: if we see a specific go routine ending with error, rebuild that routine pattern
func handleRoutineWatcherEvents() {
	logger.Info("Taking action on this event")
}
