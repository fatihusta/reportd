package monitor

import (
	"context"
	"sync"
	"time"

	"github.com/untangle/golang-shared/services/logger"
)

var routineInfoWatcher = make(chan *routineInfo)
var activeRoutines = make(map[string]bool)
var activeRoutinesMutex = &sync.RWMutex{}
var monitorRelation = RoutineContextGroup{}

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

// RoutineContextGroup is a collection of contexts and cancels that are associated
// with one specific routine group (ie: all local reporting contexts)
// This can be used with CancelContexts to send a ctx.Done() to all routines
// within a specific group
type RoutineContextGroup struct {
	Name     string
	Contexts map[string]context.Context
	Cancels  map[string]context.CancelFunc
}

// Startup is called to startup the monitor service
func Startup() {
	logger.Info("Starting routine monitor service...\n")
	routineInfoWatcher = make(chan *routineInfo)
	activeRoutines = make(map[string]bool)
	monitorRelation = CreateRoutineContextRelation(context.Background(), "monitor", []string{"routineInfoWatcher"})

	go monitorRoutineEvents(monitorRelation.Contexts["routineInfoWatcher"])

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

// CreateRoutineContextRelation will create a collection of routine contexts and cancels that are related to routineNames passed as input
// This object is used to store context relations among many contexts, allowing for closing all contexts at once if needed
func CreateRoutineContextRelation(ctx context.Context, name string, routineNames []string) RoutineContextGroup {
	returnCtxs := make(map[string]context.Context)
	returnCancels := make(map[string]context.CancelFunc)

	for _, rtName := range routineNames {
		var thisCtx, thisCleaner = context.WithCancel(ctx)

		returnCtxs[rtName] = thisCtx
		returnCancels[rtName] = thisCleaner

	}

	return RoutineContextGroup{Name: name, Contexts: returnCtxs, Cancels: returnCancels}
}

// CancelContexts is a routine that will wait for a cancel call at the top level context
// and then deliver a cancel call to all downstream contexts passed in the cancels param
func CancelContexts(relation RoutineContextGroup) {
	var wg sync.WaitGroup

	for _, f := range relation.Cancels {
		wg.Add(1)
		go func(f func(), wgs *sync.WaitGroup) {
			defer wgs.Done()
			f()
		}(f, &wg)
	}

	wg.Wait()
	logger.Info("All routines associated with %s should be cancelled...\n", relation.Name)

}

// Shutdown is called to close the routine monitor
func Shutdown() {
	// Set shutdown channel for this service
	logger.Info("Shutting down monitor service...\n")
	CancelContexts(monitorRelation)
}

// montitorRoutineEvents is a routine that monitors the routineInfoWatcher queue for any routine events to act on
// THIS IS A ROUTINE FUNCTION
func monitorRoutineEvents(ctx context.Context) {

	// Read the routineInfoWatcher channel for any Error types
	for {
		select {
		case rtEvt := <-routineInfoWatcher:
			if rtEvt.Action == err {
				logger.Info("Acting on this event: %v\n", rtEvt)
				handleRoutineWatcherEvents()
			}
		case <-time.Tick(60 * time.Second):
			logger.Info("There are %v monitored routines.\n", len(activeRoutines))
		case <-ctx.Done():
			logger.Info("Stopping routine monitor\n")
			return
		}
	}
}

// handleRoutineWatcherEvents is used to signal specific patterns to other routines
// IE: if we see a specific go routine ending with error, rebuild that routine pattern
func handleRoutineWatcherEvents() {
	logger.Info("Taking action on this event")
}
