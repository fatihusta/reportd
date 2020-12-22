package main

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/untangle/golang-shared/services/logger"
	"github.com/untangle/reportd/services/localreporting"
	"github.com/untangle/reportd/services/messenger"
	"github.com/untangle/reportd/services/monitor"
)

var shutdownFlag uint32
var shutdownChannel = make(chan bool)

// main is the entrypoint of reportd
func main() {
	logger.Startup()
	logger.Info("Starting up reportd...\n")

	startServices()

	handleSignals()

	// Loop unless we get a shutdown flag or the shutdown channel is signaled
	for !GetShutdownFlag() {
		select {
		case <-shutdownChannel:
			logger.Info("Shutdown channel initiated... %v\n", GetShutdownFlag())
			break
		case <-time.After(1 * time.Minute):
			logger.Info("\n")
			printStats()
		}
	}

	logger.Info("Shutting down reportd...\n")

	stopServices()

	logger.Info("Reportd services done shutting down.\n")

}

func startServices() {
	monitor.Startup()
	localreporting.Startup()
	messenger.Startup()

}

func stopServices() {
	var wg sync.WaitGroup

	shutdowns := []func(){
		messenger.Shutdown,
		localreporting.Shutdown,
		monitor.Shutdown,
		logger.Shutdown,
	}

	for _, f := range shutdowns {
		wg.Add(1)
		go func(f func(), wgs *sync.WaitGroup) {
			defer wgs.Done()
			f()
		}(f, &wg)
	}

	wg.Wait()
}

// Add signal handlers
func handleSignals() {
	// Add SIGINT & SIGTERM handler (exit)
	termch := make(chan os.Signal, 1)
	signal.Notify(termch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-termch
		go func() {
			logger.Warn("Received signal [%v]. Shutting down routines...\n", sig)
			SetShutdownFlag()
		}()
	}()

	// Add SIGQUIT handler (dump thread stack trace)
	quitch := make(chan os.Signal, 1)
	signal.Notify(quitch, syscall.SIGQUIT)
	go func() {
		for {
			sig := <-quitch
			logger.Info("Received signal [%v]. Calling dumpStack()\n", sig)
			go dumpStack()
		}
	}()
}

// dumpStack to /tmp/reportd.stack and log
func dumpStack() {
	buf := make([]byte, 1<<20)
	stacklen := runtime.Stack(buf, true)
	ioutil.WriteFile("/tmp/reportd.stack", buf[:stacklen], 0644)
	logger.Warn("Printing Thread Dump...\n")
	logger.Warn("\n\n%s\n\n", buf[:stacklen])
	logger.Warn("Thread dump complete.\n")
}

//  The publisher sends random messages starting with A-J:
//  This is a debug thread taken from https://github.com/pebbe/zmq4/blob/master/examples/espresso.go
func debugPublisher() {
	publisher, _ := zmq.NewSocket(zmq.PUB)
	publisher.Bind("tcp://*:5561")

	for {
		s := fmt.Sprintf("%c-%05d", rand.Intn(10)+'A', rand.Intn(100000))
		_, err := publisher.SendMessage("untangle:packetd:events", s)
		if err != nil {
			break //  Interrupted
		}
		time.Sleep(100 * time.Millisecond) //  Wait for 1/10th second
	}
}

// prints some basic stats about packetd
func printStats() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	logger.Info("Memory Stats:\n")
	logger.Info("Memory Alloc: %d kB\n", (mem.Alloc / 1024))
	logger.Info("Memory TotalAlloc: %d kB\n", (mem.TotalAlloc / 1024))
	logger.Info("Memory HeapAlloc: %d kB\n", (mem.HeapAlloc / 1024))
	logger.Info("Memory HeapSys: %d kB\n", (mem.HeapSys / 1024))
}

// GetShutdownFlag returns the shutdown flag for kernel
func GetShutdownFlag() bool {
	if atomic.LoadUint32(&shutdownFlag) != 0 {
		return true
	}
	return false
}

// SetShutdownFlag sets the shutdown flag for kernel
func SetShutdownFlag() {
	shutdownChannel <- true
	atomic.StoreUint32(&shutdownFlag, 1)
}
