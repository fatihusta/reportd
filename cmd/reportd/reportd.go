package reportd

import (
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	zmq "github.com/pebbe/zmq4"
	"github.com/untangle/packetd/services/logger"
	"github.com/untangle/reportd/services/monitor"
	"github.com/untangle/reportd/services/reports"
)

// main is the entrypoint of reportd
func main() {
	logger.Startup()
	logger.Info("Starting up reportd...\n")

	monitor.Startup()

	handleSignals()
	reports.Startup()

	monitor.Shutdown()
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
			os.Exit(0)
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
