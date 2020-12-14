package main

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
)

var routineWatcher = make(chan int)

// main is the entrypoint of reportd
func main() {

	logger.Startup()

	logger.Info("Starting up reportd...\n")

	handleSignals()

	logger.Info("Setting up zmq listening socket...\n")
	socket, err := setupZmqSocket()

	if err != nil {
		logger.Warn("Unable to setup ZMQ sockets.")
	}
	defer socket.Close()

	logger.Info("Setting up event listener on zmq socket...\n")
	go eventListener(socket)

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
func eventListener(soc *zmq.Socket) {
	routineWatcher <- +1
	logger.Info("Starting up a theoretical goroutine for event listening...\n")
	defer startFinishRoutineThread()

	timeout := time.After(5 * time.Second)
	tick := time.Tick(500 * time.Millisecond)

	for {
		select {
		case <-timeout:
			logger.Warn("eventListener timed out! \n")
			return
		case <-tick:

			logger.Info("Listening for messages\n")

			msg, err := soc.RecvMessage(0)
			if err != nil {
				logger.Warn("Unable to receive messages: %s\n", err)
				return
			}

			logger.Info("Got some data here, topic: %s, message: %s\n", msg[0], msg[1])

		}
	}
}

// eventLogger is used to log events to the database
func eventLogger() {
	routineWatcher <- +1
	defer startFinishRoutineThread()

	logger.Info("Starting up a theoretical goroutine for event logging...\n")

	timeout := time.After(5 * time.Second)
	tick := time.Tick(500 * time.Millisecond)

	for {
		select {
		case <-timeout:
			logger.Warn("event Logger timed out! \n")
			return
		case <-tick:
			logger.Info("Reading messages from event queue...")
		}
	}

}

// startFinishRoutineThread is a function to simplify how we can defer calling finishRoutine() at the top of a function,
// instead of having to always call it at the end of a routine
func startFinishRoutineThread() {
	go finishRoutine()
}

// finishRoutine is called at the end of a running go routine to empty the channel watcher
func finishRoutine() {
	routineWatcher <- -1
}

// setupZmqSocket prepares a zmq socket for listening to events
func setupZmqSocket() (soc *zmq.Socket, err error) {
	subscriber, err := zmq.NewSocket(zmq.SUB)

	if err != nil {
		logger.Err("Unable to open ZMQ socket... %s\n", err)
		return nil, err
	}

	subscriber.Connect("tcp://localhost:5561")
	subscriber.SetSubscribe("untangle:packetd:events")

	return subscriber, nil
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
