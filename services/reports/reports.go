package reports

import (
	"github.com/jsommerville-untangle/golang-shared/services/logger"
	pbe "github.com/jsommerville-untangle/golang-shared/structs/ProtoBuffEvent"
	zmq "github.com/pebbe/zmq4"
	"github.com/untangle/reportd/services/monitor"
	"google.golang.org/protobuf/proto"
)

// Startup is used to startup the reports service
func Startup() {
	logger.Info("Setting up zmq listening socket...\n")
	socket, err := setupZmqSocket()

	if err != nil {
		logger.Warn("Unable to setup ZMQ sockets.")
	}
	defer socket.Close()

	logger.Info("Setting up event listener on zmq socket...\n")
	go eventListener(socket)
}

// Shutdown is used to shutdown the reports service
func Shutdown() {

}

// eventListener is used to listen for ZMQ events being published
func eventListener(soc *zmq.Socket) {
	monitor.StartRoutine()
	logger.Info("Starting up a theoretical goroutine for event listening...\n")
	defer monitor.StartFinishRoutineThread()

	for {

		msg, err := soc.RecvMessageBytes(0)

		if err != nil {
			logger.Warn("Unable to receive messages: %s\n", err)
			return
		}


		newEvt := &pbe.ProtoBuffEvent{}
		if err := proto.Unmarshal(msg[1], newEvt); err != nil {
			logger.Warn("Unable to parse message: %s\n", err)
		}
		// Drop this into the event queue for processing

	}
}

// eventLogger is used to log events to the database
func eventLogger(rtWatch chan int) {
	monitor.StartRoutine()
	defer monitor.StartFinishRoutineThread()

	logger.Info("Starting up a theoretical goroutine for event logging...\n")

	for {
		logger.Debug("Checking for messages in event queue...\n")
	}

}

// setupZmqSocket prepares a zmq socket for listening to events
func setupZmqSocket() (soc *zmq.Socket, err error) {
	subscriber, err := zmq.NewSocket(zmq.SUB)

	if err != nil {
		logger.Err("Unable to open ZMQ socket... %s\n", err)
		return nil, err
	}

	// TODO: we should read a file created by packetd that contains a randomized
	// ZMQ port to listen on
	subscriber.Connect("tcp://localhost:5561")
	subscriber.SetSubscribe("untangle:packetd:events")

	return subscriber, nil
}
