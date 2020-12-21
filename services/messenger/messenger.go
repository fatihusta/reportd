package messenger

import (
	"github.com/jsommerville-untangle/golang-shared/services/logger"
	pbe "github.com/jsommerville-untangle/golang-shared/structs/ProtoBuffEvent"
	zmq "github.com/pebbe/zmq4"
	"github.com/untangle/reportd/services/localreporting"
	"github.com/untangle/reportd/services/monitor"
	"google.golang.org/protobuf/proto"
)

// Channel to signal these routines to stop
var serviceShutdown = make(chan bool, 1)

// Startup intializes the ZMQ socket and starts the sessionListener go routine
func Startup() {
	logger.Info("Setting up zmq listening socket...\n")
	socket, err := setupZmqSocket()
	if err != nil {
		logger.Warn("Unable to setup ZMQ sockets.")
	}

	logger.Info("Setting up Session event listener on zmq socket...\n")
	go sessionListener(socket)
}

// Shutdown signals the serviceShutdown channel to close any running goroutines spawned by this service
func Shutdown() {
	serviceShutdown <- true

}

// sessionListener is used to listen for ZMQ events being published
// THIS IS A ROUTINE
func sessionListener(soc *zmq.Socket) {
	var rtName = "session_listener"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)
	defer soc.Close()

	for {

		// TODO: this is blocking, so we need to read these directly into a channel before parsing or something
		// if <-serviceShutdown {
		// 	logger.Info("Stopping ZMQ listener\n")
		// 	return
		// }

		msg, err := soc.RecvMessageBytes(0)

		if err != nil {
			logger.Warn("Unable to receive messages: %s\n", err)
			//monitor.RoutineError(rtName)
			return
		}

		//logger.Info("Incoming Message size: %d bytes\n", len(msg[1])+int(unsafe.Sizeof(msg[1])))

		newEvt := &pbe.ProtoBuffEvent{}
		if err := proto.Unmarshal(msg[1], newEvt); err != nil {
			logger.Warn("Unable to parse message: %s\n", err)
		}
		//logger.Info("Converted message: %s\n", newEvt)

		//For now we will just unmarshal here and send to the report processing channels
		// TODO: really we should send this to some event router to determine if we need to populate both local and cloud channels
		localreporting.AddToSessionChannel(newEvt)

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
	err = subscriber.SetSubscribe("untangle:packetd:sessions")
	if err != nil {
		logger.Warn("Unable to subscribe to topic. \n")
	}
	return subscriber, nil
}
