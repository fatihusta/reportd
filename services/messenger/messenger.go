package messenger

import (
	"context"
	"unsafe"

	zmq "github.com/pebbe/zmq4"
	"github.com/untangle/golang-shared/services/logger"
	ise "github.com/untangle/golang-shared/structs/protocolbuffers/InterfaceStatsEvent"
	se "github.com/untangle/golang-shared/structs/protocolbuffers/SessionEvent"
	sse "github.com/untangle/golang-shared/structs/protocolbuffers/SessionStatsEvent"

	"github.com/untangle/reportd/services/cloudreporting"
	"github.com/untangle/reportd/services/localreporting"
	"github.com/untangle/reportd/services/monitor"
	"google.golang.org/protobuf/proto"
)

var messengerRelation = monitor.RoutineContextGroup{}
var incomingMessages = make(chan [][]byte, 1000)

// Startup intializes the ZMQ socket and starts the sessionListener go routine
func Startup() {
	logger.Info("Setting up event subscriber socket...\n")
	eventSocket, err := setupEventSubscriberSocket()
	if err != nil {
		logger.Warn("Unable to setup event subscriber socket: %s\n", err)
	}

	logger.Info("Setting up reporting query socket...\n")
	querySocket, err := setupQuerySocket()
	if err != nil {
		logger.Warn("Unable to setup report query socket: %s", err)
	}

	messengerRelation = monitor.CreateRoutineContextRelation(context.Background(), "messenger", []string{"event_router", "session_listener", "session_stats_listener", "interface_stats_listener", "query_handler", "pair_tester"})

	go eventRouter(messengerRelation.Contexts["event_router"])

	if querySocket != nil {
		go queryHandler(messengerRelation.Contexts["query_handler"], querySocket)
	}

	if eventSocket != nil {
		logger.Info("Setting up event listeners on zmq socket...\n")
		go eventListener(messengerRelation.Contexts["session_listener"], "session_listener", "untangle:packetd:sessions", eventSocket)
		go eventListener(messengerRelation.Contexts["session_stats_listener"], "session_stats_listener", "untangle:packetd:session-stats", eventSocket)
		go eventListener(messengerRelation.Contexts["interface_stats_listener"], "interface_stats_listener", "untangle:packetd:interface-stats", eventSocket)
	}

}

// Shutdown signals the serviceShutdown channel to close any running goroutines spawned by this service
func Shutdown() {
	logger.Info("Shutting down messenger service...\n")
	monitor.CancelContexts(messengerRelation)
}

// eventRouter is used for routing and parsing received messages to proper channels
// this reads the incomingMessages queue and sends them to proper localreporting or cloudreporting queues
// THIS IS A ROUTINE
func eventRouter(ctx context.Context) {
	rtName := "event_router"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping Message Router\n")
			return
		case msg := <-incomingMessages:
			logger.Debug("Routing message for: %s \n", msg[0])

			switch topic := string(msg[0]); topic {
			case "untangle:packetd:sessions":
				evt := &se.SessionEvent{}
				if err := proto.Unmarshal(msg[1], evt); err != nil {
					logger.Warn("Unable to parse message: %s\n", err)
					continue
				}
				logger.Debug("Parsed %s message: %s\n", topic, evt)
				localreporting.AddToSessionChannel(evt)

			case "untangle:packetd:session-stats":
				evt := &sse.SessionStatsEvent{}
				if err := proto.Unmarshal(msg[1], evt); err != nil {
					logger.Warn("Unable to parse message: %s\n", err)
					continue
				}
				logger.Debug("Parsed %s message: %s\n", topic, evt)
				localreporting.AddToSessionStatsChannel(evt)

			case "untangle:packetd:interface-stats":
				evt := &ise.InterfaceStatsEvent{}
				if err := proto.Unmarshal(msg[1], evt); err != nil {
					logger.Warn("Unable to parse message: %s\n", err)
					continue
				}
				logger.Debug("Parsed %s message: %s\n", topic, evt)
				localreporting.AddToInterfaceStatsChannel(evt)

				// only send WAN stats to the cloud interface stats channel
				if evt.IsWan {
					cloudreporting.AddToInterfaceStatsChannel(evt)
				}
			}
		}
	}
}

// eventListener is used to listen for ZMQ events being publishedon the passed in socket, with a specific topic
// THIS IS A ROUTINE
func eventListener(ctx context.Context, rtName string, topic string, soc *zmq.Socket) {
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)
	defer soc.Close()

	for {

		select {
		case <-ctx.Done():
			logger.Info("Stopping ZMQ listener\n")
			return
		default:
			err := soc.SetSubscribe(topic)
			if err != nil {
				logger.Warn("Unable to subscribe to topic. \n")
				monitor.RoutineError(rtName)
				return
			}

			msg, err := soc.RecvMessageBytes(0)

			if err != nil {
				logger.Warn("Unable to receive messages: %s\n", err)
				monitor.RoutineError(rtName)
				return
			}

			logger.Debug("Incoming Message on topic: %s size: %d bytes\n", topic, len(msg[1])+int(unsafe.Sizeof(msg[1])))

			incomingMessages <- msg
		}
	}
}

// setupEventSubscriberSocket prepares a zmq socket for listening to events
func setupEventSubscriberSocket() (soc *zmq.Socket, err error) {
	subscriber, err := zmq.NewSocket(zmq.SUB)

	if err != nil {
		logger.Err("Unable to open ZMQ socket... %s\n", err)
		return nil, err
	}

	// TODO: we should read a file created by packetd that contains a randomized
	// ZMQ port to listen on
	subscriber.Connect("tcp://localhost:5561")

	return subscriber, nil
}

// setupQuerySocket builds the zmq socket used for responding to
// database querys from restd
func setupQuerySocket() (soc *zmq.Socket, err error) {
	soc, err = zmq.NewSocket(zmq.PAIR)

	if err != nil {
		logger.Err("Unable to open ZMQ socket... %s\n", err)
		return nil, err
	}

	// TODO: we should read a file created by packetd that contains a randomized
	// ZMQ port to listen on
	//replySoc.Connect("tcp://localhost:5590")
	err = soc.Bind("tcp://127.0.0.1:5590")
	if err != nil {
		logger.Err("Unable to bind to ZMQ Pair socket... %s\n", err)
		return nil, err
	}

	return
}

// queryHandler listens for requests on the db query socket and handles them appropriately
func queryHandler(ctx context.Context, socket *zmq.Socket) {
	rtName := "query_handler"
	monitor.RoutineStarted(rtName)
	defer monitor.RoutineEnd(rtName)

	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping %s\n", rtName)
			return
		default:
			msg, err := socket.RecvMessage(0)
			if err != nil {
				logger.Warn("Unable to receive messages: %s\n", err)
				monitor.RoutineError(rtName)
				return
			}

			logger.Info("queryHandler mesage: %s\n", msg)
			logger.Info("Incoming Message size: %d bytes\n", len(msg)+int(unsafe.Sizeof(msg)))

			testResponse := fmt.Sprintf("This is a test response message, got ur msg: %s\n", msg)
			logger.Info("Sending back... %s", testResponse)

			socket.Send(testResponse, 0)

		}
	}
}

