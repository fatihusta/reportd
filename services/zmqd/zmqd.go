package zmqd

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/untangle/golang-shared/services/logger"
	zs "github.com/untangle/golang-shared/services/zmqServer"
	rrep "github.com/untangle/golang-shared/structs/protocolbuffers/ReportdReply"
	zreq "github.com/untangle/golang-shared/structs/protocolbuffers/ZMQRequest"
	"github.com/untangle/reportd/services/localreporting"
	"google.golang.org/protobuf/proto"
)

type reportdProc int

const (
	// ReportdService is the ZMQRequest reportd service
	ReportdService = zreq.ZMQRequest_REPORTD
	// QueryCreate is the ZMQRequest QUERY_CREATE function
	QueryCreate = zreq.ZMQRequest_QUERY_CREATE
	// QueryData is the ZMQRequest QUERY_DATA function
	QueryData = zreq.ZMQRequest_QUERY_DATA
	// QueryClose is the ZMQRequest QUERY_CLOSE function
	QueryClose = zreq.ZMQRequest_QUERY_CLOSE
)

// Startup starts the zmq socket via restdZmqServer
func Startup() {
	processer := reportdProc(0)
	zs.Startup(processer)
}

// Shutdown shuts down the zmq socket via restdZmqServer
func Shutdown() {
	zs.Shutdown()
}

// Process is the packetdProc interface Process function implementation for restdZmqServer
// It processes the ZMQRequest and retrives the correct information from packetd
func (p reportdProc) Process(request *zreq.ZMQRequest) (processedReply []byte, processErr error) {
	// Check the request is for packetd
	service := request.Service
	if service != ReportdService {
		return nil, errors.New("Attempting to process a non-packetd request: " + service.String())
	}

	// Get the function and prepare the reply
	function := request.Function
	reply := &rrep.ReportdReply{}

	// Based on the Function, retrive the proper information
	switch function {
	case QueryCreate:
		logger.Info("Handling QueryCreate\n")
		queryString := request.Data
		createdQuery, err := localreporting.CreateQuery(queryString)
		if err != nil {
			return nil, errors.New("Error creating query " + err.Error())
		}
		reply.QueryCreate = createdQuery.ID

	case QueryData:
		logger.Info("Handling QueryData\n")
		queryIDStr := request.Data
		queryID, err := strconv.ParseUint(queryIDStr, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting data for query %s "+err.Error(), queryIDStr))
		}
		data, err := localreporting.GetData(queryID)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting data for query %d "+err.Error(), queryID))
		}
		reply.QueryData = data

	case QueryClose:
		logger.Info("Handling QueryClose\n")
		queryIDStr := request.Data
		queryID, err := strconv.ParseUint(queryIDStr, 10, 64)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting data for query %s "+err.Error(), queryIDStr))
		}
		success, err := localreporting.CloseQuery(queryID)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting data for query %d "+err.Error(), queryID))
		}

		reply.QueryClose = success

	default:
		// An unknown function sets the reply error
		reply.ServerError = "Unknown function request to reportd"
	}

	// Encode the reply
	encodedReply, err := proto.Marshal(reply)
	if err != nil {
		return nil, errors.New("Error encoding reply: " + err.Error())
	}

	return encodedReply, nil
}

// ProcessError is the packetd implementation of the ProcessError function for restdZmqServer
func (p reportdProc) ProcessError(serverErr string) (processedReply []byte, processErr error) {
	// Set the ServerError in the PacketdReply
	errReply := &rrep.ReportdReply{ServerError: serverErr}

	// Encode the reply
	reply, replyErr := proto.Marshal(errReply)
	if replyErr != nil {
		logger.Err("Error on creating error message ", replyErr.Error())
		return nil, replyErr
	}
	return reply, nil
}
