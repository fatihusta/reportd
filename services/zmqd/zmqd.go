package zmqd

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/untangle/golang-shared/services/logger"
	rzs "github.com/untangle/golang-shared/services/restdZmqServer"
	rrep "github.com/untangle/golang-shared/structs/protocolbuffers/ReportdReply"
	zreq "github.com/untangle/golang-shared/structs/protocolbuffers/ZMQRequest"
	"github.com/untangle/reportd/services/localreporting"
	"github.com/untangle/reportd/util"
	"google.golang.org/protobuf/proto"
	spb "google.golang.org/protobuf/types/known/structpb"
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
	// TestInfo is the ZMQRequest TEST_INFO function
	TestInfo = zreq.ZMQRequest_TEST_INFO
)

// Startup starts the zmq socket via restdZmqServer
func Startup() {
	processer := reportdProc(0)
	rzs.Startup(processer)
}

// Shutdown shuts down the zmq socket via restdZmqServer
func Shutdown() {
	rzs.Shutdown()
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
		logger.Info("Handling QueryCreate")
		queryString := string(request.Data.Value)
		sanitizedQueryString := util.TrimLeftChar(util.TrimLeftChar(util.TrimLeftChar(queryString)))
		logger.Info("=============QUERY STRING: %s\n", sanitizedQueryString)
		createdQuery, err := localreporting.CreateQuery(sanitizedQueryString)
		logger.Info("Created query: %v", createdQuery)
		if err != nil {
			return nil, errors.New("Error creating query " + err.Error())
		}
		var queryCreateError error
		reply.QueryCreate = createdQuery.ID
		logger.Info("========Created QUERY ID: %d\n", createdQuery.ID)
		if queryCreateError != nil {
			return nil, errors.New("Error translating created query to protobuf " + queryCreateError.Error())
		}
	case QueryData:
		logger.Info("Handling QueryData")
		queryID := binary.BigEndian.Uint64(request.Data.Value)
		data, err := localreporting.GetData(queryID)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting data for query %d "+err.Error(), queryID))
		}

		response := make([]map[string]interface{}, 0)
		queryData := make(map[string]interface{})
		queryData["queryData"] = data
		response = append(response, queryData)

		// Convert table to protobuf
		var queryDataError error
		reply.QueryData, queryDataError = dataToProtobufStruct(response)
		if queryDataError != nil {
			return nil, errors.New("Error translating query data to protobuf " + queryDataError.Error())
		}
	case QueryClose:
		logger.Info("Handling QueryClose, ", request.Data)
		queryID := binary.BigEndian.Uint64(request.Data.Value)
		success, err := localreporting.CloseQuery(queryID)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Error getting data for query %d "+err.Error(), queryID))
		}

		// response := make([]map[string]interface{}, 0)
		// queryData := make(map[string]interface{})
		// queryData["data"] = success
		// response = append(response, queryData)
		reply.QueryClose = success
	// case TestInfo:
	// 	// TestInfo gets the test info for packetd for zmq testing
	// 	info := retrieveTestInfo()

	// 	// Convert table to protobuf
	// 	var testInfoErr error
	// 	reply.TestInfo, testInfoErr = dataToProtobufStruct(info)
	// 	if testInfoErr != nil {
	// 		return nil, errors.New("Error translating test info to protobuf: " + testInfoErr.Error())
	// 	}
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

// dataToProtobufStruct converts the returned packetd data into a protobuf
func dataToProtobufStruct(info []map[string]interface{}) ([]*spb.Struct, error) {
	// loop through the information and convert to a protobuf struct
	var protobufStruct []*spb.Struct
	for _, v := range info {
		infoStruct, err := spb.NewStruct(v)

		if err != nil {
			return nil, errors.New("Error translating data to a protobuf: " + err.Error())
		}

		protobufStruct = append(protobufStruct, infoStruct)
	}

	return protobufStruct, nil
}

// retrieveTestInfo creates test info to test zmq
func retrieveTestInfo() []map[string]interface{} {
	var tests []map[string]interface{}
	m1 := make(map[string]interface{})
	m1["ping"] = "pong"
	m1["tennis"] = "ball"
	tests = append(tests, m1)
	tests = append(tests, m1)
	m2 := make(map[string]interface{})
	m2["pong"] = "ping"
	m2["ball"] = "tennis"
	tests = append(tests, m2)
	tests = append(tests, m2)

	return tests
}
