package messenger

type zmqMessage struct {
	Topic   string
	Message []byte
}
