package bus

type Event struct {
	Data  []byte
	Topic string
}

type EventChannel chan *Event

type EventChannelSlice []EventChannel
