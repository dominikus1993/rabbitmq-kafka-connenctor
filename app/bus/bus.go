package bus

import "sync"

type Event struct {
	Data  []byte
	Topic string
}

type EventChannel chan Event

type EventChannelSlice []EventChannel

type EventBus struct {
	subscribers map[string]EventChannelSlice
	rm          sync.RWMutex
}

func (eb *EventBus) Subscribe(topic string, ch EventChannel) {
	eb.rm.Lock()
	if prev, found := eb.subscribers[topic]; found {
		eb.subscribers[topic] = append(prev, ch)
	} else {
		eb.subscribers[topic] = append([]EventChannel{}, ch)
	}
	eb.rm.Unlock()
}

func (eb *EventBus) Publish(topic string, data []byte) {
	eb.rm.RLock()
	if chans, found := eb.subscribers[topic]; found {
		// this is done because the slices refer to same array even though they are passed by value
		// thus we are creating a new slice with our elements thus preserve locking correctly.
		channels := append(EventChannelSlice{}, chans...)
		go func(data Event, dataChannelSlices EventChannelSlice) {
			for _, ch := range dataChannelSlices {
				ch <- data
			}
		}(Event{Data: data, Topic: topic}, channels)
	}
	eb.rm.RUnlock()
}
